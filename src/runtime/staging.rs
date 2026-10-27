//! Execute each output into a private file; publication is a separate phase.
use super::{Resources, cooperative, csv, fixed, log, original_error};
use crate::{
    config::{Dataset, Job},
    error::{Error, Result},
    planner::{PlannedJob, PlannedOutput},
    sink,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

pub(super) async fn stage_outputs(
    ctx: &SessionContext,
    job: &Job,
    planned: &PlannedJob,
    resources: &Resources,
    directory: &Path,
    run_id: &str,
) -> Result<Vec<PathBuf>> {
    let mut staged = Vec::with_capacity(job.output.len());
    for (dataset, output) in job.output.iter().zip(&planned.outputs) {
        staged.push(
            stage_output(
                ctx,
                dataset,
                output,
                resources,
                directory,
                run_id,
                planned.conversion_budget.clone(),
            )
            .await?,
        );
    }
    Ok(staged)
}

async fn stage_output(
    ctx: &SessionContext,
    d: &Dataset,
    p: &PlannedOutput,
    resources: &Resources,
    directory: &Path,
    run_id: &str,
    budget: Arc<tokio::sync::Semaphore>,
) -> Result<PathBuf> {
    let start = Instant::now();
    let file = directory.join(format!("{}.tmp", d.id));
    let mut sink = sink::OutputSink::create(d, &p.layout, &file, &p.schema)?;
    let physical = ctx
        .execute_logical_plan(p.plan.clone())
        .await?
        .create_physical_plan()
        .await
        .map_err(original_error)?;
    let physical = cooperative::wrap(physical).map_err(original_error)?;
    let mut stream = datafusion::physical_plan::execute_stream(physical.clone(), ctx.task_ctx())
        .map_err(original_error)?;
    let workers = crate::parallel::workers(resources.threads, resources.memory_limit);
    if let Some(encoder) = sink.fixed_encoder().filter(|_| workers > 1) {
        sink = fixed::write(sink, stream, encoder, budget.clone(), workers)
            .await
            .map_err(|e| e.context(format!("output {}", d.id)))?;
    } else if let Some(encoder) = sink.csv_encoder().filter(|_| workers > 1) {
        sink = csv::write(sink, stream, encoder, budget.clone(), workers)
            .await
            .map_err(|e| e.context(format!("output {}", d.id)))?;
    } else {
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| original_error(e).context(format!("output {}", d.id)))?;
            let permit = budget
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| Error::sql(e.to_string()))?;
            sink = tokio::task::spawn_blocking(move || -> Result<sink::OutputSink> {
                sink.batch(batch)?;
                drop(permit);
                Ok(sink)
            })
            .await
            .map_err(|e| Error::output(e.to_string()))??;
        }
    }
    let rows = tokio::task::spawn_blocking(move || sink.finish())
        .await
        .map_err(|e| Error::output(e.to_string()))??;
    let bytes = fs::metadata(&file)
        .map_err(|e| Error::output(e.to_string()))?
        .len();
    let (spill_count, spilled_bytes) = spill_metrics(&physical);
    log(
        json!({"event":"output_staged","run_id":run_id,"output":d.id,"path":d.path,"rows":rows,"bytes":bytes,"spill_count":spill_count,"spilled_bytes":spilled_bytes,"seconds":start.elapsed().as_secs_f64()}),
    );
    Ok(file)
}

fn spill_metrics(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> (usize, usize) {
    let metrics = plan.metrics();
    let mut count = metrics.as_ref().and_then(|m| m.spill_count()).unwrap_or(0);
    let mut bytes = metrics
        .as_ref()
        .and_then(|m| m.spilled_bytes())
        .unwrap_or(0);
    for child in plan.children() {
        let (c, b) = spill_metrics(child);
        count += c;
        bytes += b;
    }
    (count, bytes)
}
