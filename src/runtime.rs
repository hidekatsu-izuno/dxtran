mod cooperative;
mod csv;
mod fixed;
mod signals;
use crate::{
    config,
    error::{Error, Result},
    planner, publication,
    source::InputReader,
};
use datafusion::{physical_plan::displayable, prelude::SessionContext};
use serde_json::json;
pub(crate) use signals::interrupted;
use std::time::Instant;
mod cli;
mod session;
mod staging;
pub use cli::{Cli, Resources, default_threads, parse_memory};
use cli::{Command, parse_cli};
#[cfg(test)]
use cli::{default_threads_for, positive};
use session::context;

fn log(value: serde_json::Value) {
    eprintln!("{value}");
}
fn phase(code: u8) -> &'static str {
    match code {
        2 => "configuration",
        3 => "input",
        4 => "sql",
        _ => "output",
    }
}
fn original_error(e: datafusion::error::DataFusionError) -> Error {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&e);
    while let Some(s) = source {
        if let Some(e) = s.downcast_ref::<Error>() {
            return Error::new(e.code, e.message.clone());
        }
        source = s.source();
    }
    use datafusion::error::DataFusionError;
    match e.find_root() {
        DataFusionError::ResourcesExhausted(_)
        | DataFusionError::IoError(_)
        | DataFusionError::Plan(_)
        | DataFusionError::NotImplemented(_)
        | DataFusionError::Configuration(_) => Error::from(e),
        root => {
            let message = root.to_string().to_ascii_lowercase();
            let cause =
                if message.contains("divide by zero") || message.contains("division by zero") {
                    "division by zero"
                } else if message.contains("overflow") {
                    "numeric overflow"
                } else if message.contains("cast") {
                    "type conversion"
                } else {
                    "SQL computation"
                };
            Error::sql(format!("{cause} failed (input value details omitted)"))
        }
    }
}
enum Operation {
    Check { scan: bool },
    Explain,
    Run,
}

async fn execute(cli: Cli, run_id: &str) -> Result<()> {
    let (path, resources, operation) = match cli.command {
        Command::Check {
            job,
            scan,
            resources,
        } => (job, resources, Operation::Check { scan }),
        Command::Explain { job, resources } => (job, resources, Operation::Explain),
        Command::Run { job, resources } => (job, resources, Operation::Run),
    };
    let mut job = config::load_with_paths(&path, &cli.paths)?;
    if cli.allow_lua {
        job.allow_unsafe_lua();
    }
    job.check_lua_permission()?;
    let temp = temporary_parent(&resources)?;
    publication::preflight(&job, &temp)?;
    log_start(&job, &path, &resources, run_id)?;
    let guard = job.lua_runtime.guard();
    let result = async {
        job.lua_runtime.validate()?;
        let ctx = context(&resources, None)?;
        let planned = planner::plan(&ctx, &job).await?;
        validate_plans(
            &ctx,
            &job,
            &planned,
            matches!(operation, Operation::Explain),
        )
        .await?;
        if matches!(operation, Operation::Run | Operation::Check { scan: true }) {
            job.lua_runtime.start()?;
        }
        if matches!(operation, Operation::Check { scan: true }) {
            scan_inputs(&planned).await?;
        }
        if !matches!(operation, Operation::Run) {
            if matches!(operation, Operation::Check { .. }) {
                println!(
                    "OK: {} inputs, {} processes, {} outputs",
                    job.input.len(),
                    job.process.len(),
                    job.output.len()
                );
            }
            return Ok(());
        }
        run_job(&job, &planned, &resources, &temp, run_id).await
    }
    .await;
    guard.finish(result)
}
async fn run_job(
    job: &config::Job,
    planned: &planner::PlannedJob,
    resources: &Resources,
    temp: &std::path::Path,
    run_id: &str,
) -> Result<()> {
    let work = crate::filesystem::create_workspace(temp, &format!("dxtran-{run_id}-"))
        .map_err(|e| Error::output(e.to_string()))?;
    let outputs = work.path().join("outputs");
    let spill = work.path().join("spill");
    crate::filesystem::create_staging_dir(&outputs)
        .and_then(|_| crate::filesystem::create_staging_dir(&spill))
        .map_err(|e| Error::output(e.to_string()))?;
    let ctx = context(resources, Some(&spill))?;
    let staged = staging::stage_outputs(&ctx, job, planned, resources, &outputs, run_id).await?;
    publication::publish(job, &staged)?;
    // Drop DataFusion's disk manager before removing this run's working directory.
    drop(ctx);
    work.close().map_err(|e| Error::output(e.to_string()))?;
    Ok(())
}
fn temporary_parent(resources: &Resources) -> Result<std::path::PathBuf> {
    let temp = resources
        .temp_dir
        .clone()
        .unwrap_or_else(std::env::temp_dir);
    let temp = if temp.is_absolute() {
        temp
    } else {
        std::env::current_dir()
            .map_err(|e| Error::config(e.to_string()))?
            .join(temp)
    };
    Ok(temp)
}
fn log_start(
    job: &config::Job,
    path: &std::path::Path,
    resources: &Resources,
    run_id: &str,
) -> Result<()> {
    let lua_execution = if job.lua.is_some()
        || job
            .input
            .iter()
            .chain(&job.output)
            .any(|d| d.format == config::Format::Lua)
    {
        "trusted-unsafe"
    } else {
        "disabled"
    };
    log(
        json!({"event":"start","run_id":run_id,"version":env!("CARGO_PKG_VERSION"),"time":chrono::Utc::now().to_rfc3339(),"job":path,"hashes":job.hashes,"expanded_hash":job.expanded_hash,"threads":resources.threads,"memory_limit":resources.memory_limit,"lua_execution":lua_execution}),
    );
    for d in job.input.iter().chain(&job.output) {
        if matches!(d.format, config::Format::Parquet | config::Format::Lua) {
            continue;
        }
        let enc = crate::encoding::TextEncoding::resolve(&d.encoding)?;
        log(
            json!({"event":"encoding","run_id":run_id,"definition":d.id,"label":d.encoding,"canonical":enc.name(),"output_encoding":enc.output_name()}),
        );
    }
    Ok(())
}

async fn validate_plans(
    ctx: &SessionContext,
    job: &config::Job,
    planned: &planner::PlannedJob,
    explain: bool,
) -> Result<()> {
    for (d, p) in job.output.iter().zip(&planned.outputs) {
        let frame = ctx.execute_logical_plan(p.plan.clone()).await?;
        let physical = frame
            .create_physical_plan()
            .await
            .map_err(|e| Error::from(e).context(&d.id))?;
        if explain {
            println!(
                "output {}\nLogical plan:\n{}\nPhysical plan:\n{}",
                d.id,
                p.plan.display_indent(),
                displayable(physical.as_ref()).indent(true)
            );
        }
    }
    Ok(())
}
async fn scan_inputs(planned: &planner::PlannedJob) -> Result<()> {
    for (source, layout) in &planned.inputs {
        let dataset = source.dataset.clone();
        let layout = layout.clone();
        let count = tokio::task::spawn_blocking(move || -> Result<u64> {
            let mut reader = InputReader::open(&dataset, &layout)?;
            let mut count = 0;
            while let Some(batch) = reader.next_batch(layout.schema.clone(), None)? {
                count += batch.num_rows() as u64;
            }
            Ok(count)
        })
        .await
        .map_err(|e| Error::sql(e.to_string()))??;
        println!("{}: {count} records scanned", source.dataset.id);
    }
    Ok(())
}
pub fn main_cli() -> i32 {
    let cli = match parse_cli(std::env::args_os()) {
        Ok(c) => c,
        Err(e) => {
            let code = if e.use_stderr() { 2 } else { 0 };
            let _ = e.print();
            return code;
        }
    };
    let threads = match &cli.command {
        Command::Check { resources, .. }
        | Command::Explain { resources, .. }
        | Command::Run { resources, .. } => resources.threads,
    };
    let run_id = format!(
        "{}-{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%S%.9f"),
        std::process::id()
    );
    let start = Instant::now();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(threads)
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return 4;
        }
    };
    let registration = {
        let _entered = runtime.enter();
        signals::Signals::new()
    };
    let (signals, signal_rx) = match registration {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{e}");
            return 4;
        }
    };
    let result=runtime.block_on(async {tokio::select! {biased;_=signal_rx=>Err(Error::sql("interrupted")),result=execute(cli,&run_id)=>result}});
    drop(signals);
    let code = match result {
        Ok(()) => 0,
        Err(e) => {
            log(json!({"event":"error","run_id":run_id,"phase":phase(e.code),"message":e.message}));
            i32::from(e.code)
        }
    };
    runtime.shutdown_timeout(std::time::Duration::from_secs(5));
    log(
        json!({"event":"finish","run_id":run_id,"time":chrono::Utc::now().to_rfc3339(),"seconds":start.elapsed().as_secs_f64(),"exit_code":code}),
    );
    code
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults() {
        assert_eq!(
            (1..=10).map(default_threads_for).collect::<Vec<_>>(),
            [1, 1, 1, 2, 2, 3, 3, 4, 4, 4]
        );
        assert_eq!(parse_memory("1GiB").unwrap(), 1 << 30);
        assert!(positive("0").is_err());
        assert!(parse_memory("0MiB").is_err());
    }
    #[test]
    fn computation_errors_do_not_log_input_values() {
        let error = original_error(datafusion::error::DataFusionError::Execution(
            "Cannot cast secret-input-42 to integer".into(),
        ));
        assert!(!error.message.contains("secret-input-42"));
        assert!(error.message.contains("type conversion"));
    }
}
