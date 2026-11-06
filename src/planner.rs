mod dependencies;
mod hooks;
mod names;
mod order;
mod sql;
mod view;
use dependencies::process_order;
use order::{OrderPlanner, public_columns};
use sql::prepare_sql;
pub use sql::references;
pub(crate) mod unnest;

use crate::{
    aggregate::{AggregateChecks, HookStep},
    config::Job,
    error::{Error, Result},
    layout::RecordLayout,
    snapshot::SnapshotTable,
    source::{InputReader, SourceTable},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use std::{collections::BTreeMap, sync::Arc};
pub struct PlannedOutput {
    pub plan: LogicalPlan,
    pub schema: SchemaRef,
    pub layout: RecordLayout,
    pub(crate) before: AggregateChecks,
}
pub struct PlannedJob {
    pub(crate) conversion_budget: Arc<tokio::sync::Semaphore>,
    pub inputs: Vec<(SourceTable, RecordLayout)>,
    pub outputs: Vec<PlannedOutput>,
    pub(crate) hooks: Vec<HookStep>,
}
pub async fn plan(ctx: &SessionContext, job: &Job) -> Result<PlannedJob> {
    let bound;
    let job = if job.params_bound {
        job
    } else {
        bound = job.clone().with_params(&[]).await?;
        &bound
    };
    job.check_lua_permission()?;
    let mut inputs = vec![];
    let mut sets = BTreeMap::new();
    let mut order = OrderPlanner::default();
    let selection = hooks::Selection::new(job)?;
    let mut snapshots = BTreeMap::new();
    let mut hooks = Vec::new();
    let conversion_budget = Arc::new(tokio::sync::Semaphore::new(
        ctx.copied_config().target_partitions(),
    ));
    for (i, d) in job.input.iter().enumerate() {
        let layout = RecordLayout::input(d).map_err(|e| e.context(&d.id))?;
        InputReader::open(d, &layout)?;
        let source = SourceTable {
            dataset: d.clone(),
            layout: layout.clone(),
            dataset_no: i + 1,
            order_name: None,
            conversion_budget: conversion_budget.clone(),
        };
        ctx.register_table(&d.id, Arc::new(source.clone()))?;
        let after = AggregateChecks::compile(
            &d.after,
            "input",
            &d.id,
            "after",
            &d.id,
            layout.schema.clone(),
            0,
        )
        .await?;
        if selection.freeze.contains(&d.id) {
            let logical = ctx.table(&d.id).await?.into_unoptimized_plan();
            let ordered = order.rewrite(&logical)?;
            let plan = crate::snapshot::ordered_plan(ordered.plan, ordered.key)?;
            let table = SnapshotTable::new(
                Arc::new(plan.schema().as_arrow().clone()),
                true,
                conversion_budget.clone(),
            )?;
            ctx.deregister_table(&d.id)?;
            ctx.register_table(&d.id, Arc::new(table.clone()))?;
            snapshots.insert(d.id.clone(), table.clone());
            hooks.push(HookStep {
                id: d.id.clone(),
                input: true,
                active: selection.reachable.contains(&d.id),
                plan,
                table,
                before: Vec::new(),
                after,
            });
        }
        inputs.push((source, layout));
    }
    for i in process_order(job)? {
        let p = &job.process[i];
        let mut before = Vec::new();
        for (index, check) in p.before.iter().enumerate() {
            let target = &selection.before_targets[&p.id][index];
            let schema = Arc::new(ctx.table(target).await?.schema().as_arrow().clone());
            let compiled = AggregateChecks::compile(
                std::slice::from_ref(check),
                "process",
                &p.id,
                "before",
                target,
                schema,
                index,
            )
            .await?;
            if selection.reachable.contains(&p.id) {
                before.push((snapshots[target].clone(), compiled));
            }
        }
        let sql = prepare_sql(&p.sql, &mut sets)?;
        let frame = ctx.sql(&sql).await.map_err(|e| {
            Error::from(e).context(format!(
                "process {} {}",
                p.id,
                p.origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })?;
        let logical = frame.into_unoptimized_plan();
        names::validate_plan(&logical).map_err(|e| e.context(format!("process {}", p.id)))?;
        order.sets.clone_from(&sets);
        let ordered = order.rewrite(&logical).map_err(|e| {
            e.context(format!(
                "process {} {}",
                p.id,
                p.origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })?;
        let after = AggregateChecks::compile(
            &p.after,
            "process",
            &p.id,
            "after",
            &p.id,
            Arc::new(logical.schema().as_arrow().clone()),
            0,
        )
        .await?;
        if selection.freeze.contains(&p.id) {
            let plan = crate::snapshot::ordered_plan(ordered.plan, ordered.key)?;
            let table = SnapshotTable::new(
                Arc::new(plan.schema().as_arrow().clone()),
                true,
                conversion_budget.clone(),
            )?;
            ctx.register_table(&p.id, Arc::new(table.clone()))?;
            snapshots.insert(p.id.clone(), table.clone());
            hooks.push(HookStep {
                id: p.id.clone(),
                input: false,
                active: true,
                plan,
                table,
                before,
                after,
            });
        } else {
            ctx.register_table(&p.id, Arc::new(view::ProcessView::new(logical, ordered)))?;
        }
    }
    let mut outputs = vec![];
    for d in &job.output {
        let sql = prepare_sql(d.sql.as_ref().unwrap(), &mut sets)?;
        let frame = ctx.sql(&sql).await.map_err(|e| {
            Error::from(e).context(format!(
                "output {} {}",
                d.id,
                d.sql_origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })?;
        let logical = frame.into_unoptimized_plan();
        names::validate_plan(&logical).map_err(|e| e.context(format!("output {}", d.id)))?;
        order.sets.clone_from(&sets);
        let ordered = order.rewrite(&logical).map_err(|e| {
            e.context(format!(
                "output {} {}",
                d.id,
                d.sql_origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })?;
        let sort = ordered.key.clone().sort(true, false);
        let sorted = LogicalPlanBuilder::from(ordered.plan)
            .sort(vec![sort])?
            .build()?;
        let columns = public_columns(&sorted);
        let plan = LogicalPlanBuilder::from(sorted).project(columns)?.build()?;
        let schema = Arc::new(plan.schema().as_arrow().clone());
        let layout = RecordLayout::output(d, schema.clone()).map_err(|e| e.context(&d.id))?;
        let before = AggregateChecks::compile(
            &d.before,
            "output",
            &d.id,
            "before",
            &d.id,
            schema.clone(),
            0,
        )
        .await?;
        outputs.push(PlannedOutput {
            plan,
            schema,
            layout,
            before,
        });
    }
    Ok(PlannedJob {
        inputs,
        outputs,
        conversion_budget,
        hooks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    #[test]
    fn scope_and_safety() {
        assert_eq!(
            references(
                "WITH local AS (SELECT * FROM sales) SELECT * FROM local JOIN customers ON true"
            )
            .unwrap(),
            BTreeSet::from(["sales".into(), "customers".into()])
        );
        for sql in [
            "DELETE FROM sales",
            "SELECT 1; SELECT 2",
            "SELECT * FROM read_csv('x')",
            "WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x",
        ] {
            assert!(references(sql).is_err(), "{sql}");
        }
    }
}
