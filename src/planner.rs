mod dependencies;
mod names;
mod order;
mod sql;
use dependencies::process_order;
use order::{OrderPlanner, public_columns};
use sql::prepare_sql;
pub use sql::references;
pub(crate) mod unnest;

use crate::{
    config::Job,
    error::{Error, Result},
    layout::RecordLayout,
    source::{InputReader, SourceTable},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::view::ViewTable,
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use std::{collections::BTreeMap, sync::Arc};
pub struct PlannedOutput {
    pub plan: LogicalPlan,
    pub schema: SchemaRef,
    pub layout: RecordLayout,
}
pub struct PlannedJob {
    pub(crate) conversion_budget: Arc<tokio::sync::Semaphore>,
    pub inputs: Vec<(SourceTable, RecordLayout)>,
    pub outputs: Vec<PlannedOutput>,
}
pub async fn plan(ctx: &SessionContext, job: &Job) -> Result<PlannedJob> {
    let mut inputs = vec![];
    let mut sets = BTreeMap::new();
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
        inputs.push((source, layout));
    }
    for i in process_order(job)? {
        let p = &job.process[i];
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
        OrderPlanner {
            serial: 0,
            sets: sets.clone(),
        }
        .rewrite(&logical)
        .map_err(|e| {
            e.context(format!(
                "process {} {}",
                p.id,
                p.origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })?;
        ctx.register_table(&p.id, Arc::new(ViewTable::new(logical, None)))?;
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
        let ordered = OrderPlanner {
            serial: 0,
            sets: sets.clone(),
        }
        .rewrite(&logical)
        .map_err(|e| {
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
        outputs.push(PlannedOutput {
            plan,
            schema,
            layout,
        });
    }
    Ok(PlannedJob {
        inputs,
        outputs,
        conversion_budget,
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
