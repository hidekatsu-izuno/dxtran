//! Ordered record checks and user messages on stdout/stderr.
use crate::{
    config::{CheckLevel, Dataset},
    error::{Error, Result},
};
use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    common::{DFSchema, ScalarValue},
    physical_expr::{
        PhysicalExpr,
        utils::{collect_columns, reassign_expr_columns},
    },
    prelude::SessionContext,
};
use serde_json::json;
use std::{
    io::{self, Write},
    sync::Arc,
};

#[derive(Clone, Debug)]
struct Rule {
    expression: Arc<dyn PhysicalExpr>,
    level: CheckLevel,
    message: String,
    origin: String,
}

#[derive(Clone, Debug)]
pub(crate) struct Checks {
    rules: Vec<Rule>,
    schema: SchemaRef,
    id: String,
    output: bool,
    columns: Vec<usize>,
}
impl Default for Checks {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            schema: Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            id: String::new(),
            output: false,
            columns: Vec::new(),
        }
    }
}

impl Checks {
    pub(crate) fn compile(dataset: &Dataset, schema: SchemaRef, output: bool) -> Result<Self> {
        let mut compiled = Self {
            schema,
            id: dataset.id.clone(),
            output,
            ..Default::default()
        };
        if dataset.check.is_empty() {
            return Ok(compiled);
        }
        let df_schema = DFSchema::try_from(compiled.schema.as_ref().clone())
            .map_err(|e| Error::config(e.to_string()))?;
        let context = SessionContext::new();
        crate::functions::register(&context);
        for (i, check) in dataset.check.iter().enumerate() {
            let origin = check
                .when_origin
                .as_ref()
                .or(check.origin.as_ref())
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("{} check {}", dataset.id, i + 1));
            let expression =
                crate::predicate::compile(&context, &check.when, &df_schema).map_err(|e| {
                    Error::config(e.to_string())
                        .context(format!("{origin} check.when: {}", check.when))
                })?;
            compiled.rules.push(Rule {
                expression,
                level: check.level,
                message: check.message.clone(),
                origin,
            });
        }
        compiled.columns = compiled
            .rules
            .iter()
            .flat_map(|r| {
                collect_columns(&r.expression)
                    .into_iter()
                    .map(|c| c.index())
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        compiled.schema = Arc::new(
            compiled
                .schema
                .project(&compiled.columns)
                .map_err(|e| Error::config(e.to_string()))?,
        );
        for rule in &mut compiled.rules {
            rule.expression = reassign_expr_columns(rule.expression.clone(), &compiled.schema)
                .map_err(|e| Error::config(e.to_string()))?;
        }
        Ok(compiled)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
    fn error(&self, message: impl Into<String>) -> Error {
        if self.output {
            Error::output(message)
        } else {
            Error::input(message)
        }
    }
    pub(crate) fn row(&self, values: &[ScalarValue], record: u64) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let arrays = self
            .columns
            .iter()
            .map(|&i| values[i].to_array_of_size(1))
            .collect::<datafusion::error::Result<Vec<_>>>()
            .map_err(|e| self.error(e.to_string()))?;
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| self.error(e.to_string()))?;
        self.check_row(&batch, 0, record)
    }
    /// Project once per batch; only columns referenced by checks are sliced.
    pub(crate) fn project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        batch
            .project(&self.columns)
            .map_err(|e| self.error(e.to_string()))
    }
    pub(crate) fn batch(&self, batch: &RecordBatch, completed: u64) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let batch = self.project(batch)?;
        for row in 0..batch.num_rows() {
            self.check_row(&batch, row, completed + row as u64 + 1)?;
        }
        Ok(())
    }
    pub(crate) fn check_row(&self, batch: &RecordBatch, row: usize, record: u64) -> Result<()> {
        let side = if self.output { "output" } else { "input" };
        let batch = batch.slice(row, 1);
        for (i, rule) in self.rules.iter().enumerate() {
            let location = || {
                format!(
                    "{side} {} record {record} check {} {}",
                    self.id,
                    i + 1,
                    rule.origin
                )
            };
            let result = rule.expression.evaluate(&batch).map_err(|_| {
                self.error("check.when SQL evaluation failed")
                    .context(location())
            })?;
            if !crate::predicate::is_true(&result, 0) {
                continue;
            }
            let level = match rule.level {
                CheckLevel::Error => {
                    return Err(self
                        .error(format!("check error: {}", rule.message))
                        .context(location()));
                }
                CheckLevel::Warn => "warn",
                CheckLevel::Info => "info",
            };
            let message = json!({"event":"check","level":level,"side":side,"definition":self.id,
                    "record":record,"check":i + 1,"message":rule.message});
            let written = if matches!(rule.level, CheckLevel::Info) {
                writeln!(io::stdout().lock(), "{message}")
            } else {
                writeln!(io::stderr().lock(), "{message}")
            };
            written.map_err(|e| {
                self.error(format!("check log write failed: {e}"))
                    .context(location())
            })?;
        }
        Ok(())
    }
}
