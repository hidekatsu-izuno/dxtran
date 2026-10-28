//! Ordered record checks and user messages on stdout/stderr.
use crate::{
    config::{CheckLevel, Dataset},
    error::{Error, Result},
};
use datafusion::{
    arrow::{
        array::{Array, AsArray},
        datatypes::SchemaRef,
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    common::{DFSchema, ScalarValue},
    physical_expr::PhysicalExpr,
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
}
impl Default for Checks {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            schema: Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            id: String::new(),
            output: false,
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
        let arrays = values
            .iter()
            .map(|v| v.to_array_of_size(1))
            .collect::<datafusion::error::Result<Vec<_>>>()
            .map_err(|e| self.error(e.to_string()))?;
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| self.error(e.to_string()))?;
        self.batch(&batch, record - 1)
    }
    pub(crate) fn batch(&self, batch: &RecordBatch, completed: u64) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let side = if self.output { "output" } else { "input" };
        for row in 0..batch.num_rows() {
            let record = completed + row as u64 + 1;
            let batch = batch.slice(row, 1);
            for (i, rule) in self.rules.iter().enumerate() {
                let location = format!(
                    "{side} {} record {record} check {} {}",
                    self.id,
                    i + 1,
                    rule.origin
                );
                let result = rule
                    .expression
                    .evaluate(&batch)
                    .and_then(|v| v.into_array(1))
                    .map_err(|_| {
                        self.error("check.when SQL evaluation failed")
                            .context(&location)
                    })?;
                let result = result.as_boolean();
                if result.is_null(0) || !result.value(0) {
                    continue;
                }
                let level = match rule.level {
                    CheckLevel::Error => {
                        return Err(self
                            .error(format!("check error: {}", rule.message))
                            .context(&location));
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
                        .context(location)
                })?;
            }
        }
        Ok(())
    }
}
