//! Trusted LuaJIT plugins, isolated per scan, with lazy cursors and bounded batches.
use crate::{
    config::{Dataset, Field, Format, identifier},
    encoding::TextEncoding,
    error::{Error, Result},
    framing::MAX_RECORD,
    layout::RecordLayout,
};
use datafusion::{
    arrow::{
        datatypes::{DataType, Field as ArrowField, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    common::ScalarValue,
};
use mlua::{Function, Lua, Table, Value};
use std::{collections::BTreeSet, sync::Arc};

pub(crate) fn layout(d: &Dataset) -> Result<RecordLayout> {
    if !d.encoding.is_empty()
        || d.compression.is_some()
        || d.csv.is_some()
        || d.fixed.is_some()
        || d.rdw.is_some()
        || d.parquet.is_some()
    {
        return Err(Error::config(
            "lua does not accept encoding, compression or file-format blocks",
        ));
    }
    if d.fields.is_empty() {
        return Err(Error::config("lua requires input.fields"));
    }
    let mut names = BTreeSet::new();
    let mut fields = Vec::new();
    for spec in &d.fields {
        let mut extra = spec.clone();
        extra.name = None;
        extra.kind = None;
        extra.nullable = None;
        extra.origin = None;
        extra.origins.clear();
        if extra != Field::default() {
            return Err(
                Error::config("lua fields accept only name, type and nullable").context(
                    spec.origin
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                ),
            );
        }
        let name = spec
            .name
            .as_deref()
            .filter(|n| identifier(n))
            .ok_or_else(|| Error::config("invalid or missing lua column name"))?;
        if !names.insert(name) {
            return Err(Error::config(format!("duplicate lua column '{name}'")));
        }
        let kind = match spec.kind.as_deref().unwrap_or("string") {
            "string" => DataType::Utf8,
            "int64" => DataType::Int64,
            "uint64" => DataType::UInt64,
            "float64" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "binary" => DataType::Binary,
            _ => {
                return Err(Error::config(
                    "lua type must be string, int64, uint64, float64, boolean or binary",
                ));
            }
        };
        fields.push(ArrowField::new(name, kind, spec.nullable.unwrap_or(false)));
    }
    Ok(RecordLayout {
        fields: vec![],
        schema: Arc::new(Schema::new(fields)),
        length: 0,
        rest: false,
        separator: vec![],
        max_record_length: 0,
        format: Format::Lua,
        encoding: TextEncoding::UTF_8,
        conditional: None,
        checks: Default::default(),
    })
}

pub(crate) struct Input {
    _lua: Lua,
    job: Option<Arc<crate::lua::JobState>>,
    open: Function,
    options: Table,
    cursor: Option<Arc<crate::lua::Resource>>,
    next: Option<Function>,
    finished: bool,
    schema: SchemaRef,
    pending: Option<Vec<ScalarValue>>,
    dataset: Dataset,
    record: u64,
    checks: crate::check::Checks,
}

impl Input {
    pub(crate) fn load(d: &Dataset, layout: &RecordLayout) -> Result<Self> {
        let load = || -> mlua::Result<Self> {
            let crate::lua::Plugin {
                lua,
                module,
                options,
                job,
            } = crate::lua::load(d)?;
            let open: Function = module.raw_get("open")?;
            Ok(Self {
                _lua: lua,
                job,
                open,
                options,
                cursor: None,
                next: None,
                finished: false,
                schema: layout.schema.clone(),
                pending: None,
                dataset: d.clone(),
                record: 0,
                checks: layout.checks.clone(),
            })
        };
        load().map_err(|e| {
            Error::config(e.to_string()).context(format!("input {} {} lua", d.id, d.path.display()))
        })
    }

    fn error(&self, error: impl std::fmt::Display) -> Error {
        Error::input(error.to_string()).context(format!(
            "input {} {} lua record {}",
            self.dataset.id,
            self.dataset.path.display(),
            self.record + 1
        ))
    }

    fn start(&mut self) -> Result<()> {
        if self.cursor.is_some() {
            return Ok(());
        }
        if let Some(job) = &self.job {
            job.start_locked().map_err(|e| self.error(e))?;
        }
        let cursor: Table = self
            .open
            .call(self.options.clone())
            .map_err(|e| self.error(e))?;
        // Retain the cursor before validation so malformed cursors are closed.
        let resource = crate::lua::Resource::new(cursor.clone(), false);
        if let Some(job) = &self.job {
            job.register(&resource);
        }
        self.cursor = Some(resource);
        let _: Option<Function> = cursor.raw_get("close").map_err(|e| self.error(e))?;
        self.next = Some(cursor.raw_get("next").map_err(|e| self.error(e))?);
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        let job = self.job.clone();
        let _guard = job.as_ref().map(|job| job.cleanup_lock());
        self.close_inner()
    }
    fn close_inner(&mut self) -> Result<()> {
        self.finished = true;
        self.next = None;
        if let Some(cursor) = self.cursor.take() {
            cursor.close().map_err(|e| self.error(e))?;
        }
        Ok(())
    }

    fn row(&mut self) -> Result<Option<Vec<ScalarValue>>> {
        let job = self.job.clone();
        let _guard = job
            .as_ref()
            .map(|job| job.lock())
            .transpose()
            .map_err(|e| self.error(e))?;
        self.row_inner()
    }
    fn row_inner(&mut self) -> Result<Option<Vec<ScalarValue>>> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        if self.finished {
            return Ok(None);
        }
        self.start()?;
        let value: Value = self
            .next
            .as_ref()
            .unwrap()
            .call(())
            .map_err(|e| self.error(e))?;
        let table = match value {
            Value::Nil => {
                self.close_inner()?;
                return Ok(None);
            }
            Value::Table(table) => table,
            _ => return Err(self.error("next() must return a row table or nil")),
        };
        for pair in table.clone().pairs::<Value, Value>() {
            let (key, _) = pair.map_err(|e| self.error(e))?;
            let known = match key {
                Value::String(key) => key
                    .to_str()
                    .ok()
                    .is_some_and(|key| self.schema.index_of(key.as_ref()).is_ok()),
                _ => false,
            };
            if !known {
                return Err(self.error("row contains an undeclared column"));
            }
        }
        let mut row = Vec::with_capacity(self.schema.fields().len());
        let mut bytes = 0usize;
        for field in self.schema.fields() {
            let value: Value = table
                .raw_get(field.name().as_str())
                .map_err(|e| self.error(e))?;
            // Account before copying a Lua string into Rust/Arrow.
            bytes = bytes
                .saturating_add(size_of::<ScalarValue>())
                .saturating_add(match &value {
                    Value::String(s) => s.as_bytes().len(),
                    _ => 0,
                });
            if bytes > MAX_RECORD - 32 - size_of::<ScalarValue>() {
                return Err(self.error("decoded record exceeds 16 MiB"));
            }
            row.push(convert(value, field).map_err(|message| {
                self.error(message)
                    .context(format!("column {}", field.name()))
            })?);
        }
        self.checks.row(&row, self.record + 1)?;
        self.record += 1;
        Ok(Some(row))
    }

    pub(crate) fn take(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let result = self.batch(max_rows);
        if result.is_err() {
            let _ = self.close();
        }
        result
    }

    fn batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        let mut columns: Vec<Vec<ScalarValue>> = vec![vec![]; self.schema.fields().len()];
        let mut rows = 0;
        let mut bytes = 0;
        while rows < max_rows {
            let Some(row) = self.row()? else {
                break;
            };
            let size =
                row.iter().map(ScalarValue::size).sum::<usize>() + 32 + size_of::<ScalarValue>();
            if size > MAX_RECORD {
                return Err(self.error("decoded record exceeds 16 MiB"));
            }
            if rows > 0 && bytes + size > MAX_RECORD {
                self.pending = Some(row);
                break;
            }
            bytes += size;
            for (column, value) in columns.iter_mut().zip(row) {
                column.push(value);
            }
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        let arrays = columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<datafusion::error::Result<Vec<_>>>()
            .map_err(|e| self.error(e))?;
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map(Some)
            .map_err(|e| self.error(e))
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        // Also runs when a downstream LIMIT/error drops the source stream.
        let _ = self.close();
    }
}

fn convert(value: Value, field: &ArrowField) -> std::result::Result<ScalarValue, &'static str> {
    if value == Value::Nil {
        return if field.is_nullable() {
            ScalarValue::try_from(field.data_type()).map_err(|_| "unsupported null type")
        } else {
            Err("NULL in a non-nullable column")
        };
    }
    let bad =
        || "Lua value does not match the declared type (use strings for exact 64-bit integers)";
    match (field.data_type(), value) {
        (DataType::Utf8, Value::String(s)) => Ok(ScalarValue::Utf8(Some(
            s.to_str().map_err(|_| "string must be UTF-8")?.to_string(),
        ))),
        (DataType::Binary, Value::String(s)) => {
            Ok(ScalarValue::Binary(Some(s.as_bytes().to_vec())))
        }
        (DataType::Boolean, Value::Boolean(b)) => Ok(ScalarValue::Boolean(Some(b))),
        (DataType::Int64, Value::String(s)) => Ok(ScalarValue::Int64(Some(
            s.to_str().map_err(|_| bad())?.parse().map_err(|_| bad())?,
        ))),
        (DataType::UInt64, Value::String(s)) => Ok(ScalarValue::UInt64(Some(
            s.to_str().map_err(|_| bad())?.parse().map_err(|_| bad())?,
        ))),
        (DataType::Int64, Value::Integer(n)) if exact_integer(n as f64) => {
            Ok(ScalarValue::Int64(Some(n)))
        }
        (DataType::UInt64, Value::Integer(n)) if n >= 0 && exact_integer(n as f64) => {
            Ok(ScalarValue::UInt64(Some(n as u64)))
        }
        (DataType::Int64, Value::Number(n)) if exact_integer(n) => {
            Ok(ScalarValue::Int64(Some(n as i64)))
        }
        (DataType::UInt64, Value::Number(n)) if exact_integer(n) && n >= 0.0 => {
            Ok(ScalarValue::UInt64(Some(n as u64)))
        }
        (DataType::Float64, Value::Number(n)) if n.is_finite() => Ok(ScalarValue::Float64(Some(n))),
        (DataType::Float64, Value::Integer(n)) => Ok(ScalarValue::Float64(Some(n as f64))),
        _ => Err(bad()),
    }
}

fn exact_integer(n: f64) -> bool {
    n.is_finite() && n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991.0
}
