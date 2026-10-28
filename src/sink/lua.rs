//! Stage Arrow data first, then replay it into one transactional plugin writer.
use crate::{
    config::{Dataset, Format, identifier},
    encoding::TextEncoding,
    error::{Error, Result},
    framing::MAX_RECORD,
    layout::RecordLayout,
};
use datafusion::{
    arrow::{
        datatypes::{DataType, SchemaRef},
        ipc::reader::FileReader,
    },
    common::ScalarValue,
};
use mlua::{Function, Lua, Table, Value};
use std::{collections::BTreeSet, fs::File, path::Path, sync::Arc};

fn column_type(kind: &DataType) -> Result<&'static str> {
    Ok(match kind {
        DataType::Null => "null",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "int64",
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => "uint64",
        DataType::Float32 | DataType::Float64 => "float64",
        DataType::Boolean => "boolean",
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary",
        DataType::Decimal128(_, _) => "decimal",
        DataType::Date32 => "date",
        DataType::Time64(_) => "time",
        DataType::Timestamp(_, _) => "timestamp",
        _ => {
            return Err(Error::config(format!(
                "unsupported lua output SQL type {kind:?}; use SQL CAST"
            )));
        }
    })
}

pub(crate) fn layout(d: &Dataset, schema: SchemaRef) -> Result<RecordLayout> {
    if !d.encoding.is_empty()
        || !d.fields.is_empty()
        || d.compression.is_some()
        || d.csv.is_some()
        || d.fixed.is_some()
        || d.rdw.is_some()
        || d.parquet.is_some()
    {
        return Err(Error::config(
            "lua output uses the SQL schema; fields, encoding, compression and file-format blocks are not allowed",
        ));
    }
    if !matches!(
        d.mode.as_deref(),
        None | Some("append" | "overwrite" | "merge")
    ) {
        return Err(Error::config(
            "lua output mode must be append, overwrite or merge",
        ));
    }
    let mut names = BTreeSet::new();
    for field in schema.fields() {
        if !identifier(field.name()) || !names.insert(field.name()) {
            return Err(Error::config(
                "lua output column names must be unique public identifiers",
            ));
        }
        column_type(field.data_type())?;
    }
    if schema.fields().is_empty() {
        return Err(Error::config("lua output requires at least one SQL column"));
    }
    // Load and validate the API without opening a writer or a transaction.
    Output::load(d, &schema)?;
    Ok(RecordLayout {
        fields: vec![],
        schema,
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

fn columns(lua: &Lua, schema: &SchemaRef) -> mlua::Result<Table> {
    let columns = lua.create_table()?;
    for (index, field) in schema.fields().iter().enumerate() {
        let column = lua.create_table()?;
        column.set("name", field.name().as_str())?;
        column.set(
            "type",
            column_type(field.data_type()).map_err(mlua::Error::external)?,
        )?;
        column.set("nullable", field.is_nullable())?;
        match field.data_type() {
            DataType::Decimal128(precision, scale) => {
                column.set("precision", *precision)?;
                column.set("scale", *scale)?;
            }
            DataType::Timestamp(unit, zone) => {
                column.set("unit", format!("{unit:?}"))?;
                column.set("timezone", zone.as_deref())?;
            }
            DataType::Time64(unit) => column.set("unit", format!("{unit:?}"))?,
            _ => (),
        }
        columns.raw_set(index + 1, column)?;
    }
    Ok(columns)
}

struct Output {
    lua: Lua,
    job: Option<Arc<crate::lua::JobState>>,
    open: Function,
    options: Table,
    columns: Table,
    cursor: Option<Arc<crate::lua::Resource>>,
    write: Option<Function>,
    dataset: Dataset,
    rows: u64,
}

impl Output {
    fn load(d: &Dataset, schema: &SchemaRef) -> Result<Self> {
        let load = || -> mlua::Result<Self> {
            let crate::lua::Plugin {
                lua,
                module,
                options,
                job,
            } = crate::lua::load(d)?;
            let open: Function = module.raw_get("open_writer")?;
            let columns = columns(&lua, schema)?;
            if let Some(validate) = module.raw_get::<Option<Function>>("validate_output")? {
                validate.call::<()>((
                    options.clone(),
                    columns.clone(),
                    d.mode.as_deref().unwrap_or("append"),
                ))?;
            }
            Ok(Self {
                lua,
                job,
                open,
                options,
                columns,
                cursor: None,
                write: None,
                dataset: d.clone(),
                rows: 0,
            })
        };
        load().map_err(|e| {
            Error::config(e.to_string()).context(format!(
                "output {} {} lua",
                d.id,
                d.path.display()
            ))
        })
    }

    fn error(&self, error: impl std::fmt::Display) -> Error {
        Error::output(error.to_string()).context(format!(
            "output {} {} lua record {}",
            self.dataset.id,
            self.dataset.path.display(),
            self.rows + 1
        ))
    }

    fn start(&mut self) -> Result<()> {
        let job = self.job.clone();
        let _guard = job
            .as_ref()
            .map(|job| job.lock())
            .transpose()
            .map_err(|e| self.error(e))?;
        if let Some(job) = &job {
            job.start_locked().map_err(|e| self.error(e))?;
        }
        let cursor: Table = self
            .open
            .call((
                self.options.clone(),
                self.columns.clone(),
                self.dataset.mode.as_deref().unwrap_or("append"),
            ))
            .map_err(|e| self.error(e))?;
        // Keep the cursor even if API validation fails, so rollback still runs.
        let resource = crate::lua::Resource::new(cursor.clone(), true);
        if let Some(job) = &job {
            job.register(&resource);
        }
        self.cursor = Some(resource);
        for name in ["commit", "rollback"] {
            let _: Function = cursor.raw_get(name).map_err(|e| self.error(e))?;
        }
        let _: Option<Function> = cursor.raw_get("close").map_err(|e| self.error(e))?;
        self.write = Some(cursor.raw_get("write").map_err(|e| self.error(e))?);
        Ok(())
    }

    fn row(&mut self, values: Vec<ScalarValue>, schema: &SchemaRef) -> Result<()> {
        let job = self.job.clone();
        let _guard = job
            .as_ref()
            .map(|job| job.lock())
            .transpose()
            .map_err(|e| self.error(e))?;
        if values.iter().map(ScalarValue::size).sum::<usize>() > MAX_RECORD {
            return Err(self.error("decoded output record exceeds 16 MiB"));
        }
        let row = self.lua.create_table().map_err(|e| self.error(e))?;
        for (field, value) in schema.fields().iter().zip(values) {
            let converted = to_lua(&self.lua, value)
                .map_err(|e| self.error(e).context(format!("column {}", field.name())))?;
            row.raw_set(field.name().as_str(), converted)
                .map_err(|e| self.error(e))?;
        }
        self.write
            .as_ref()
            .unwrap()
            .call::<()>(row)
            .map_err(|e| self.error(e))?;
        self.rows += 1;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let job = self.job.clone();
        let _guard = job
            .as_ref()
            .map(|job| job.lock())
            .transpose()
            .map_err(|e| self.error(e))?;
        let commit: Function = self
            .cursor
            .as_ref()
            .unwrap()
            .table
            .raw_get("commit")
            .map_err(|e| self.error(e))?;
        commit.call::<()>(()).map_err(|e| {
            self.error(e)
                .context("commit failed; the external commit outcome may be unknown")
        })?;
        self.cursor.as_ref().unwrap().mark_committed();
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        let job = self.job.clone();
        let _guard = job.as_ref().map(|job| job.cleanup_lock());
        self.write = None;
        let Some(cursor) = self.cursor.take() else {
            return Ok(());
        };
        cursor.close().map_err(|error| {
            self.error(error).context(if cursor.committed() {
                "commit succeeded, but writer cleanup failed"
            } else {
                "writer rollback/cleanup failed"
            })
        })
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn to_lua(lua: &Lua, value: ScalarValue) -> mlua::Result<Value> {
    if value.is_null() {
        return Ok(Value::Nil);
    }
    let string = |bytes: &[u8]| lua.create_string(bytes).map(Value::String);
    match value {
        ScalarValue::Boolean(Some(value)) => Ok(Value::Boolean(value)),
        ScalarValue::Float32(Some(value)) if value.is_finite() => Ok(Value::Number(value as f64)),
        ScalarValue::Float64(Some(value)) if value.is_finite() => Ok(Value::Number(value)),
        ScalarValue::Float32(_) | ScalarValue::Float64(_) => {
            Err(mlua::Error::runtime("non-finite float in lua output"))
        }
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => string(value.as_bytes()),
        ScalarValue::Binary(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::BinaryView(Some(value))
        | ScalarValue::FixedSizeBinary(_, Some(value)) => string(&value),
        ScalarValue::Date32(_)
        | ScalarValue::Time64Microsecond(_)
        | ScalarValue::Time64Nanosecond(_)
        | ScalarValue::TimestampSecond(_, _)
        | ScalarValue::TimestampMillisecond(_, _)
        | ScalarValue::TimestampMicrosecond(_, _)
        | ScalarValue::TimestampNanosecond(_, _) => {
            let array = value.to_array_of_size(1).map_err(mlua::Error::external)?;
            let text = datafusion::arrow::util::display::array_value_to_string(array.as_ref(), 0)
                .map_err(mlua::Error::external)?;
            string(text.as_bytes())
        }
        _ => string(value.to_string().as_bytes()),
    }
}

pub(crate) fn spool(path: &Path) -> Result<FileReader<File>> {
    File::open(path)
        .map_err(|e| Error::output(e.to_string()))
        .and_then(|file| FileReader::try_new(file, None).map_err(|e| Error::output(e.to_string())))
}

pub(crate) fn publish(d: &Dataset, path: &Path) -> Result<()> {
    let reader = spool(path)?;
    let schema = reader.schema();
    let mut writer = Output::load(d, &schema).map_err(|e| Error::output(e.message))?;
    let result = (|| -> Result<()> {
        if crate::runtime::interrupted() {
            return Err(Error::sql("interrupted"));
        }
        writer.start()?;
        for batch in reader {
            let batch = batch.map_err(|e| writer.error(e))?;
            for row in 0..batch.num_rows() {
                if crate::runtime::interrupted() {
                    return Err(Error::sql("interrupted"));
                }
                let values = batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, row))
                    .collect::<datafusion::error::Result<Vec<_>>>()
                    .map_err(|e| writer.error(e))?;
                writer.row(values, &schema)?;
            }
        }
        if crate::runtime::interrupted() {
            return Err(Error::sql("interrupted"));
        }
        writer.commit()
    })();
    let cleanup = writer.cleanup();
    match (result, cleanup) {
        (Ok(()), cleanup) => cleanup,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup)) => Err(error.context(cleanup)),
    }
}
