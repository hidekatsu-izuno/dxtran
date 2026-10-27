//! Native Arrow/Parquet I/O shared by planning, scanning and execution.
use crate::{
    config::{Dataset, Format},
    encoding::TextEncoding,
    error::{Error, Result},
    framing::MAX_RECORD,
    layout::RecordLayout,
};
use datafusion::arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        ArrowSchemaConverter,
        arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    },
    basic::Compression,
    file::properties::WriterProperties,
};
use std::{collections::BTreeSet, fs::File};

fn input_error(d: &Dataset, error: impl std::fmt::Display) -> Error {
    Error::input(error.to_string()).context(format!("input {} {} parquet", d.id, d.path.display()))
}
fn open(d: &Dataset) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let file = File::open(&d.path).map_err(|e| input_error(d, e))?;
    if !file.metadata().map_err(|e| input_error(d, e))?.is_file() {
        return Err(Error::config("input must be a regular file").context(d.path.display()));
    }
    ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| input_error(d, e))
}

pub(crate) fn properties(d: &Dataset) -> Result<WriterProperties> {
    let options = d.parquet.clone().unwrap_or_default();
    if options.row_group_size == 0 {
        return Err(Error::config("parquet.row_group_size must be positive"));
    }
    let compression = match options.compression.as_str() {
        "uncompressed" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(Default::default()),
        "gzip" => Compression::GZIP(Default::default()),
        "brotli" => Compression::BROTLI(Default::default()),
        "lz4_raw" => Compression::LZ4_RAW,
        _ => {
            return Err(Error::config(
                "parquet.compression must be uncompressed, snappy, zstd, gzip, brotli or lz4_raw",
            ));
        }
    };
    Ok(WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(options.row_group_size))
        .set_max_row_group_bytes(Some(MAX_RECORD))
        .build())
}

fn validate_names(schema: &Schema) -> Result<()> {
    let mut names = BTreeSet::new();
    if schema.fields().is_empty() {
        return Err(Error::config("parquet requires at least one column"));
    }
    for field in schema.fields() {
        if !crate::config::identifier(field.name()) || !names.insert(field.name()) {
            return Err(Error::config(format!(
                "parquet column '{}' must be a unique public identifier",
                field.name()
            )));
        }
    }
    Ok(())
}

pub(crate) fn layout(d: &Dataset, result: Option<SchemaRef>) -> Result<RecordLayout> {
    let output = result.is_some();
    if !d.encoding.is_empty()
        || !d.fields.is_empty()
        || d.csv.is_some()
        || d.fixed.is_some()
        || d.rdw.is_some()
    {
        return Err(Error::config(
            "parquet uses its native schema; encoding, fields, csv, fixed and rdw are not allowed",
        ));
    }
    if !matches!(
        d.mode.as_deref(),
        None | Some("overwrite" | "error_if_exists")
    ) {
        return Err(Error::config("invalid output mode"));
    }
    if !output && d.parquet.is_some() {
        return Err(Error::config(
            "parquet options are output-only; input codecs are read from the file",
        ));
    }
    let schema = if output {
        properties(d)?;
        let schema =
            result.ok_or_else(|| Error::config("parquet output requires a SQL result schema"))?;
        ArrowSchemaConverter::new().convert(&schema).map_err(|e| {
            Error::config(format!(
                "unsupported parquet output schema: {e}; use SQL CAST"
            ))
        })?;
        schema
    } else {
        open(d)?.schema().clone()
    };
    validate_names(&schema)?;
    Ok(RecordLayout {
        fields: Vec::new(),
        schema,
        length: 0,
        rest: false,
        separator: Vec::new(),
        max_record_length: 0,
        format: Format::Parquet,
        // The record-layout encoding is unused for native Arrow storage.
        encoding: TextEncoding(encoding_rs::UTF_8),
        conditional: None,
        checks: Default::default(),
    })
}

pub(crate) struct Input {
    reader: ParquetRecordBatchReader,
    pending: Option<RecordBatch>,
    offset: usize,
}
impl Input {
    pub(crate) fn open(d: &Dataset, schema: &SchemaRef, file: File) -> Result<Self> {
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| input_error(d, e))?;
        if builder.schema() != schema {
            return Err(input_error(d, "schema changed after planning"));
        }
        // This is an estimate, not a cap on decompressed pages or process RSS.
        let bytes = builder
            .metadata()
            .row_groups()
            .iter()
            .fold(0u64, |n, group| {
                n.saturating_add(group.total_byte_size().max(0) as u64)
            });
        let rows = builder.metadata().file_metadata().num_rows().max(1) as u64;
        let width = bytes.div_ceil(rows).max(1).min(usize::MAX as u64) as usize;
        let size = (MAX_RECORD / width).clamp(1, 8192);
        let reader = builder
            .with_batch_size(size)
            .build()
            .map_err(|e| input_error(d, e))?;
        Ok(Self {
            reader,
            pending: None,
            offset: 0,
        })
    }
    pub(crate) fn take(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        while self
            .pending
            .as_ref()
            .is_none_or(|b| self.offset == b.num_rows())
        {
            self.pending = self
                .reader
                .next()
                .transpose()
                .map_err(|e| Error::input(format!("parquet decoding failed: {e}")))?;
            self.offset = 0;
            if self.pending.is_none() {
                return Ok(None);
            }
        }
        let batch = self.pending.as_ref().unwrap();
        let rows = max_rows.min(batch.num_rows() - self.offset);
        let result = batch.slice(self.offset, rows);
        self.offset += rows;
        Ok(Some(result))
    }
}
