//! Sequential record I/O, validation, and bounded Arrow batches.
use super::{
    batch::{BatchColumn, BatchRow, RowSize, field_extra_bound, order_key},
    csv::{CsvReader, CsvRecord},
};
use crate::{
    config::{Dataset, Format},
    encoding::TextReader,
    error::{Error, Result},
    field::ValueSink,
    framing,
    layout::RecordLayout,
};
use datafusion::{
    arrow::{
        array::StringBuilder,
        datatypes::SchemaRef,
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    common::ScalarValue,
    error::{DataFusionError, Result as DFResult},
};
use std::{fs::File, io::BufReader, sync::Arc};

enum Reader {
    Csv(CsvReader<TextReader<crate::compression::Input>>),
    Physical(BufReader<crate::compression::Input>),
    Parquet(Box<crate::parquet_io::Input>),
    Lua(Box<super::lua::Input>),
}
pub struct InputReader {
    pub(super) dataset: Dataset,
    pub layout: RecordLayout,
    reader: Reader,
    pub(super) record: u64,
    byte_position: u64,
    body: Vec<u8>,
    pub(super) cells: CsvRecord,
    pub(super) record_ready: bool,
    record_position: u64,
    row_base_size: usize,
    physical_row_bound: usize,
    checked_row: Option<Vec<ScalarValue>>,
}
impl InputReader {
    pub fn open(dataset: &Dataset, layout: &RecordLayout) -> Result<Self> {
        if dataset.format == Format::Lua {
            return Ok(Self {
                dataset: dataset.clone(),
                layout: layout.clone(),
                reader: Reader::Lua(Box::new(super::lua::Input::load(dataset, layout)?)),
                record: 0,
                byte_position: 0,
                body: Vec::new(),
                cells: CsvRecord::default(),
                record_ready: false,
                record_position: 0,
                row_base_size: 0,
                physical_row_bound: 0,
                checked_row: None,
            });
        }
        let file = File::open(&dataset.path)
            .map_err(|e| Error::input(e.to_string()).context(dataset.path.display()))?;
        if !file
            .metadata()
            .map_err(|e| Error::input(e.to_string()))?
            .is_file()
        {
            return Err(Error::config("input must be a regular file"));
        }
        let reader = if dataset.format == Format::Csv {
            let options = dataset.csv.clone().unwrap_or_default();
            let mut reader = CsvReader::new(
                TextReader::new(
                    crate::compression::Input::new(file, dataset.compression)
                        .map_err(|e| Error::input(e.to_string()).context(dataset.path.display()))?,
                    layout.encoding,
                ),
                options.delimiter.as_bytes()[0],
                options.quote.as_bytes()[0],
            );
            if options.header {
                let header = reader
                    .next_record()
                    .map_err(|e| {
                        e.context(format!("input {} {}", dataset.id, dataset.path.display()))
                    })?
                    .ok_or_else(|| Error::input("required CSV header is missing"))?;
                if header.iter().map(|c| c.value.as_str()).ne(layout
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str()))
                {
                    return Err(Error::input(
                        "CSV header names, count and order must exactly match input.fields",
                    )
                    .context(dataset.path.display()));
                }
            }
            Reader::Csv(reader)
        } else if dataset.format == Format::Parquet {
            Reader::Parquet(Box::new(crate::parquet_io::Input::open(
                dataset,
                &layout.schema,
                file,
            )?))
        } else {
            Reader::Physical(BufReader::new(
                crate::compression::Input::new(file, dataset.compression)
                    .map_err(|e| Error::input(e.to_string()).context(dataset.path.display()))?,
            ))
        };
        let row_base_size: usize = if dataset.format == Format::Parquet {
            0
        } else {
            layout
                .schema
                .fields()
                .iter()
                .map(|f| ScalarValue::try_from(f.data_type()).map(|v| v.size()))
                .collect::<DFResult<Vec<_>>>()?
                .into_iter()
                .sum()
        };
        let physical_row_bound = layout.fields.iter().fold(row_base_size, |size, f| {
            size.saturating_add(field_extra_bound(f, f.length.unwrap_or(0)))
        });
        Ok(Self {
            dataset: dataset.clone(),
            layout: layout.clone(),
            reader,
            record: 0,
            byte_position: 0,
            body: Vec::new(),
            cells: CsvRecord::default(),
            record_ready: false,
            record_position: 0,
            row_base_size,
            physical_row_bound,
            checked_row: None,
        })
    }
    pub fn next_row(&mut self) -> Result<Option<Vec<ScalarValue>>> {
        let mut row = Vec::with_capacity(self.layout.schema.fields().len());
        Ok(self.next_row_into(&mut row)?.then_some(row))
    }
    pub fn next_row_into(&mut self, row: &mut Vec<ScalarValue>) -> Result<bool> {
        row.clear();
        if matches!(self.reader, Reader::Parquet(_) | Reader::Lua(_)) {
            let Some(batch) = self.native_batch(self.layout.schema.clone(), None, 1)? else {
                return Ok(false);
            };
            for array in batch.columns() {
                row.push(ScalarValue::try_from_array(array, 0)?);
            }
            return Ok(true);
        }
        if !self.read_record()? {
            return Ok(false);
        }
        self.decode_record(row)?;
        self.record_ready = false;
        Ok(true)
    }
    // Retain one raw record when a batch is full. No string/scalar row is needed
    // on the normal path, and its record number is assigned only once.
    pub(super) fn read_record(&mut self) -> Result<bool> {
        if self.record_ready {
            return Ok(true);
        }
        let record = self.record + 1;
        let id = &self.dataset.id;
        let path = &self.dataset.path;
        let position = self.byte_position;
        let context = |e: Error| {
            e.context(format!(
                "input {id} {} record {record} byte {position}",
                path.display()
            ))
        };
        match &mut self.reader {
            Reader::Parquet(_) | Reader::Lua(_) => unreachable!("native Arrow batches"),
            Reader::Csv(reader) => {
                let first_line = reader.line;
                if !reader.read_record(&mut self.cells).map_err(context)? {
                    return Ok(false);
                };
                if self.cells.len() != self.layout.fields.len() {
                    return Err(context(Error::input(format!(
                        "CSV column count mismatch, physical lines {first_line}..{}",
                        reader.line
                    ))));
                }
            }
            Reader::Physical(reader) => {
                let found = match self.dataset.format {
                    Format::Fixed => framing::fixed_into(
                        reader,
                        self.layout.length,
                        &self.layout.separator,
                        &mut self.body,
                    ),
                    Format::Rdw => {
                        framing::rdw_into(reader, self.layout.max_record_length, &mut self.body)
                    }
                    _ => unreachable!(),
                }
                .map_err(context)?;
                if !found {
                    return Ok(false);
                }
                self.byte_position += self.body.len() as u64
                    + if self.dataset.format == Format::Rdw {
                        4
                    } else {
                        self.layout.separator.len() as u64
                    };
                if self.layout.conditional.is_none()
                    && (self.body.len() < self.layout.length
                        || !self.layout.rest && self.body.len() != self.layout.length)
                {
                    return Err(context(Error::input(
                        "record body does not match layout length",
                    )));
                }
            }
        }
        self.record_position = position;
        self.record = record;
        self.record_ready = true;
        self.checked_row = None;
        if !self.layout.checks.is_empty() {
            // Decode once, then retain the checked row across size measurement
            // and batch read-ahead. Neither path may emit a message twice.
            let mut row = Vec::new();
            self.decode_record(&mut row)?;
            self.layout.checks.row(&row, record)?;
            self.checked_row = Some(row);
        }
        Ok(true)
    }
    pub(super) fn decode_record(&self, values: &mut impl ValueSink) -> Result<()> {
        if let Some(row) = &self.checked_row {
            for value in row {
                values.scalar(value.clone())?;
            }
            return Ok(());
        }
        let context = |e: Error| {
            e.context(format!(
                "input {} {} record {} byte {}",
                self.dataset.id,
                self.dataset.path.display(),
                self.record,
                self.record_position
            ))
        };
        if self.dataset.format == Format::Csv {
            for ((value, was_quoted), field) in self.cells.cells().zip(&self.layout.fields) {
                let text = if value.is_empty() && !was_quoted {
                    None
                } else {
                    Some(field.trim_text(value))
                };
                field.parse_text_to(text, values).map_err(|e| {
                    context(e.context(format!("column {}", field.spec.name.as_deref().unwrap())))
                })?;
            }
        } else {
            self.layout
                .decode_physical(&self.body, values)
                .map_err(context)?;
        }
        Ok(())
    }
    pub(super) fn row_size_bound(&self) -> usize {
        if self.dataset.format == Format::Csv {
            // Decoded strings/binaries cannot exceed their UTF-8 CSV cell size.
            return self.row_base_size.saturating_add(self.cells.bytes());
        }
        let mut bound = self.physical_row_bound;
        if self.layout.rest {
            for field in self.layout.fields.iter().filter(|f| f.length.is_none()) {
                bound = bound.saturating_add(field_extra_bound(
                    field,
                    self.body.len().saturating_sub(field.offset),
                ));
            }
        }
        bound
    }
    pub(crate) fn next_batch(
        &mut self,
        schema: SchemaRef,
        dataset_no: Option<usize>,
    ) -> Result<Option<RecordBatch>> {
        if matches!(self.reader, Reader::Parquet(_) | Reader::Lua(_)) {
            return self.native_batch(schema, dataset_no, 8192);
        }
        let mut columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| BatchColumn::new(f.data_type()))
            .collect();
        let mut rows = 0;
        let mut bytes = 0;
        let order_size = if dataset_no.is_some() {
            size_of::<ScalarValue>() + 32
        } else {
            0
        };
        while rows < 8192 && bytes < framing::MAX_RECORD && self.read_record()? {
            let bound = self.row_size_bound().saturating_add(order_size);
            let mut row = BatchRow {
                columns: &mut columns,
                column: 0,
                bytes: 0,
            };
            if bound > framing::MAX_RECORD {
                // A conservative bound must not reject a valid record (e.g.
                // a large padded field that trims to an empty string).
                let mut size = RowSize::default();
                self.decode_record(&mut size)?;
                let actual = size.0 + order_size;
                if actual > framing::MAX_RECORD {
                    return Err(Error::input("decoded record exceeds 16 MiB"));
                }
                if rows > 0 && bytes + actual > framing::MAX_RECORD {
                    break;
                }
                self.decode_record(&mut row)?;
            } else {
                if rows > 0 && bytes + bound > framing::MAX_RECORD {
                    // Even a read-ahead record must be fully validated if a
                    // downstream LIMIT stops before requesting the next batch.
                    self.decode_record(&mut RowSize::default())?;
                    break;
                }
                self.decode_record(&mut row)?;
            }
            if let Some(dataset) = dataset_no {
                let key = order_key(dataset, self.record);
                row.text(Some(std::str::from_utf8(&key).expect("ASCII order key")))?;
            }
            debug_assert!(row.bytes <= bound);
            bytes += row.bytes;
            rows += 1;
            self.record_ready = false;
        }
        if rows == 0 {
            return Ok(None);
        }
        let arrays = columns
            .into_iter()
            .map(BatchColumn::finish)
            .collect::<DFResult<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map(Some)
        .map_err(DataFusionError::from)
        .map_err(Error::from)
    }
    fn native_batch(
        &mut self,
        schema: SchemaRef,
        dataset_no: Option<usize>,
        max_rows: usize,
    ) -> Result<Option<RecordBatch>> {
        let batch = match &mut self.reader {
            Reader::Parquet(reader) => reader.take(max_rows),
            Reader::Lua(reader) => reader.take(max_rows),
            _ => unreachable!(),
        };
        let Some(batch) = batch.map_err(|e| {
            e.context(format!(
                "input {} {} record {}",
                self.dataset.id,
                self.dataset.path.display(),
                self.record + 1
            ))
        })?
        else {
            return Ok(None);
        };
        if self.dataset.format != Format::Lua {
            self.layout.checks.batch(&batch, self.record)?;
        }
        let mut arrays = batch.columns().to_vec();
        if let Some(dataset) = dataset_no {
            let mut keys = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 32);
            for row in 1..=batch.num_rows() {
                let key = order_key(dataset, self.record + row as u64);
                keys.append_value(std::str::from_utf8(&key).expect("ASCII order key"));
            }
            arrays.push(Arc::new(keys.finish()));
        }
        self.record += batch.num_rows() as u64;
        RecordBatch::try_new_with_options(
            schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map(Some)
        .map_err(DataFusionError::from)
        .map_err(Error::from)
    }
}
