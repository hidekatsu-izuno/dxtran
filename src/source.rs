mod csv;
mod fixed;
pub use csv::CsvReader;
use csv::CsvRecord;

use crate::{
    config::{Dataset, Format},
    encoding::TextReader,
    error::{Error, Result},
    field::{FieldType, ValueSink},
    framing,
    layout::RecordLayout,
};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, BinaryBuilder, Decimal128Builder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    catalog::{Session, TableProvider},
    common::ScalarValue,
    error::{DataFusionError, Result as DFResult},
    execution::TaskContext,
    logical_expr::{Expr, TableType},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::stream;
use std::{
    fmt,
    fs::File,
    io::{BufRead, BufReader, Read},
    sync::Arc,
};

#[derive(Debug, PartialEq)]
pub struct Cell {
    pub value: String,
    pub was_quoted: bool,
}

// Build common input columns directly; keep the general ScalarValue conversion
// for the remaining types. This avoids retaining a heap-allocated scalar per cell.
enum BatchColumn {
    Text(StringBuilder),
    Decimal(Decimal128Builder),
    Binary(BinaryBuilder),
    Other(Vec<ScalarValue>),
}
impl BatchColumn {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::Utf8 => Self::Text(StringBuilder::new()),
            DataType::Decimal128(_, _) => {
                Self::Decimal(Decimal128Builder::new().with_data_type(data_type.clone()))
            }
            DataType::Binary => Self::Binary(BinaryBuilder::new()),
            _ => Self::Other(Vec::new()),
        }
    }
    fn push(&mut self, value: ScalarValue) -> DFResult<()> {
        match (self, value) {
            (Self::Text(builder), ScalarValue::Utf8(value)) => builder.append_option(value),
            (Self::Decimal(builder), ScalarValue::Decimal128(value, _, _)) => {
                builder.append_option(value)
            }
            (Self::Binary(builder), ScalarValue::Binary(value)) => builder.append_option(value),
            (Self::Other(values), value) => values.push(value),
            _ => {
                return Err(DataFusionError::Internal(
                    "input value disagrees with its column type".into(),
                ));
            }
        }
        Ok(())
    }
    fn finish(self) -> DFResult<ArrayRef> {
        match self {
            Self::Text(mut builder) => Ok(Arc::new(builder.finish())),
            Self::Decimal(mut builder) => Ok(Arc::new(builder.finish())),
            Self::Binary(mut builder) => Ok(Arc::new(builder.finish())),
            Self::Other(values) => ScalarValue::iter_to_array(values),
        }
    }
}

struct BatchRow<'a> {
    columns: &'a mut [BatchColumn],
    column: usize,
    bytes: usize,
}

#[derive(Default)]
struct RowSize(usize);
impl ValueSink for RowSize {
    fn text(&mut self, value: Option<&str>) -> Result<()> {
        self.0 += size_of::<ScalarValue>() + value.map_or(0, str::len);
        Ok(())
    }
    fn decimal(&mut self, _: Option<i128>, _: u8, _: i8) -> Result<()> {
        self.0 += size_of::<ScalarValue>();
        Ok(())
    }
    fn binary(&mut self, value: Option<&[u8]>) -> Result<()> {
        self.0 += size_of::<ScalarValue>() + value.map_or(0, <[u8]>::len);
        Ok(())
    }
    fn scalar(&mut self, value: ScalarValue) -> Result<()> {
        self.0 += value.size();
        Ok(())
    }
}
impl ValueSink for BatchRow<'_> {
    fn text(&mut self, value: Option<&str>) -> Result<()> {
        let BatchColumn::Text(builder) = &mut self.columns[self.column] else {
            return Err(Error::sql("input string disagrees with its column type"));
        };
        builder.append_option(value);
        self.column += 1;
        self.bytes += size_of::<ScalarValue>() + value.map_or(0, str::len);
        Ok(())
    }
    fn decimal(&mut self, value: Option<i128>, _: u8, _: i8) -> Result<()> {
        let BatchColumn::Decimal(builder) = &mut self.columns[self.column] else {
            return Err(Error::sql("input decimal disagrees with its column type"));
        };
        builder.append_option(value);
        self.column += 1;
        self.bytes += size_of::<ScalarValue>();
        Ok(())
    }
    fn binary(&mut self, value: Option<&[u8]>) -> Result<()> {
        let BatchColumn::Binary(builder) = &mut self.columns[self.column] else {
            return Err(Error::sql("input binary disagrees with its column type"));
        };
        builder.append_option(value);
        self.column += 1;
        self.bytes += size_of::<ScalarValue>() + value.map_or(0, <[u8]>::len);
        Ok(())
    }
    fn scalar(&mut self, value: ScalarValue) -> Result<()> {
        self.bytes += value.size();
        self.columns[self.column].push(value)?;
        self.column += 1;
        Ok(())
    }
}

fn order_key(dataset: usize, record: u64) -> [u8; 32] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = [0; 32];
    for (number, part) in [dataset as u64, record]
        .into_iter()
        .zip(key.as_chunks_mut::<16>().0)
    {
        for (i, byte) in part.iter_mut().enumerate() {
            *byte = HEX[((number >> (60 - i * 4)) & 15) as usize];
        }
    }
    key
}

enum Reader {
    Csv(CsvReader<TextReader<crate::compression::Input>>),
    Physical(BufReader<crate::compression::Input>),
    Parquet(Box<crate::parquet_io::Input>),
}
pub struct InputReader {
    dataset: Dataset,
    pub layout: RecordLayout,
    reader: Reader,
    record: u64,
    byte_position: u64,
    body: Vec<u8>,
    cells: CsvRecord,
    record_ready: bool,
    record_position: u64,
    row_base_size: usize,
    physical_row_bound: usize,
}
impl InputReader {
    pub fn open(dataset: &Dataset, layout: &RecordLayout) -> Result<Self> {
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
        })
    }
    pub fn next_row(&mut self) -> Result<Option<Vec<ScalarValue>>> {
        let mut row = Vec::with_capacity(self.layout.schema.fields().len());
        Ok(self.next_row_into(&mut row)?.then_some(row))
    }
    pub fn next_row_into(&mut self, row: &mut Vec<ScalarValue>) -> Result<bool> {
        row.clear();
        if matches!(self.reader, Reader::Parquet(_)) {
            let Some(batch) = self.parquet_batch(self.layout.schema.clone(), None, 1)? else {
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
    fn read_record(&mut self) -> Result<bool> {
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
            Reader::Parquet(_) => unreachable!("parquet uses native Arrow batches"),
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
                if self.body.len() < self.layout.length
                    || !self.layout.rest && self.body.len() != self.layout.length
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
        Ok(true)
    }
    fn decode_record(&self, values: &mut impl ValueSink) -> Result<()> {
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
                    Some(value)
                };
                field.parse_text_to(text, values).map_err(|e| {
                    context(e.context(format!("column {}", field.spec.name.as_deref().unwrap())))
                })?;
            }
        } else {
            for field in &self.layout.fields {
                let end = field
                    .length
                    .map(|n| field.offset + n)
                    .unwrap_or(self.body.len());
                let bytes = self
                    .body
                    .get(field.offset..end)
                    .ok_or_else(|| context(Error::input("field lies outside record body")))?;
                field.decode_to(bytes, values).map_err(|e| {
                    context(e.context(format!(
                        "column {} offset {} width {}",
                        field.spec.name.as_deref().unwrap_or("FILLER"),
                        field.offset,
                        bytes.len()
                    )))
                })?;
            }
        }
        Ok(())
    }
    fn row_size_bound(&self) -> usize {
        if self.dataset.format == Format::Csv {
            // Decoded strings/binaries cannot exceed their UTF-8 CSV cell size.
            return self.row_base_size.saturating_add(self.cells.bytes());
        }
        let mut bound = self.physical_row_bound;
        if self.layout.rest {
            let field = self.layout.fields.last().unwrap();
            bound = bound.saturating_add(field_extra_bound(field, self.body.len() - field.offset));
        }
        bound
    }
    pub(crate) fn next_batch(
        &mut self,
        schema: SchemaRef,
        dataset_no: Option<usize>,
    ) -> Result<Option<RecordBatch>> {
        if matches!(self.reader, Reader::Parquet(_)) {
            return self.parquet_batch(schema, dataset_no, 8192);
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
    fn parquet_batch(
        &mut self,
        schema: SchemaRef,
        dataset_no: Option<usize>,
        max_rows: usize,
    ) -> Result<Option<RecordBatch>> {
        let Reader::Parquet(reader) = &mut self.reader else {
            unreachable!()
        };
        let Some(batch) = reader.take(max_rows).map_err(|e| {
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

fn field_extra_bound(field: &crate::field::CompiledField, width: usize) -> usize {
    match field.kind {
        FieldType::String if field.encoding.0 == encoding_rs::UTF_8 => width,
        FieldType::String => field
            .encoding
            .0
            .new_decoder_without_bom_handling()
            .max_utf8_buffer_length_without_replacement(width)
            .unwrap_or(usize::MAX),
        FieldType::Binary => width,
        _ => 0,
    }
}

#[derive(Clone, Debug)]
pub struct SourceTable {
    pub dataset: Dataset,
    pub layout: RecordLayout,
    pub dataset_no: usize,
    pub order_name: Option<String>,
    pub conversion_budget: Arc<tokio::sync::Semaphore>,
}
impl SourceTable {
    pub fn internal(&self, name: &str) -> Self {
        let mut s = self.clone();
        s.order_name = Some(name.into());
        s
    }
    pub fn output_schema(&self) -> SchemaRef {
        let mut fields = self.layout.schema.fields().to_vec();
        if let Some(name) = &self.order_name {
            fields.push(Arc::new(Field::new(name, DataType::Utf8, false)));
        }
        Arc::new(Schema::new(fields))
    }
}
#[async_trait]
impl TableProvider for SourceTable {
    fn schema(&self) -> SchemaRef {
        self.output_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SourceExec::new(self.clone(), projection.cloned())))
    }
}
#[derive(Debug)]
struct SourceExec {
    source: SourceTable,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}
impl SourceExec {
    fn new(source: SourceTable, projection: Option<Vec<usize>>) -> Self {
        let schema = source.output_schema();
        let schema = Arc::new(if let Some(p) = &projection {
            schema.project(p).unwrap()
        } else {
            (*schema).clone()
        });
        let orderings = source
            .order_name
            .as_ref()
            .and_then(|name| schema.index_of(name).ok())
            .and_then(|index| {
                datafusion::physical_expr::LexOrdering::new(vec![
                    datafusion::physical_expr::PhysicalSortExpr::new(
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            schema.field(index).name(),
                            index,
                        )),
                        datafusion::arrow::compute::SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    ),
                ])
            });
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            projection,
            properties,
        }
    }
}
impl DisplayAs for SourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DxtranSource: id={}, format={:?}",
            self.source.dataset.id, self.source.dataset.format
        )
    }
}
impl ExecutionPlan for SourceExec {
    fn name(&self) -> &str {
        "DxtranSource"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("source has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "invalid source partition".into(),
            ));
        }
        let limit = match context.memory_pool().memory_limit() {
            datafusion::execution::memory_pool::MemoryLimit::Finite(n) => n,
            _ => usize::MAX,
        };
        let workers = crate::parallel::workers(context.session_config().target_partitions(), limit);
        if workers > 1 && fixed::eligible(&self.source)? {
            return fixed::stream(self.source.clone(), self.projection.clone(), workers);
        }
        let source = self.source.clone();
        let projection = self.projection.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        std::thread::spawn(move || {
            let mut reader = match InputReader::open(&source.dataset, &source.layout) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                    return;
                }
            };
            let schema = source.output_schema();
            let dataset_no = source.order_name.as_ref().map(|_| source.dataset_no);
            loop {
                if tx.is_closed() {
                    return;
                }
                let permit = futures::executor::block_on(source.conversion_budget.acquire())
                    .expect("conversion semaphore");
                let result = reader
                    .next_batch(schema.clone(), dataset_no)
                    .and_then(|batch| {
                        batch
                            .map(|batch| {
                                if let Some(p) = &projection {
                                    batch
                                        .project(p)
                                        .map_err(DataFusionError::from)
                                        .map_err(Error::from)
                                } else {
                                    Ok(batch)
                                }
                            })
                            .transpose()
                    });
                drop(permit);
                match result {
                    Ok(Some(batch)) => {
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                        break;
                    }
                }
            }
        });
        let stream = stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|batch| (batch, rx))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_reader(
        mut config: serde_json::Value,
        data: &[u8],
    ) -> (tempfile::TempDir, InputReader, SchemaRef) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input");
        std::fs::write(&path, data).unwrap();
        config["path"] = json!(path);
        config["id"] = json!("data");
        let dataset: Dataset = serde_json::from_value(config).unwrap();
        let layout = RecordLayout::compile(&dataset, false, None).unwrap();
        let mut fields = layout.schema.fields().to_vec();
        fields.push(Arc::new(Field::new("order_key", DataType::Utf8, false)));
        let schema = Arc::new(Schema::new(fields));
        let reader = InputReader::open(&dataset, &layout).unwrap();
        (dir, reader, schema)
    }

    #[test]
    fn direct_batches_preserve_all_field_types_and_nulls() {
        let cases = [
            ("string", "41422020"),
            ("decimal", "202020313233"),
            ("boolean", "7472756520"),
            ("date", "323032362d31302d3235"),
            ("time", "31323a33343a35362e313233343536"),
            (
                "timestamp",
                "323032362d31302d32355431323a33343a35362e3132333435365a",
            ),
            ("binary", "00ff"),
            ("bits", "8001"),
            ("int-le", "feff"),
            ("int-be", "fffe"),
            ("uint-le", "0201"),
            ("uint-be", "0102"),
            ("float-le", "0000803f"),
            ("float-be", "3ff0000000000000"),
            ("hfloat-le", "0000000000001041"),
            ("hfloat-be", "41100000000000003300000000000000"),
            ("zoned", "f1f2c3"),
            ("lzoned", "4ef1f2f3"),
            ("tzoned", "f1f2f34e"),
            ("uzoned", "f1f2f3"),
            ("packed", "123c"),
            ("upacked", "123f"),
            ("npacked", "0123"),
        ];
        let mut fields = Vec::new();
        let mut normal = Vec::new();
        for (i, (kind, hex)) in cases.into_iter().enumerate() {
            let bytes = hex::decode(hex).unwrap();
            let mut field = json!({"name":format!("f{i}"),"type":kind,"length":bytes.len(),"nullable":true,"null_bytes":"ff".repeat(bytes.len())});
            if kind == "bits" {
                field["bits"] = json!([{"name":"b0","bit":0},{"name":"b15","bit":15}]);
            }
            fields.push(field);
            normal.extend(bytes);
        }
        let null = vec![255; normal.len()];
        let raw = [normal.as_slice(), &null, &normal].concat();
        for format in ["fixed", "rdw"] {
            let mut framed = Vec::new();
            if format == "fixed" {
                framed.extend_from_slice(&raw);
            } else {
                for bytes in [&normal, &null, &normal] {
                    framing::write_rdw(&mut framed, bytes, 32760).unwrap();
                }
            }
            let mut config = json!({"format":format,"encoding":"utf-8","fields":fields});
            if format == "fixed" {
                config["fixed"] = json!({"record_separator":"none"});
            }
            let (_dir, mut reader, schema) = test_reader(config, &framed);
            let mut row_reader = InputReader::open(&reader.dataset, &reader.layout).unwrap();
            let batch = reader.next_batch(schema.clone(), Some(7)).unwrap().unwrap();
            assert_eq!(batch.num_rows(), 3);
            for row in 0..3 {
                let mut expected = row_reader.next_row().unwrap().unwrap();
                expected.push(ScalarValue::Utf8(Some(format!(
                    "{:016x}{:016x}",
                    7,
                    row + 1
                ))));
                let actual = batch
                    .columns()
                    .iter()
                    .map(|c| ScalarValue::try_from_array(c, row).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{format} row {row}");
                if row == 1 {
                    assert!(actual[..actual.len() - 1].iter().all(ScalarValue::is_null));
                }
            }
            assert!(reader.next_batch(schema, Some(7)).unwrap().is_none());
        }
    }

    #[test]
    fn direct_decoding_preserves_text_null_and_encoding_rules() {
        for encoding in ["utf-8", "shift_jis", "utf-16le", "utf-16be", "iso-2022-jp"] {
            let encoding = crate::encoding::TextEncoding::resolve(encoding).unwrap();
            let data = encoding.encode("日本　").unwrap();
            let config = json!({"format":"fixed","encoding":encoding.0.name(),"fixed":{"record_separator":"none"},"fields":[{"name":"text","length":data.len(),"pad_char":"　"}]});
            let (_dir, mut reader, schema) = test_reader(config, &data);
            let batch = reader.next_batch(schema, Some(1)).unwrap().unwrap();
            assert_eq!(
                ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
                ScalarValue::Utf8(Some("日本".into()))
            );
        }
        let config = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"n","type":"decimal","length":8,"precision":6,"scale":2,"nullable":true,"null_if_pattern":"NULL[0-9]+"}]});
        let (_dir, mut reader, schema) = test_reader(config.clone(), b"NULL123   123.45");
        let batch = reader.next_batch(schema, Some(1)).unwrap().unwrap();
        assert!(
            ScalarValue::try_from_array(batch.column(0), 0)
                .unwrap()
                .is_null()
        );
        assert_eq!(
            ScalarValue::try_from_array(batch.column(0), 1).unwrap(),
            ScalarValue::Decimal128(Some(12345), 6, 2)
        );
        let (_dir, mut reader, schema) = test_reader(config, b"  123.45NULLIFY ");
        let error = reader.next_batch(schema, Some(1)).unwrap_err();
        assert_eq!(error.code, 3);
        assert!(error.message.contains("record 2 byte 8"));
        assert!(error.message.contains("column n offset 0 width 8"));
    }

    #[test]
    fn conservative_bound_does_not_reject_large_trimmed_records() {
        let config = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"text","length":framing::MAX_RECORD}]});
        let (_dir, mut reader, schema) =
            test_reader(config.clone(), &vec![b' '; framing::MAX_RECORD]);
        let batch = reader.next_batch(schema.clone(), Some(1)).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            ScalarValue::try_from_array(batch.column(0), 0).unwrap(),
            ScalarValue::Utf8(Some(String::new()))
        );
        assert!(reader.next_batch(schema, Some(1)).unwrap().is_none());
        let (_dir, mut reader, schema) = test_reader(config, &vec![b'x'; framing::MAX_RECORD]);
        assert_eq!(reader.next_batch(schema, Some(1)).unwrap_err().code, 3);
    }

    #[test]
    fn read_ahead_record_is_validated_before_returning_a_full_batch() {
        let width = framing::MAX_RECORD / 2;
        let config = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"text","length":width-2,"trim":"none"},{"name":"number","length":2,"type":"uzoned","zone":"ascii"}]});
        let mut bytes = vec![b'x'; width * 2];
        bytes[width - 2..width].copy_from_slice(b"12");
        bytes[width * 2 - 2..].copy_from_slice(b"1x");
        let (_dir, mut reader, schema) = test_reader(config, &bytes);
        let error = reader.next_batch(schema, Some(1)).unwrap_err();
        assert_eq!(error.code, 3);
        assert!(error.message.contains("record 2"));
        assert!(error.message.contains("column number"));
    }

    #[test]
    fn order_key_preserves_hex_format_and_boundaries() {
        for dataset in [1, 15, 16, u32::MAX as usize] {
            for record in [0, 15, 16, u32::MAX as u64, u64::MAX] {
                assert_eq!(
                    std::str::from_utf8(&order_key(dataset, record)).unwrap(),
                    format!("{dataset:016x}{record:016x}")
                );
                if record < u64::MAX {
                    assert!(order_key(dataset, record) < order_key(dataset, record + 1));
                }
            }
        }
    }
    #[test]
    fn csv_null_and_empty() {
        let mut r = CsvReader::new(&b"a,,\"\",\"x\ny\"\r\n\n\"a\"\"b\",\n"[..], b',', b'"');
        let row = r.next_record().unwrap().unwrap();
        assert_eq!(
            row[1],
            Cell {
                value: "".into(),
                was_quoted: false
            }
        );
        assert_eq!(
            row[2],
            Cell {
                value: "".into(),
                was_quoted: true
            }
        );
        assert_eq!(row[3].value, "x\ny");
        assert_eq!(r.next_record().unwrap().unwrap().len(), 1);
        assert_eq!(r.next_record().unwrap().unwrap()[0].value, "a\"b");
        assert!(r.next_record().unwrap().is_none());
    }
    #[test]
    fn csv_errors() {
        for data in [&b"\"abc"[..], &b"\"a\"x\n"[..], &b"a\r"[..]] {
            assert!(CsvReader::new(data, b',', b'"').next_record().is_err());
        }
    }
}
