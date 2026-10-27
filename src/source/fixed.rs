//! Decode disjoint fixed-record ranges concurrently, emitting them in file order.
use super::{
    SourceTable,
    batch::{BatchColumn, BatchRow, field_extra_bound, order_key},
};
use crate::field::ValueSink;
use crate::parallel::{CancelOnDrop, check_cancelled};
use crate::{
    config::Format,
    error::{Error, Result},
    framing,
};
use datafusion::{
    arrow::{
        array::{BinaryBuilder, Decimal128Builder, StringBuilder},
        datatypes::{DataType, SchemaRef},
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    common::ScalarValue,
    error::{DataFusionError, Result as DFResult},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use std::{fs::File, io::Read, sync::Arc};
use std::{
    io::{Seek, SeekFrom},
    sync::atomic::AtomicBool,
};

fn row_bound(source: &SourceTable) -> DFResult<usize> {
    let base = source
        .layout
        .schema
        .fields()
        .iter()
        .map(|f| ScalarValue::try_from(f.data_type()).map(|v| v.size()))
        .sum::<DFResult<usize>>()?;
    let bound = source.layout.fields.iter().fold(base, |size, field| {
        size.saturating_add(field_extra_bound(field, field.length.unwrap_or(0)))
    });
    Ok(bound.saturating_add(if source.order_name.is_some() {
        size_of::<ScalarValue>() + 32
    } else {
        0
    }))
}

pub(super) fn eligible(source: &SourceTable) -> DFResult<bool> {
    // Very wide/expanding records use the existing measured-size fallback.
    Ok(source.dataset.format == Format::Fixed
        && source.dataset.compression.is_none()
        && crate::parallel::worthwhile(&source.layout)
        && row_bound(source)? <= framing::MAX_RECORD)
}

struct FixedRanges {
    source: SourceTable,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    size: u64,
    stride: usize,
    batch_rows: usize,
}
impl FixedRanges {
    fn open(source: SourceTable, projection: Option<Vec<usize>>) -> Result<Self> {
        let file = File::open(&source.dataset.path)
            .map_err(|e| Error::input(e.to_string()).context(source.dataset.path.display()))?;
        let metadata = file.metadata().map_err(|e| Error::input(e.to_string()))?;
        if !metadata.is_file() {
            return Err(Error::config("input must be a regular file"));
        }
        let stride = source.layout.length + source.layout.separator.len();
        let batch_rows = 8192
            .min(framing::MAX_RECORD / row_bound(&source)?.max(1))
            .min(framing::MAX_RECORD / stride)
            .max(1);
        Ok(Self {
            schema: source.output_schema(),
            source,
            projection,
            size: metadata.len(),
            stride,
            batch_rows,
        })
    }
    fn batches(&self) -> u64 {
        self.size
            .div_ceil(self.stride as u64)
            .div_ceil(self.batch_rows as u64)
    }
    fn decode(&self, batch: u64, cancelled: &AtomicBool) -> Result<RecordBatch> {
        check_cancelled(cancelled)?;
        let first = batch * self.batch_rows as u64;
        let offset = first * self.stride as u64;
        let length = (self.size - offset).min((self.batch_rows * self.stride) as u64) as usize;
        let context = |e: Error, record: u64, position: u64| {
            e.context(format!(
                "input {} {} record {} byte {}",
                self.source.dataset.id,
                self.source.dataset.path.display(),
                record,
                position
            ))
        };
        // Independent descriptors avoid a shared seek cursor. The file is only
        // opened for the bounded range being converted, never once per record.
        let mut file = File::open(&self.source.dataset.path)
            .map_err(|e| context(Error::input(e.to_string()), first + 1, offset))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| context(Error::input(e.to_string()), first + 1, offset))?;
        let mut raw = vec![0; length];
        file.read_exact(&mut raw)
            .map_err(|e| context(Error::input(e.to_string()), first + 1, offset))?;
        let capacity = raw.len().div_ceil(self.stride);
        let mut widths = Vec::new();
        if self.source.layout.conditional.is_some() {
            widths.resize(self.source.layout.schema.fields().len(), 0);
        } else {
            for field in &self.source.layout.fields {
                widths.extend(std::iter::repeat_n(
                    field_extra_bound(field, field.length.unwrap_or(0)),
                    field.names().len(),
                ));
            }
        }
        if self.source.order_name.is_some() {
            widths.push(32);
        }
        let mut columns: Vec<_> = self
            .schema
            .fields()
            .iter()
            .zip(widths)
            .map(|(field, width)| match field.data_type() {
                DataType::Utf8 => {
                    BatchColumn::Text(StringBuilder::with_capacity(capacity, capacity * width))
                }
                DataType::Decimal128(_, _) => BatchColumn::Decimal(
                    Decimal128Builder::with_capacity(capacity)
                        .with_data_type(field.data_type().clone()),
                ),
                DataType::Binary => {
                    BatchColumn::Binary(BinaryBuilder::with_capacity(capacity, capacity * width))
                }
                _ => BatchColumn::Other(Vec::with_capacity(capacity)),
            })
            .collect();
        let mut rows = 0;
        for (index, record) in raw.chunks(self.stride).enumerate() {
            if index % 256 == 0 {
                check_cancelled(cancelled)?;
            }
            let number = first + index as u64 + 1;
            let position = offset + (index * self.stride) as u64;
            let fail = |e| context(e, number, position);
            let body = record
                .get(..self.source.layout.length)
                .ok_or_else(|| fail(Error::input("truncated record")))?;
            let separator = &record[body.len()..];
            // A final body without its separator is accepted by fixed_into.
            if !separator.is_empty() && separator.len() != self.source.layout.separator.len() {
                return Err(fail(Error::input("truncated record")));
            }
            if !separator.is_empty() && separator != self.source.layout.separator {
                return Err(fail(Error::input("fixed record separator mismatch")));
            }
            let mut row = BatchRow {
                columns: &mut columns,
                column: 0,
                bytes: 0,
            };
            self.source
                .layout
                .decode_physical(body, &mut row)
                .map_err(fail)?;
            if self.source.order_name.is_some() {
                let key = order_key(self.source.dataset_no, number);
                row.text(Some(std::str::from_utf8(&key).expect("ASCII order key")))?;
            }
            rows += 1;
        }
        let arrays = columns
            .into_iter()
            .map(BatchColumn::finish)
            .collect::<DFResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(DataFusionError::from)?;
        Ok(if let Some(projection) = &self.projection {
            batch.project(projection).map_err(DataFusionError::from)?
        } else {
            batch
        })
    }
}

pub(super) fn stream(
    source: SourceTable,
    projection: Option<Vec<usize>>,
    workers: usize,
) -> DFResult<SendableRecordBatchStream> {
    let schema = source.output_schema();
    let schema = Arc::new(if let Some(p) = &projection {
        schema.project(p)?
    } else {
        (*schema).clone()
    });
    let budget = source.conversion_budget.clone();
    let guard = CancelOnDrop::default();
    let cancelled = guard.0.clone();
    let stream = stream::once(async move {
        let ranges = tokio::task::spawn_blocking(move || FixedRanges::open(source, projection))
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let ranges = Arc::new(ranges);
        Ok::<_, DataFusionError>(
            stream::iter(0..ranges.batches())
                .map(move |index| {
                    let ranges = ranges.clone();
                    let budget = budget.clone();
                    let cancelled = cancelled.clone();
                    async move {
                        let permit = budget
                            .acquire_owned()
                            .await
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            ranges
                                .decode(index, &cancelled)
                                .map_err(|e| DataFusionError::External(Box::new(e)))
                        })
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?
                    }
                })
                .buffered(workers),
        )
    })
    .try_flatten()
    .boxed();
    // Dropping the consumer (LIMIT, error, signal) also stops already running
    // blocking jobs at the next record checkpoint.
    let stream = stream::unfold((stream, guard), |(mut stream, guard)| async move {
        stream.next().await.map(|item| (item, (stream, guard)))
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}
