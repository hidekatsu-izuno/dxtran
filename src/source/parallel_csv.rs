//! Keep CSV/charset framing sequential; convert bounded parsed chunks in parallel.
use super::{
    InputReader, SourceTable,
    batch::{BatchColumn, BatchRow, RowSize, order_key},
};
use crate::field::ValueSink;
use crate::parallel::{CancelOnDrop, check_cancelled};
use crate::{
    error::{Error, Result},
    framing,
};
use datafusion::{
    arrow::record_batch::{RecordBatch, RecordBatchOptions},
    common::ScalarValue,
    error::{DataFusionError, Result as DFResult},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;
use futures::stream;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

struct Chunk {
    first: u64,
    text: String,
    ends: Vec<(usize, bool)>,
    rows: usize,
    error: Option<Error>,
}
impl Chunk {
    fn read(reader: &mut InputReader, order: bool, cancelled: &AtomicBool) -> Result<Option<Self>> {
        let mut chunk = Self {
            first: reader.record + u64::from(!reader.record_ready),
            text: String::new(),
            ends: Vec::new(),
            rows: 0,
            error: None,
        };
        let mut bytes = 0;
        let order_size = if order {
            size_of::<ScalarValue>() + 32
        } else {
            0
        };
        let result = (|| -> Result<()> {
            while chunk.rows < 8192 && bytes < framing::MAX_RECORD {
                if chunk.rows.is_multiple_of(256) {
                    check_cancelled(cancelled)?;
                }
                if !reader.read_record()? {
                    break;
                }
                let mut bound = reader.row_size_bound().saturating_add(order_size);
                if bound > framing::MAX_RECORD {
                    let mut size = RowSize::default();
                    reader.decode_record(&mut size)?;
                    bound = size.0 + order_size;
                    if bound > framing::MAX_RECORD {
                        return Err(Error::input("decoded record exceeds 16 MiB"));
                    }
                }
                if chunk.rows > 0 && bytes + bound > framing::MAX_RECORD {
                    // As on the serial path, validate the read-ahead record
                    // even if a downstream LIMIT stops after this chunk.
                    reader.decode_record(&mut RowSize::default())?;
                    break;
                }
                let offset = chunk.text.len();
                chunk.text.push_str(&reader.cells.text);
                chunk.ends.extend(
                    reader
                        .cells
                        .ends
                        .iter()
                        .map(|&(end, quoted)| (offset + end, quoted)),
                );
                chunk.rows += 1;
                bytes += bound;
                reader.record_ready = false;
            }
            Ok(())
        })();
        // A syntax error later in a chunk must not hide an earlier type error.
        chunk.error = result.err();
        if chunk.rows == 0 {
            return match chunk.error {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        Ok(Some(chunk))
    }
    fn decode(
        self,
        source: &SourceTable,
        projection: &Option<Vec<usize>>,
        cancelled: &AtomicBool,
    ) -> Result<RecordBatch> {
        let schema = source.output_schema();
        let mut columns: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| BatchColumn::new(f.data_type()))
            .collect();
        let mut start = 0;
        for (index, cells) in self.ends.chunks(source.layout.fields.len()).enumerate() {
            if index % 256 == 0 {
                check_cancelled(cancelled)?;
            }
            let record = self.first + index as u64;
            let mut row = BatchRow {
                columns: &mut columns,
                column: 0,
                bytes: 0,
            };
            for (&(end, quoted), field) in cells.iter().zip(&source.layout.fields) {
                let value = &self.text[start..end];
                start = end;
                let text = if value.is_empty() && !quoted {
                    None
                } else {
                    Some(field.trim_text(value))
                };
                field.parse_text_to(text, &mut row).map_err(|e| {
                    e.context(format!(
                        "input {} {} record {} byte 0: column {}",
                        source.dataset.id,
                        source.dataset.path.display(),
                        record,
                        field.spec.name.as_deref().unwrap()
                    ))
                })?;
            }
            if source.order_name.is_some() {
                let key = order_key(source.dataset_no, record);
                row.text(Some(std::str::from_utf8(&key).expect("ASCII order key")))?;
            }
        }
        if let Some(error) = self.error {
            return Err(error);
        }
        let arrays = columns
            .into_iter()
            .map(BatchColumn::finish)
            .collect::<DFResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            schema,
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(self.rows)),
        )
        .map_err(DataFusionError::from)?;
        Ok(if let Some(p) = projection {
            batch.project(p).map_err(DataFusionError::from)?
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
    let guard = CancelOnDrop::default();
    let cancelled = guard.0.clone();
    let parser_cancelled = cancelled.clone();
    let parser_source = source.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    std::thread::Builder::new()
        .name("dxtran-csv-reader".into())
        .spawn(move || {
            let result = (|| -> Result<()> {
                let mut reader = InputReader::open(&parser_source.dataset, &parser_source.layout)?;
                while !tx.is_closed() {
                    let permit =
                        futures::executor::block_on(parser_source.conversion_budget.acquire())
                            .map_err(|e| Error::sql(e.to_string()))?;
                    let chunk = Chunk::read(
                        &mut reader,
                        parser_source.order_name.is_some(),
                        &parser_cancelled,
                    );
                    drop(permit);
                    let Some(chunk) = chunk? else {
                        break;
                    };
                    let failed = chunk.error.is_some();
                    if tx.blocking_send(Ok(chunk)).is_err() || failed {
                        break;
                    }
                }
                Ok(())
            })();
            if let Err(e) = result {
                let _ = tx.blocking_send(Err(e));
            }
        })
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let source = Arc::new(source);
    let chunks = stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|chunk| (chunk, rx))
    });
    let batches = chunks
        .map(move |chunk| {
            let source = source.clone();
            let projection = projection.clone();
            let cancelled = cancelled.clone();
            async move {
                let chunk = chunk?;
                let permit = source
                    .conversion_budget
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::sql(e.to_string()))?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    chunk.decode(&source, &projection, &cancelled)
                })
                .await
                .map_err(|e| Error::input(e.to_string()))?
            }
        })
        .buffered(workers)
        .boxed();
    let batches = stream::unfold((batches, guard), |(mut batches, guard)| async {
        batches.next().await.map(|batch| {
            (
                batch.map_err(|e| DataFusionError::External(Box::new(e))),
                (batches, guard),
            )
        })
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}
