//! Convert fixed output chunks in parallel and commit bytes in stream order.
use super::original_error;
use crate::{
    error::{Error, Result},
    sink,
};
use crate::{parallel::CancelOnDrop, sink::PhysicalEncoder};
use datafusion::{arrow::record_batch::RecordBatch, physical_plan::SendableRecordBatchStream};
use futures::StreamExt;
use futures::stream;
use std::sync::Arc;

pub(super) async fn write(
    sink: sink::OutputSink,
    stream: SendableRecordBatchStream,
    encoder: Arc<PhysicalEncoder>,
    budget: Arc<tokio::sync::Semaphore>,
    workers: usize,
) -> Result<sink::OutputSink> {
    let start_row = sink.rows;
    let compressed = sink.is_compressed();
    let writer_budget = budget.clone();
    // Keep disk writes independent of the CPU pool. Waiting for each blocking
    // write in the producer stopped polling/scheduling conversions during I/O.
    let guard = CancelOnDrop::default();
    let cancelled = guard.0.clone();
    let batch_rows = encoder.batch_rows();
    // Large fixed layouts may expand a small Arrow batch beyond 16 MiB.
    // Slice before scheduling work so each encoded buffer remains bounded.
    let chunks = stream::try_unfold(
        (stream, None::<RecordBatch>, 0usize, start_row),
        move |(mut stream, mut batch, mut offset, start_row)| async move {
            while batch.as_ref().is_none_or(|b| offset >= b.num_rows()) {
                batch = match stream.next().await {
                    Some(result) => Some(result.map_err(original_error)?),
                    None => return Ok::<_, Error>(None),
                };
                offset = 0;
            }
            let current = batch.as_ref().unwrap();
            let rows = batch_rows.min(current.num_rows() - offset);
            let chunk = current.slice(offset, rows);
            Ok(Some((
                (start_row, chunk),
                (stream, batch, offset + rows, start_row + rows as u64),
            )))
        },
    );
    let encoded = chunks
        .map(move |chunk| {
            let encoder = encoder.clone();
            let budget = budget.clone();
            let cancelled = cancelled.clone();
            async move {
                let (start_row, batch) = chunk?;
                let rows = batch.num_rows();
                let permit = budget
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::sql(e.to_string()))?;
                let bytes = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    encoder.encode_fixed(&batch, start_row, &cancelled)
                })
                .await
                .map_err(|e| Error::output(e.to_string()))??;
                Ok::<_, Error>((bytes, rows))
            }
        })
        .buffered(workers);
    super::writer::write_chunks(
        sink,
        encoded,
        writer_budget,
        compressed,
        "dxtran-fixed-writer",
        |sink, chunk: &Vec<u8>, rows| sink.write_fixed_bytes(chunk, rows),
    )
    .await
}
