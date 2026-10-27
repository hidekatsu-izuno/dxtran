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
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<(Vec<u8>, usize)>(1);
    let (finished, completion) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("dxtran-fixed-writer".into())
        .spawn(move || {
            let result = (|| {
                let mut sink = sink;
                while let Some((bytes, rows)) = receiver.blocking_recv() {
                    // Compression consumes CPU on this writer thread too.
                    let _permit = if compressed {
                        Some(
                            futures::executor::block_on(writer_budget.acquire())
                                .map_err(|e| Error::sql(e.to_string()))?,
                        )
                    } else {
                        None
                    };
                    sink.write_fixed_bytes(&bytes, rows)?;
                }
                Ok::<_, Error>(sink)
            })();
            let _ = finished.send(result);
        })
        .map_err(|e| Error::output(e.to_string()))?;
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
    futures::pin_mut!(encoded);
    let mut conversion_error = None;
    while let Some(result) = encoded.next().await {
        match result {
            Ok(chunk) => {
                if sender.send(chunk).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                conversion_error = Some(error);
                break;
            }
        }
    }
    drop(sender);
    // Await earlier writes before reporting a later conversion error. Nothing
    // is published until this and the normal flush/fsync both succeed.
    let sink = completion
        .await
        .map_err(|e| Error::output(e.to_string()))??;
    if let Some(error) = conversion_error {
        return Err(error);
    }
    Ok(sink)
}
