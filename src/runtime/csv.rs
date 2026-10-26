//! Format CSV batches concurrently; encode text and write in original order.
use super::*;
use crate::{parallel::ordered, sink::CsvEncoder};
use datafusion::physical_plan::SendableRecordBatchStream;

pub(super) async fn write(
    sink: sink::OutputSink,
    stream: SendableRecordBatchStream,
    encoder: Arc<CsvEncoder>,
    budget: Arc<tokio::sync::Semaphore>,
    workers: usize,
) -> Result<sink::OutputSink> {
    let start_row = sink.rows;
    let writer_uses_cpu = sink.csv_writer_uses_cpu();
    let writer_budget = budget.clone();
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<(String, usize)>(1);
    let (finished, completion) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("dxtran-csv-writer".into())
        .spawn(move || {
            let result = (|| {
                let mut sink = sink;
                while let Some((text, rows)) = receiver.blocking_recv() {
                    let _permit = if writer_uses_cpu {
                        Some(
                            futures::executor::block_on(writer_budget.acquire())
                                .map_err(|e| Error::sql(e.to_string()))?,
                        )
                    } else {
                        None
                    };
                    sink.write_csv_text(&text, rows)?;
                }
                Ok::<_, Error>(sink)
            })();
            let _ = finished.send(result);
        })
        .map_err(|e| Error::output(e.to_string()))?;
    let mut row = start_row;
    let batches = stream.map(move |batch| {
        batch.map_err(original_error).map(|batch| {
            let start = row;
            row += batch.num_rows() as u64;
            (start, batch)
        })
    });
    let encoded = ordered(batches, workers, move |(start, batch), tx, cancelled| {
        let encoder = encoder.clone();
        let budget = budget.clone();
        async move {
            let mut offset = 0;
            while offset < batch.num_rows() && !tx.is_closed() {
                let permit = budget
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| Error::sql(e.to_string()))?;
                let encoder = encoder.clone();
                let batch = batch.clone();
                let cancelled = cancelled.clone();
                let (text, next) = tokio::task::spawn_blocking(move || -> Result<_> {
                    let _permit = permit;
                    let mut text = String::new();
                    let next = encoder.encode(
                        &batch,
                        offset,
                        start,
                        &mut text,
                        1024 * 1024,
                        Some(&cancelled),
                    )?;
                    Ok((text, next))
                })
                .await
                .map_err(|e| Error::output(e.to_string()))??;
                if tx.send((text, next - offset)).await.is_err() {
                    break;
                }
                offset = next;
            }
            Ok(())
        }
    });
    futures::pin_mut!(encoded);
    let mut conversion_error = None;
    while let Some(result) = encoded.next().await {
        match result {
            Ok(chunk) => {
                if sender.send(chunk).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                conversion_error = Some(e);
                break;
            }
        }
    }
    drop(sender);
    let sink = completion
        .await
        .map_err(|e| Error::output(e.to_string()))??;
    if let Some(error) = conversion_error {
        return Err(error);
    }
    Ok(sink)
}
