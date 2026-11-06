//! Bounded ordered writes shared by the parallel output encoders.
use crate::{
    error::{Error, Result},
    sink::OutputSink,
};
use futures::{Stream, StreamExt};
use std::sync::Arc;

pub(super) async fn write_chunks<T: Send + 'static>(
    sink: OutputSink,
    chunks: impl Stream<Item = Result<(T, usize)>>,
    budget: Arc<tokio::sync::Semaphore>,
    uses_cpu: bool,
    name: &'static str,
    mut write: impl FnMut(&mut OutputSink, &T, usize) -> Result<()> + Send + 'static,
) -> Result<OutputSink> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<(T, usize)>(1);
    let (finished, completion) = tokio::sync::oneshot::channel();
    // Disk waits must not occupy the conversion pool or stop polling producers.
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let result: Result<OutputSink> = (|| {
                let mut sink = sink;
                while let Some((chunk, rows)) = receiver.blocking_recv() {
                    let _permit = if uses_cpu {
                        Some(
                            futures::executor::block_on(budget.acquire())
                                .map_err(|e| Error::sql(e.to_string()))?,
                        )
                    } else {
                        None
                    };
                    write(&mut sink, &chunk, rows)?;
                }
                Ok(sink)
            })();
            let _ = finished.send(result);
        })
        .map_err(|e| Error::output(e.to_string()))?;
    futures::pin_mut!(chunks);
    let mut conversion_error = None;
    while let Some(result) = chunks.next().await {
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
    // Earlier writes take precedence over a later conversion failure.
    let sink = completion
        .await
        .map_err(|e| Error::output(e.to_string()))??;
    if let Some(error) = conversion_error {
        return Err(error);
    }
    Ok(sink)
}
