//! Shared limits and cancellation for bounded file conversion pipelines.
use crate::{
    error::{Error, Result},
    field::FieldType,
    framing::MAX_RECORD,
    layout::RecordLayout,
};
use futures::{Stream, StreamExt};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Task(Option<tokio::task::JoinHandle<Result<()>>>);
impl Drop for Task {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

/// Run bounded producers ahead of the consumer, but drain each in input order.
/// Each producer holds at most one queued item and one item being built. Never
/// block a CPU worker on a channel: producers send only after releasing permits.
pub(crate) fn ordered<S, W, T, F, Fut>(
    work: S,
    workers: usize,
    produce: F,
) -> impl Stream<Item = Result<T>>
where
    S: Stream<Item = Result<W>>,
    W: Send + 'static,
    T: Send + 'static,
    F: Fn(W, tokio::sync::mpsc::Sender<T>, Arc<AtomicBool>) -> Fut + Clone,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let guard = CancelOnDrop::default();
    let cancelled = guard.0.clone();
    let streams = work
        .map(move |work| {
            let produce = produce.clone();
            let cancelled = cancelled.clone();
            async move {
                let (tx, mut rx) = tokio::sync::mpsc::channel(1);
                // Construct the future before spawning, so F itself need not
                // live on a worker. A failed work item is still ordered.
                let future = work.map(|work| produce(work, tx, cancelled));
                let task = Task(Some(tokio::spawn(async move { future?.await })));
                let first = rx.recv().await;
                stream_inner(first, rx, task)
            }
        })
        .buffered(workers)
        .flatten();
    // Dropping LIMIT/error consumers also cancels already running CPU work.
    futures::stream::unfold(
        (Box::pin(streams), guard),
        |(mut streams, guard)| async move { streams.next().await.map(|item| (item, (streams, guard))) },
    )
}

fn stream_inner<T: Send>(
    first: Option<T>,
    rx: tokio::sync::mpsc::Receiver<T>,
    task: Task,
) -> impl Stream<Item = Result<T>> {
    futures::stream::try_unfold(
        (first, rx, task),
        |(mut first, mut rx, mut task)| async move {
            if let Some(item) = first.take() {
                return Ok(Some((item, (None, rx, task))));
            }
            if let Some(item) = rx.recv().await {
                return Ok(Some((item, (None, rx, task))));
            }
            task.0
                .take()
                .expect("producer task")
                .await
                .map_err(|e| Error::sql(e.to_string()))??;
            Ok(None)
        },
    )
}

pub(crate) fn workers(threads: usize, memory_limit: usize) -> usize {
    // Conversion buffers are outside DataFusion's pool. Keep their concurrency
    // conservative for small pools, without claiming a process-wide RSS limit.
    threads.min((memory_limit / (2 * MAX_RECORD)).max(1)).max(1)
}

pub(crate) fn worthwhile(layout: &RecordLayout) -> bool {
    // A single plain UTF-8/string or binary field is mostly memory/I/O work.
    // Scheduling and buffering made that case slower in the fixed-I/O benchmark.
    !matches!(layout.fields.as_slice(), [field]
        if (field.kind == FieldType::String && field.encoding.0 == encoding_rs::UTF_8
            && field.spec.null_if_pattern.is_none()) || field.kind == FieldType::Binary)
}

#[derive(Default)]
pub(crate) struct CancelOnDrop(pub Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
pub(crate) fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    if cancelled.load(Ordering::Relaxed) {
        Err(Error::sql("conversion cancelled"))
    } else {
        Ok(())
    }
}
