//! Shared limits and cancellation for bounded file conversion pipelines.
use crate::{
    error::{Error, Result},
    field::FieldType,
    framing::MAX_RECORD,
    layout::RecordLayout,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

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
