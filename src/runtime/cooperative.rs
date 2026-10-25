//! Make each physical operator yield between batches, including operators whose
//! children can repeatedly return ready batches without touching Tokio I/O.
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    common::tree_node::TreeNodeRecursion,
    error::Result,
    execution::TaskContext,
    physical_expr::PhysicalExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream,
    },
};
use futures::Stream;
use std::{
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

struct CooperativeStream {
    inner: SendableRecordBatchStream,
    yield_next: bool,
}
impl Stream for CooperativeStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if super::signals::interrupted() {
            return Poll::Ready(Some(Err(datafusion::error::DataFusionError::External(
                Box::new(crate::error::Error::sql("interrupted")),
            ))));
        }
        if self.yield_next {
            self.yield_next = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let result = self.inner.as_mut().poll_next(cx);
        if matches!(result, Poll::Ready(Some(_))) {
            self.yield_next = true;
        }
        result
    }
}
impl RecordBatchStream for CooperativeStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
#[derive(Debug)]
struct CooperativeExec {
    inner: Arc<dyn ExecutionPlan>,
}
impl DisplayAs for CooperativeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("DxtranCooperative")
    }
}
impl ExecutionPlan for CooperativeExec {
    fn name(&self) -> &str {
        "DxtranCooperative"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "cooperative operator requires one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            inner: children.remove(0),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(CooperativeStream {
            inner: self.inner.execute(partition, context)?,
            yield_next: false,
        }))
    }
}
pub fn wrap(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|p| wrap(p.clone()))
        .collect::<Result<Vec<_>>>()?;
    let inner =
        datafusion::physical_plan::execution_plan::replace_children_if_necessary(plan, children)?;
    Ok(Arc::new(CooperativeExec { inner }))
}
