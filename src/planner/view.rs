//! Keep process references named until deterministic order is propagated.
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider, view::ViewTable},
    error::Result,
    logical_expr::{Expr, LogicalPlanBuilder, TableType},
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct ProcessView {
    public: ViewTable,
    ordered: super::order::Ordered,
}

impl ProcessView {
    pub(super) fn new(ordered: super::order::Ordered) -> Result<Self> {
        // The original logical plan owns references to earlier ProcessViews,
        // causing recursive cleanup of long dependency chains. Keep only the
        // expanded ordered plan, with private columns removed from the public view.
        let plan = LogicalPlanBuilder::from(ordered.plan.clone())
            .project(super::order::public_columns(&ordered.plan))?
            .build()?;
        Ok(Self {
            public: ViewTable::new(plan, None),
            ordered,
        })
    }
    pub(super) fn ordered(&self) -> &super::order::Ordered {
        &self.ordered
    }
}

#[async_trait]
impl TableProvider for ProcessView {
    // The default get_logical_plan returns None. ViewTable's implementation
    // would make SQL planning inline the whole dependency chain at every step.
    fn schema(&self) -> SchemaRef {
        self.public.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.public.scan(state, projection, filters, limit).await
    }
}
