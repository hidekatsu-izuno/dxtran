//! DataFusion table/physical-plan adapter and conversion strategy selection.
use super::{InputReader, fixed, parallel_csv, parquet};
use crate::{
    config::{Dataset, Format},
    error::Error,
    layout::RecordLayout,
};
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::{Session, TableProvider},
    error::{DataFusionError, Result as DFResult},
    execution::TaskContext,
    logical_expr::{Expr, TableType},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::stream;
use std::{fmt, sync::Arc};

#[derive(Clone, Debug)]
pub struct SourceTable {
    pub dataset: Dataset,
    pub layout: RecordLayout,
    pub dataset_no: usize,
    pub order_name: Option<String>,
    pub conversion_budget: Arc<tokio::sync::Semaphore>,
}
impl SourceTable {
    pub fn internal(&self, name: &str) -> Self {
        let mut s = self.clone();
        s.order_name = Some(name.into());
        s
    }
    pub fn output_schema(&self) -> SchemaRef {
        let mut fields = self.layout.schema.fields().to_vec();
        if let Some(name) = &self.order_name {
            fields.push(Arc::new(Field::new(name, DataType::Utf8, false)));
        }
        Arc::new(Schema::new(fields))
    }
}
#[async_trait]
impl TableProvider for SourceTable {
    fn schema(&self) -> SchemaRef {
        self.output_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SourceExec::new(self.clone(), projection.cloned())))
    }
}
#[derive(Debug)]
struct SourceExec {
    source: SourceTable,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}
impl SourceExec {
    fn new(source: SourceTable, projection: Option<Vec<usize>>) -> Self {
        let schema = source.output_schema();
        let schema = Arc::new(if let Some(p) = &projection {
            schema.project(p).unwrap()
        } else {
            (*schema).clone()
        });
        let orderings = source
            .order_name
            .as_ref()
            .and_then(|name| schema.index_of(name).ok())
            .and_then(|index| {
                datafusion::physical_expr::LexOrdering::new(vec![
                    datafusion::physical_expr::PhysicalSortExpr::new(
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            schema.field(index).name(),
                            index,
                        )),
                        datafusion::arrow::compute::SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    ),
                ])
            });
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            source,
            projection,
            properties,
        }
    }
}
impl DisplayAs for SourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DxtranSource: id={}, format={:?}",
            self.source.dataset.id, self.source.dataset.format
        )
    }
}
impl ExecutionPlan for SourceExec {
    fn name(&self) -> &str {
        "DxtranSource"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("source has no children".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "invalid source partition".into(),
            ));
        }
        let limit = match context.memory_pool().memory_limit() {
            datafusion::execution::memory_pool::MemoryLimit::Finite(n) => n,
            _ => usize::MAX,
        };
        // Checks have ordered logging/abort effects. Keep a checked input's
        // decode and checks together so a later codec error cannot hide an
        // earlier check, and parallel read-ahead cannot emit later messages.
        let workers = if self.source.layout.checks.is_empty() {
            crate::parallel::workers(context.session_config().target_partitions(), limit)
        } else {
            1
        };
        if workers > 1 && fixed::eligible(&self.source)? {
            return fixed::stream(self.source.clone(), self.projection.clone(), workers);
        }
        if workers > 1 && self.source.dataset.format == Format::Parquet {
            return parquet::stream(self.source.clone(), self.projection.clone(), workers);
        }
        if workers > 1
            && self.source.dataset.format == Format::Csv
            && crate::parallel::worthwhile(&self.source.layout)
        {
            return parallel_csv::stream(self.source.clone(), self.projection.clone(), workers);
        }
        let source = self.source.clone();
        let projection = self.projection.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        std::thread::spawn(move || {
            let mut reader = match InputReader::open(&source.dataset, &source.layout) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                    return;
                }
            };
            let schema = source.output_schema();
            let dataset_no = source.order_name.as_ref().map(|_| source.dataset_no);
            loop {
                if tx.is_closed() {
                    return;
                }
                let permit = futures::executor::block_on(source.conversion_budget.acquire())
                    .expect("conversion semaphore");
                let result = reader
                    .next_batch(schema.clone(), dataset_no)
                    .and_then(|batch| {
                        batch
                            .map(|batch| {
                                if let Some(p) = &projection {
                                    batch
                                        .project(p)
                                        .map_err(DataFusionError::from)
                                        .map_err(Error::from)
                                } else {
                                    Ok(batch)
                                }
                            })
                            .transpose()
                    });
                drop(permit);
                match result {
                    Ok(Some(batch)) => {
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(DataFusionError::External(Box::new(e))));
                        break;
                    }
                }
            }
        });
        let stream = stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|batch| (batch, rx))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
