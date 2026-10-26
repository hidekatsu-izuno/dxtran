//! Attach element ordinals before a physical optimizer can reorder expanded rows.
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, AsArray, StringBuilder},
        compute::cast,
        datatypes::{DataType, SchemaRef},
        record_batch::RecordBatch,
    },
    catalog::Session,
    common::{Column, DFSchemaRef, UnnestOptions, tree_node::TreeNodeRecursion},
    error::{DataFusionError, Result},
    execution::{TaskContext, context::QueryPlanner},
    logical_expr::{
        Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
        logical_plan::Unnest, physical_planning_context::PhysicalPlanningContext,
    },
    physical_expr::{EquivalenceProperties, PhysicalExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
        unnest::{ListUnnest, UnnestExec},
    },
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use futures::StreamExt;
use std::{
    fmt::{self, Write},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct OrderedUnnest {
    pub unnest: Unnest,
    pub key: Column,
}

impl UserDefinedLogicalNodeCore for OrderedUnnest {
    fn name(&self) -> &str {
        "DxtranOrderedUnnest"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.unnest.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.unnest.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        self.unnest
            .exec_columns
            .iter()
            .chain([&self.key])
            .cloned()
            .map(Expr::Column)
            .collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DxtranOrderedUnnest: {:?}", self.unnest.exec_columns)
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 || exprs.len() != self.unnest.exec_columns.len() + 1 {
            return Err(DataFusionError::Internal(
                "invalid ordered UNNEST inputs".into(),
            ));
        }
        let mut columns = exprs
            .into_iter()
            .map(|expr| match expr {
                Expr::Column(column) => Ok(column),
                _ => Err(DataFusionError::Internal("UNNEST requires columns".into())),
            })
            .collect::<Result<Vec<_>>>()?;
        let key = columns.pop().unwrap();
        let unnest = Unnest::try_new(
            Arc::new(inputs.remove(0)),
            columns,
            self.unnest.options.clone(),
        )?;
        Ok(Self { unnest, key })
    }
}

#[derive(Debug)]
pub struct OrderedQueryPlanner;

#[async_trait]
impl QueryPlanner for OrderedQueryPlanner {
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(OrderedUnnestPlanner)])
            .create_physical_plan(plan, session)
            .await
    }
}
struct OrderedUnnestPlanner;
#[async_trait]
impl ExtensionPlanner for OrderedUnnestPlanner {
    async fn plan_extension(
        &self,
        _: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _: &[&LogicalPlan],
        inputs: &[Arc<dyn ExecutionPlan>],
        _: &dyn Session,
        _: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<OrderedUnnest>() else {
            return Ok(None);
        };
        let schema = Arc::new(node.unnest.schema.as_arrow().clone());
        let key_index = node.unnest.schema.index_of_column(&node.key)?;
        Ok(Some(Arc::new(OrderedUnnestExec::new(
            inputs[0].clone(),
            node.unnest
                .list_type_columns
                .iter()
                .map(|(index, col)| ListUnnest {
                    index_in_input_schema: *index,
                    depth: col.depth,
                })
                .collect(),
            node.unnest.struct_type_columns.clone(),
            schema,
            node.unnest.options.clone(),
            key_index,
        )?)))
    }
}

#[derive(Debug)]
struct OrderedUnnestExec {
    input: Arc<dyn ExecutionPlan>,
    inner: UnnestExec,
    lists: Vec<ListUnnest>,
    structs: Vec<usize>,
    options: UnnestOptions,
    key_index: usize,
    properties: Arc<PlanProperties>,
}
impl OrderedUnnestExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        lists: Vec<ListUnnest>,
        structs: Vec<usize>,
        schema: SchemaRef,
        options: UnnestOptions,
        key_index: usize,
    ) -> Result<Self> {
        let inner = UnnestExec::new(
            input.clone(),
            lists.clone(),
            structs.clone(),
            schema.clone(),
            options.clone(),
        )?;
        // The key changes here. In particular, do not advertise the input's key
        // ordering or equivalences to the optimizer.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            inner.properties().emission_type,
            inner.properties().boundedness,
        ));
        Ok(Self {
            input,
            inner,
            lists,
            structs,
            options,
            key_index,
            properties,
        })
    }
}
impl DisplayAs for OrderedUnnestExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("DxtranOrderedUnnestExec")
    }
}
impl ExecutionPlan for OrderedUnnestExec {
    fn name(&self) -> &str {
        "DxtranOrderedUnnestExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    // Expose the pre-expansion input, so no repartition/sort can be inserted
    // between expansion and ordinal assignment.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
            return Err(DataFusionError::Internal(
                "ordered UNNEST requires one child".into(),
            ));
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.lists.clone(),
            self.structs.clone(),
            self.schema(),
            self.options.clone(),
            self.key_index,
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let key_index = self.key_index;
        let mut previous = None::<String>;
        let mut prefix = String::new();
        let mut ordinal = 0u64;
        let stream = self.inner.execute(partition, context)?.map(move |batch| {
            let batch = batch?;
            let keys = batch.column(key_index);
            let mut builder = StringBuilder::new();
            let mut key = String::new();
            for row in 0..batch.num_rows() {
                let parent = if keys.is_null(row) {
                    "0"
                } else {
                    match keys.data_type() {
                        DataType::Utf8 => keys.as_string::<i32>().value(row),
                        DataType::LargeUtf8 => keys.as_string::<i64>().value(row),
                        DataType::Utf8View => keys.as_string_view().value(row),
                        _ => {
                            return Err(DataFusionError::Internal(
                                "invalid UNNEST order key".into(),
                            ));
                        }
                    }
                };
                if previous.as_deref() != Some(parent) {
                    let previous = previous.get_or_insert_with(String::new);
                    previous.clear();
                    previous.push_str(parent);
                    prefix.clear();
                    // Hex and a delimiter below '0' preserve parent order,
                    // including prefix keys. Encode once per parent row.
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    for byte in parent.bytes() {
                        prefix.push(HEX[usize::from(byte >> 4)] as char);
                        prefix.push(HEX[usize::from(byte & 15)] as char);
                    }
                    prefix.push('/');
                    ordinal = 0;
                }
                ordinal = ordinal.checked_add(1).ok_or_else(|| {
                    DataFusionError::Execution("UNNEST element ordinal overflow".into())
                })?;
                key.clear();
                key.push_str(&prefix);
                write!(&mut key, "{ordinal:016x}").unwrap();
                builder.append_value(&key);
            }
            let mut columns = batch.columns().to_vec();
            columns[key_index] = cast(&builder.finish(), keys.data_type())?;
            Ok(RecordBatch::try_new(batch.schema(), columns)?)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
