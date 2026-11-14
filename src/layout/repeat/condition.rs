use super::*;
use datafusion::{
    arrow::record_batch::RecordBatchOptions,
    common::DFSchema,
    physical_expr::{
        PhysicalExpr,
        utils::{collect_columns, reassign_expr_columns},
    },
    prelude::SessionContext,
};

#[derive(Clone, Debug)]
pub(super) struct Condition {
    pub bindings: Vec<(usize, String)>,
    schema: SchemaRef,
    expression: Arc<dyn PhysicalExpr>,
    sql: String,
}

impl Condition {
    fn compile(sql: &str, scopes: &[Fields], context: &SessionContext) -> Result<Self> {
        let mut visible = BTreeMap::new();
        for (depth, scope) in scopes.iter().enumerate() {
            for field in scope {
                visible.insert(field.name().clone(), (depth, field.clone()));
            }
        }
        let fields: Vec<_> = visible.values().map(|(_, field)| field.clone()).collect();
        let schema = Schema::new(fields);
        let df_schema =
            DFSchema::try_from(schema.clone()).map_err(|e| Error::config(e.to_string()))?;
        let expression = crate::predicate::compile(context, sql, &df_schema)
            .map_err(|e| Error::config(e.to_string()).context(format!("when: {sql}")))?;
        let projection: Vec<_> = collect_columns(&expression)
            .into_iter()
            .map(|c| c.index())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let schema = Arc::new(
            schema
                .project(&projection)
                .map_err(|e| Error::config(e.to_string()))?,
        );
        let bindings = schema
            .fields()
            .iter()
            .map(|f| (visible[f.name()].0, f.name().clone()))
            .collect();
        let expression =
            reassign_expr_columns(expression, &schema).map_err(|e| Error::config(e.to_string()))?;
        Ok(Self {
            bindings,
            schema,
            expression,
            sql: sql.to_owned(),
        })
    }

    pub(super) fn evaluate(&self, values: &[ScalarValue], output: bool) -> Result<bool> {
        let bad = |e: String| {
            Error::new(if output { 5 } else { 3 }, e).context(format!("when: {}", self.sql))
        };
        let arrays = values
            .iter()
            .map(|v| v.to_array_of_size(1).map_err(|e| bad(e.to_string())))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| bad(e.to_string()))?;
        let result = self
            .expression
            .evaluate(&batch)
            .map_err(|e| bad(e.to_string()))?;
        Ok(crate::predicate::is_true(&result, 0))
    }

    pub(super) fn output_matches(&self, scopes: &[Row]) -> Result<bool> {
        let values = self
            .bindings
            .iter()
            .map(|(depth, name)| {
                scopes[*depth]
                    .get(name)
                    .cloned()
                    .ok_or_else(|| Error::output(format!("missing when field '{name}'")))
            })
            .collect::<Result<Vec<_>>>()?;
        self.evaluate(&values, true)
    }
}

impl Group {
    pub(super) fn bind_conditions(
        &mut self,
        actual: Option<&Fields>,
        scopes: &mut Vec<Fields>,
    ) -> Result<()> {
        let fields = actual.cloned().unwrap_or_else(|| {
            self.nodes
                .iter()
                .filter(|n| n.when_sql.is_none())
                .flat_map(|n| n.columns.iter().cloned())
                .collect::<Vec<_>>()
                .into()
        });
        scopes.push(fields);
        let context = SessionContext::new();
        crate::functions::register(&context);
        for node in &mut self.nodes {
            if let Some(sql) = &node.when_sql {
                node.when = Some(
                    Condition::compile(sql, scopes, &context)
                        .map_err(|e| e.context(&node.origin))?,
                );
            }
            if let Element::Group(child) = &mut node.element {
                let nested = actual.map(|fields| {
                    let field = fields
                        .iter()
                        .find(|f| Some(f.name()) == node.name.as_ref())
                        .expect("validated output group");
                    let t = if node.repeat.is_some() {
                        list_item(field.data_type())
                            .expect("validated output list")
                            .data_type()
                    } else {
                        field.data_type()
                    };
                    let DataType::Struct(fields) = t else {
                        unreachable!("validated output struct");
                    };
                    fields
                });
                child.bind_conditions(nested, scopes)?;
            }
        }
        scopes.pop();
        Ok(())
    }
}
