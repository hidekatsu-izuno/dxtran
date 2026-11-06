//! Whole-relation checks at input/process/output execution boundaries.
use crate::{
    config::{Check, CheckLevel},
    error::{Error, Result},
    snapshot::SnapshotTable,
};
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::ScalarValue,
    datasource::MemTable,
    logical_expr::LogicalPlan,
    prelude::SessionContext,
    sql::sqlparser::{
        ast::{Expr, Query, Visit, Visitor},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Token,
    },
};
use futures::StreamExt;
use serde_json::json;
use std::{
    io::{self, Write},
    ops::ControlFlow,
    sync::Arc,
};

const TARGET: &str = "dxt$aggregate_target";

#[derive(Clone)]
struct Rule {
    config: Check,
    query: String,
    number: usize,
}

#[derive(Clone, Default)]
pub(crate) struct AggregateChecks {
    rules: Vec<Rule>,
    side: &'static str,
    timing: &'static str,
    id: String,
    target: String,
    code: u8,
}

struct SafeExpression;
impl Visitor for SafeExpression {
    type Break = &'static str;
    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Break("subqueries are not allowed in aggregate checks")
    }
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(f) = expr
            && f.over.is_some()
        {
            return ControlFlow::Break("window functions are not allowed in aggregate checks");
        }
        ControlFlow::Continue(())
    }
}

impl AggregateChecks {
    pub(crate) async fn compile(
        checks: &[Check],
        side: &'static str,
        id: &str,
        timing: &'static str,
        target: &str,
        schema: SchemaRef,
        offset: usize,
    ) -> Result<Self> {
        let code = match side {
            "input" => 3,
            "process" => 4,
            _ => 5,
        };
        let mut result = Self {
            side,
            id: id.into(),
            timing,
            target: target.into(),
            code,
            rules: Vec::new(),
        };
        if checks.is_empty() {
            return Ok(result);
        }
        let ctx = SessionContext::new();
        crate::functions::register(&ctx);
        ctx.register_table(TARGET, Arc::new(MemTable::try_new(schema, vec![vec![]])?))?;
        for (i, check) in checks.iter().enumerate() {
            let location = result.location(i + offset + 1, check);
            if check.from.is_some() && !(side == "process" && timing == "before") {
                return Err(
                    Error::config("from is only supported in process.before").context(location)
                );
            }
            let compile = async {
                let dialect = GenericDialect {};
                let mut parser = Parser::new(&dialect).try_with_sql(&check.when).map_err(|e| Error::config(e.to_string()))?;
                let expression = parser.parse_expr().map_err(|e| Error::config(e.to_string()))?;
                if parser.peek_token().token != Token::EOF {
                    return Err(Error::config("when must contain exactly one SQL expression"));
                }
                if let ControlFlow::Break(message) = expression.visit(&mut SafeExpression) {
                    return Err(Error::config(message));
                }
                // COUNT forces one result row even for empty data and constant conditions.
                let query = format!("SELECT ({expression}) AS \"dxt$condition\", COUNT(*) AS \"dxt$rows\" FROM \"{TARGET}\" AS \"{target}\"");
                let frame = ctx.sql(&query).await.map_err(|e| Error::config(e.to_string()))?;
                if frame.schema().field(0).data_type() != &DataType::Boolean {
                    return Err(Error::config("aggregate check.when must return Boolean"));
                }
                use datafusion::common::tree_node::TreeNodeRecursion;
                frame.logical_plan().apply_with_subqueries(|node| {
                    if matches!(node, LogicalPlan::Window(_) | LogicalPlan::Subquery(_))
                        || node.expressions().iter().any(|expr| expr.is_volatile()) {
                        return Err(datafusion::error::DataFusionError::Plan("aggregate checks require deterministic expressions without windows or subqueries".into()));
                    }
                    Ok(TreeNodeRecursion::Continue)
                }).map_err(|e| Error::config(e.to_string()))?;
                Ok(query)
            }.await.map_err(|e: Error| e.context(&location))?;
            result.rules.push(Rule {
                config: check.clone(),
                query: compile,
                number: i + offset + 1,
            });
        }
        Ok(result)
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
    fn location(&self, number: usize, check: &Check) -> String {
        let origin = check
            .when_origin
            .as_ref()
            .or(check.origin.as_ref())
            .map(ToString::to_string)
            .unwrap_or_default();
        format!(
            "{} {} {} check {number} {origin}",
            self.side, self.id, self.timing
        )
    }
    pub(crate) async fn run(&self, ctx: &SessionContext, table: &SnapshotTable) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        ctx.register_table(TARGET, Arc::new(table.clone()))?;
        let result = self.evaluate(ctx).await;
        let removed = ctx.deregister_table(TARGET).map_err(Error::from);
        result.and(removed.map(|_| ()))
    }
    async fn evaluate(&self, ctx: &SessionContext) -> Result<()> {
        for rule in &self.rules {
            let location = self.location(rule.number, &rule.config);
            let evaluated = async {
                let physical = ctx.sql(&rule.query).await?.create_physical_plan().await?;
                let physical = crate::runtime::cooperative::wrap(physical)?;
                let mut stream =
                    datafusion::physical_plan::execute_stream(physical, ctx.task_ctx())?;
                let batch = stream.next().await.transpose()?.ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("missing aggregate result".into())
                })?;
                if batch.num_rows() != 1 || stream.next().await.transpose()?.is_some() {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "aggregate check requires one result row".into(),
                    ));
                }
                let matched = matches!(
                    ScalarValue::try_from_array(batch.column(0), 0)?,
                    ScalarValue::Boolean(Some(true))
                );
                let count = ScalarValue::try_from_array(batch.column(1), 0)?;
                Ok::<_, datafusion::error::DataFusionError>((matched, count))
            }
            .await
            .map_err(|_| {
                Error::new(
                    self.code,
                    "aggregate check.when SQL evaluation failed (input value details omitted)",
                )
                .context(&location)
            })?;
            if !evaluated.0 {
                continue;
            }
            if matches!(rule.config.level, CheckLevel::Error) {
                return Err(
                    Error::new(self.code, format!("check error: {}", rule.config.message))
                        .context(location),
                );
            }
            let level = if matches!(rule.config.level, CheckLevel::Info) {
                "info"
            } else {
                "warn"
            };
            let message = json!({"event":"check","aggregate":true,"level":level,"side":self.side,
                "definition":self.id,"timing":self.timing,"from":self.target,"check":rule.number,
                "rows":if let ScalarValue::Int64(Some(n)) = evaluated.1 { n } else { 0 },"message":rule.config.message});
            let written = if matches!(rule.config.level, CheckLevel::Info) {
                writeln!(io::stdout().lock(), "{message}")
            } else {
                writeln!(io::stderr().lock(), "{message}")
            };
            written.map_err(|e| {
                Error::new(self.code, format!("check log write failed: {e}")).context(location)
            })?;
        }
        Ok(())
    }
}

pub(crate) struct HookStep {
    pub id: String,
    pub input: bool,
    pub active: bool,
    pub plan: LogicalPlan,
    pub table: SnapshotTable,
    pub before: Vec<(SnapshotTable, AggregateChecks)>,
    pub after: AggregateChecks,
}
impl HookStep {
    pub(crate) async fn run(&self, ctx: &SessionContext, path: &std::path::Path) -> Result<u64> {
        for (table, check) in &self.before {
            check.run(ctx, table).await?;
        }
        let rows = self.table.materialize(ctx, self.plan.clone(), path).await?;
        self.after.run(ctx, &self.table).await?;
        Ok(rows)
    }
}
