//! Resolve business parameters once and bind typed literals through the SQL AST.
use crate::{
    config::{Check, Job},
    error::{Error, Result},
};
use datafusion::{
    common::{ScalarValue, tree_node::TreeNodeRecursion},
    logical_expr::{LogicalPlan, lit},
    prelude::SessionContext,
    sql::{
        sqlparser::{
            ast::{Expr, Query, Value, Visit, VisitMut, Visitor, VisitorMut},
            dialect::GenericDialect,
            parser::Parser,
            tokenizer::Token,
        },
        unparser::expr_to_sql,
    },
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, ops::ControlFlow};

pub(crate) fn identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn expression(sql: &str) -> Result<Expr> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(sql)
        .map_err(|e| Error::config(e.to_string()))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| Error::config(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(Error::config("expected exactly one SQL expression"));
    }
    Ok(expr)
}

struct DefaultExpression;
impl Visitor for DefaultExpression {
    type Break = &'static str;
    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<Self::Break> {
        ControlFlow::Break("parameter defaults cannot contain subqueries")
    }
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if matches!(expr, Expr::Value(v) if matches!(v.value, Value::Placeholder(_))) {
            return ControlFlow::Break("parameter defaults cannot reference parameters");
        }
        ControlFlow::Continue(())
    }
}

struct Bind<'a>(&'a BTreeMap<String, Expr>);
impl VisitorMut for Bind<'_> {
    type Break = Error;
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Error> {
        if let Expr::Value(v) = expr
            && let Value::Placeholder(name) = &v.value
        {
            let Some(value) = name.strip_prefix('$').and_then(|name| self.0.get(name)) else {
                return ControlFlow::Break(Error::config(format!("undeclared parameter '{name}'")));
            };
            *expr = Expr::Nested(Box::new(value.clone()));
        }
        ControlFlow::Continue(())
    }
}
impl Bind<'_> {
    fn expression(&mut self, sql: &mut String) -> Result<()> {
        if !sql.contains('$') {
            return Ok(());
        }
        let mut expr = expression(sql)?;
        if let ControlFlow::Break(e) = VisitMut::visit(&mut expr, self) {
            return Err(e);
        }
        *sql = expr.to_string();
        Ok(())
    }
    fn query(&mut self, sql: &mut String) -> Result<()> {
        if !sql.contains('$') {
            return Ok(());
        }
        let mut statements =
            Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| Error::sql(e.to_string()))?;
        if let ControlFlow::Break(e) = VisitMut::visit(&mut statements, self) {
            return Err(e);
        }
        *sql = statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        Ok(())
    }
    fn checks(&mut self, checks: &mut [Check]) -> Result<()> {
        for check in checks {
            self.expression(&mut check.when).map_err(|e| {
                e.context(
                    check
                        .when_origin
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                )
            })?;
        }
        Ok(())
    }
}

pub(crate) async fn bind(job: &mut Job, overrides: &[String]) -> Result<()> {
    if job.params_bound {
        return Err(Error::config("business parameters are already bound"));
    }
    let mut supplied = BTreeMap::new();
    for arg in overrides {
        let (name, value) = arg
            .split_once('=')
            .ok_or_else(|| Error::config("--param requires NAME=VALUE"))?;
        if !identifier(name) || !job.params.contains_key(name) {
            return Err(Error::config(format!("undeclared parameter '{name}'")));
        }
        if supplied.insert(name, value).is_some() {
            return Err(Error::config(format!("duplicate parameter '{name}'")));
        }
    }
    for (name, param) in &job.params {
        if param.default.is_none() && !supplied.contains_key(name.as_str()) {
            return Err(
                Error::config(format!("required parameter '{name}' was not supplied")).context(
                    param
                        .origin
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                ),
            );
        }
    }
    let ctx = (!job.params.is_empty()).then(SessionContext::new);
    if let Some(ctx) = &ctx {
        crate::functions::register(ctx);
    }
    let mut values = BTreeMap::new();
    for (name, param) in &job.params {
        let resolved = async {
            // Validate defaults even when overridden, but evaluate only the chosen value.
            let default = if let Some(sql) = &param.default {
                let expr = expression(sql)?;
                if let ControlFlow::Break(message) = expr.visit(&mut DefaultExpression) {
                    return Err(Error::config(message));
                }
                let frame = ctx
                    .as_ref()
                    .unwrap()
                    .sql(&format!("SELECT ({expr}) AS value"))
                    .await
                    .map_err(|e| Error::config(e.to_string()))?;
                frame
                    .logical_plan()
                    .apply_with_subqueries(|node| {
                        if matches!(
                            node,
                            LogicalPlan::Aggregate(_)
                                | LogicalPlan::Window(_)
                                | LogicalPlan::Subquery(_)
                        ) {
                            return Err(datafusion::error::DataFusionError::Plan(
                                "parameter defaults require scalar SQL expressions".into(),
                            ));
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .map_err(|e| Error::config(e.to_string()))?;
                Some(frame)
            } else {
                None
            };
            let value = if let Some(value) = supplied.get(name.as_str()) {
                ScalarValue::Utf8(Some((*value).to_owned()))
            } else {
                let batches = default
                    .unwrap()
                    .collect()
                    .await
                    .map_err(|_| Error::config("parameter default SQL evaluation failed"))?;
                if batches.len() != 1 || batches[0].num_rows() != 1 {
                    return Err(Error::config(
                        "parameter default must return one scalar value",
                    ));
                }
                ScalarValue::try_from_array(batches[0].column(0), 0)
                    .map_err(|e| Error::config(e.to_string()))?
            };
            // Preserve numeric widths, decimal scale and typed NULLs across SQL parsing.
            // Numeric text also represents NaN/Infinity without becoming identifiers.
            let data_type = value.data_type();
            let literal = if data_type.is_numeric() && !value.is_null() {
                lit(value.to_string())
            } else {
                lit(value)
            };
            expr_to_sql(&datafusion::functions::core::expr_fn::arrow_cast(
                literal,
                lit(data_type.to_string()),
            ))
            .map_err(|e| Error::config(e.to_string()))
        }
        .await
        .map_err(|e| {
            e.context(format!(
                "parameter {name} {}",
                param
                    .origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })?;
        values.insert(name.clone(), resolved);
    }
    let mut binder = Bind(&values);
    for dataset in job.input.iter_mut().chain(&mut job.output) {
        if let Some(sql) = &mut dataset.sql {
            binder.query(sql)?;
        }
        for checks in [&mut dataset.check, &mut dataset.before, &mut dataset.after] {
            binder.checks(checks)?;
        }
        for field in &mut dataset.fields {
            if let Some(when) = &mut field.when {
                binder.expression(when)?;
            }
        }
    }
    for process in &mut job.process {
        binder.query(&mut process.sql)?;
        binder.checks(&mut process.before)?;
        binder.checks(&mut process.after)?;
    }
    if !values.is_empty() {
        let mut hash = Sha256::new();
        hash.update(job.expanded_hash.as_bytes());
        for (name, value) in &values {
            hash.update(serde_json::to_vec(&(name, value.to_string())).unwrap());
        }
        job.expanded_hash = hex::encode(hash.finalize());
    }
    job.params_bound = true;
    Ok(())
}
