//! Resolve business parameters once and bind typed literals through the SQL AST.
mod paths;
mod sql;
use crate::{
    config::{Check, Job, Param, ParamType},
    error::{Error, Result},
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    common::{DFSchema, ScalarValue, tree_node::TreeNodeRecursion},
    logical_expr::{Expr as LogicalExpr, LogicalPlan, lit},
    prelude::{SessionConfig, SessionContext},
    sql::{
        sqlparser::ast::{Expr, Query, UnaryOperator, Value, Visit, VisitMut, Visitor, VisitorMut},
        unparser::expr_to_sql,
    },
};
use sha2::{Digest, Sha256};
use sql::expression;
use std::{collections::BTreeMap, ops::ControlFlow};

impl Param {
    fn convert(&self, value: ScalarValue, context: &SessionContext) -> Result<ScalarValue> {
        let Some(kind) = self.kind else {
            return Ok(value);
        };
        if kind == ParamType::Decimal {
            if value.is_null() {
                return Ok(ScalarValue::Decimal128(None, 1, 0));
            }
            if let ScalarValue::Decimal128(Some(coefficient), _, scale) = &value {
                return decimal_value(*coefficient, *scale);
            }
            let text = if let Some(Some(text)) = value.try_as_str() {
                text.to_owned()
            } else if value.data_type().is_numeric() {
                value.to_string()
            } else {
                return Err(Error::config(
                    "parameter value cannot be converted to decimal",
                ));
            };
            return decimal(&text, context);
        }
        let data_type = match kind {
            ParamType::String => DataType::Utf8,
            ParamType::Boolean => DataType::Boolean,
            ParamType::Date => DataType::Date32,
            ParamType::Time => DataType::Time64(TimeUnit::Microsecond),
            ParamType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ParamType::Decimal => unreachable!(),
        };
        let invalid = || {
            Error::config(format!(
                "parameter value cannot be converted to {data_type}"
            ))
        };
        if value.is_null() {
            return ScalarValue::try_new_null(&data_type).map_err(|_| invalid());
        }
        if kind == ParamType::Boolean
            && let Some(Some(text)) = value.try_as_str()
        {
            return match text {
                text if text.eq_ignore_ascii_case("true") => Ok(ScalarValue::Boolean(Some(true))),
                text if text.eq_ignore_ascii_case("false") => Ok(ScalarValue::Boolean(Some(false))),
                _ => Err(invalid()),
            };
        }
        value.cast_to(&data_type).map_err(|_| invalid())
    }
}

fn decimal(text: &str, context: &SessionContext) -> Result<ScalarValue> {
    let invalid = || Error::config("invalid decimal parameter or precision exceeds 38 digits");
    // Accept only one numeric constant; CLI text never becomes an SQL expression.
    let number = expression(text).map_err(|_| invalid())?;
    let numeric =
        |expr: &Expr| matches!(expr, Expr::Value(v) if matches!(v.value, Value::Number(_, _)));
    if !numeric(&number)
        && !matches!(&number,
        Expr::UnaryOp { op: UnaryOperator::Plus | UnaryOperator::Minus, expr } if numeric(expr))
    {
        return Err(invalid());
    }
    let logical = context
        .parse_sql_expr(&number.to_string(), &DFSchema::empty())
        .map_err(|_| invalid())?;
    let (coefficient, scale) = match logical {
        LogicalExpr::Literal(ScalarValue::Decimal128(Some(value), _, scale), _) => (value, scale),
        LogicalExpr::Literal(ScalarValue::Int64(Some(value)), _) => (i128::from(value), 0),
        LogicalExpr::Literal(ScalarValue::UInt64(Some(value)), _) => (i128::from(value), 0),
        _ => return Err(invalid()),
    };
    decimal_value(coefficient, scale)
}

fn decimal_value(coefficient: i128, scale: i8) -> Result<ScalarValue> {
    let digits = (coefficient.unsigned_abs().max(1).ilog10() + 1) as u8;
    if digits > 38 || !(-38..=38).contains(&scale) {
        return Err(Error::config(
            "decimal parameter exceeds the supported precision or scale",
        ));
    }
    Ok(ScalarValue::Decimal128(
        Some(coefficient),
        digits.max(scale.max(0) as u8),
        scale,
    ))
}

pub(crate) fn identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
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

struct Resolved {
    scalar: ScalarValue,
    literal: Expr,
}

struct Bind<'a>(&'a BTreeMap<String, Resolved>);
impl VisitorMut for Bind<'_> {
    type Break = Error;
    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Error> {
        if let Expr::Value(v) = expr
            && let Value::Placeholder(name) = &v.value
        {
            let Some(value) = name
                .strip_prefix("${")
                .and_then(|name| name.strip_suffix('}'))
                .and_then(|name| self.0.get(name))
            else {
                return ControlFlow::Break(Error::config(format!("undeclared parameter '{name}'")));
            };
            *expr = Expr::Nested(Box::new(value.literal.clone()));
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
        let mut statements = sql::query(sql)?;
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
    let ctx = job
        .params
        .values()
        .any(|p| p.kind != Some(ParamType::Decimal))
        .then(SessionContext::new);
    let decimal_ctx = job
        .params
        .values()
        .any(|p| p.kind == Some(ParamType::Decimal))
        .then(|| {
            let mut config = SessionConfig::new();
            config.options_mut().sql_parser.parse_float_as_decimal = true;
            SessionContext::new_with_config(config)
        });
    for context in ctx.iter().chain(&decimal_ctx) {
        crate::functions::register(context);
    }
    let mut values = BTreeMap::new();
    for (name, param) in &job.params {
        let context = if param.kind == Some(ParamType::Decimal) {
            decimal_ctx.as_ref().unwrap()
        } else {
            ctx.as_ref().unwrap()
        };
        let resolved = async {
            // Validate defaults even when overridden, but evaluate only the chosen value.
            let default = if let Some(sql) = &param.default {
                let expr = expression(sql)?;
                if let ControlFlow::Break(message) = expr.visit(&mut DefaultExpression) {
                    return Err(Error::config(message));
                }
                let frame = context
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
            let value = param.convert(value, context)?;
            let data_type = value.data_type();
            let literal = if data_type.is_numeric() && !value.is_null() {
                let text = lit(value.to_string());
                if matches!(data_type, DataType::Decimal128(_, scale) if scale < 0) {
                    // Arrow cannot cast strings directly to negative-scale decimals.
                    // The intermediate type covers 38 coefficient + 38 implicit digits.
                    datafusion::functions::core::expr_fn::arrow_cast(text, lit("Decimal256(76, 0)"))
                } else {
                    text
                }
            } else {
                lit(value.clone())
            };
            expr_to_sql(&datafusion::functions::core::expr_fn::arrow_cast(
                literal,
                lit(data_type.to_string()),
            ))
            .map(|literal| Resolved {
                scalar: value,
                literal,
            })
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
    let expanded_paths = paths::expand(job, &values)?;
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
    if !values.is_empty() || !expanded_paths.is_empty() {
        let mut hash = Sha256::new();
        hash.update(job.expanded_hash.as_bytes());
        for (name, value) in &values {
            hash.update(serde_json::to_vec(&(name, value.literal.to_string())).unwrap());
        }
        if !expanded_paths.is_empty() {
            hash.update(serde_json::to_vec(&expanded_paths).unwrap());
        }
        job.expanded_hash = hex::encode(hash.finalize());
    }
    job.params_bound = true;
    Ok(())
}
