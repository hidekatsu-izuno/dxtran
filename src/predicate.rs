//! Shared SQL Boolean expression compilation for layout conditions and checks.
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        DFSchema,
        tree_node::{Transformed, TreeNode},
    },
    error::{DataFusionError, Result},
    logical_expr::{
        Expr,
        simplify::{ExprSimplifyResult, SimplifyContext},
    },
    physical_expr::PhysicalExpr,
    prelude::SessionContext,
};
use std::sync::Arc;

pub(crate) fn is_true(value: &datafusion::logical_expr::ColumnarValue, row: usize) -> bool {
    use datafusion::{
        arrow::array::{Array, AsArray},
        common::ScalarValue,
        logical_expr::ColumnarValue,
    };
    match value {
        ColumnarValue::Scalar(ScalarValue::Boolean(value)) => *value == Some(true),
        ColumnarValue::Array(array) => {
            let values = array.as_boolean();
            !values.is_null(row) && values.value(row)
        }
        _ => unreachable!("compiled Boolean predicate"),
    }
}

pub(crate) fn compile(
    context: &SessionContext,
    sql: &str,
    schema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let logical = context.parse_sql_expr(sql, schema)?;
    if logical.is_volatile() {
        return Err(DataFusionError::Plan(
            "condition requires a deterministic SQL expression".into(),
        ));
    }
    // arrow_cast encodes exact parameter types, but must become a native Cast
    // before physical evaluation. Do not fold other expressions here: their
    // failures must retain the record check's execution timing and error code.
    let info = SimplifyContext::builder()
        .with_schema(Arc::new(schema.clone()))
        .build();
    let logical = logical
        .transform_up(|expr| {
            if let Expr::ScalarFunction(mut function) = expr {
                if function.func.name() == "arrow_cast" {
                    match function.func.simplify(function.args, &info)? {
                        ExprSimplifyResult::Simplified(expr) => return Ok(Transformed::yes(expr)),
                        ExprSimplifyResult::Original(args) => function.args = args,
                    }
                }
                Ok(Transformed::no(Expr::ScalarFunction(function)))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data;
    let expression = context.create_physical_expr(logical, schema)?;
    if expression.data_type(schema.as_arrow())? != DataType::Boolean {
        return Err(DataFusionError::Plan(
            "SQL expression must return Boolean".into(),
        ));
    }
    Ok(expression)
}
