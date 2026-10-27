//! Reserve one namespace for internal columns and relation aliases.
use crate::error::{Error, Result};
use datafusion::{
    common::tree_node::TreeNodeRecursion,
    error::DataFusionError,
    logical_expr::LogicalPlan,
    sql::sqlparser::{
        ast::{Query, Select, SelectItem, Statement, TableAlias, TableFactor, Visit, Visitor},
        tokenizer::{Token, TokenWithSpan},
    },
};
use std::ops::ControlFlow;

pub(super) const INTERNAL_PREFIX: &str = "dxt$";

pub(super) fn is_reserved(name: &str) -> bool {
    name.get(..INTERNAL_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(INTERNAL_PREFIX))
}

fn reserved_name(name: &str) -> String {
    format!("SQL identifier '{name}' uses the reserved internal prefix '{INTERNAL_PREFIX}'")
}

pub(super) fn validate_sql(statements: &[Statement], tokens: &[TokenWithSpan]) -> Result<()> {
    // Tokenization distinguishes identifiers from string values and comments,
    // and covers identifier positions beyond SELECT and FROM aliases.
    for token in tokens {
        if let Token::Word(word) = &token.token
            && is_reserved(&word.value)
        {
            return Err(Error::sql(reserved_name(&word.value)).context(token.span.start));
        }
    }
    // sqlparser also accepts single-quoted strings as aliases. Those must be
    // checked as AST aliases without rejecting ordinary string values.
    for statement in statements {
        if let ControlFlow::Break(message) = statement.visit(&mut AliasNames) {
            return Err(Error::sql(message));
        }
    }
    Ok(())
}

struct AliasNames;
fn check_alias(name: &str) -> ControlFlow<String> {
    if is_reserved(name) {
        ControlFlow::Break(reserved_name(name))
    } else {
        ControlFlow::Continue(())
    }
}
fn check_table_alias(alias: &TableAlias) -> ControlFlow<String> {
    check_alias(&alias.name.value)?;
    for column in &alias.columns {
        check_alias(&column.name.value)?;
    }
    if let Some(at) = &alias.at {
        check_alias(&at.value)?;
    }
    ControlFlow::Continue(())
}
impl Visitor for AliasNames {
    type Break = String;
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<String> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                check_table_alias(&cte.alias)?;
            }
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<String> {
        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => check_alias(&alias.value)?,
                SelectItem::ExprWithAliases { aliases, .. } => {
                    for alias in aliases {
                        check_alias(&alias.value)?;
                    }
                }
                _ => (),
            }
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<String> {
        let alias = match table {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::JsonTable { alias, .. }
            | TableFactor::OpenJsonTable { alias, .. }
            | TableFactor::NestedJoin { alias, .. }
            | TableFactor::Pivot { alias, .. }
            | TableFactor::Unpivot { alias, .. }
            | TableFactor::MatchRecognize { alias, .. }
            | TableFactor::XmlTable { alias, .. }
            | TableFactor::SemanticView { alias, .. } => alias,
        };
        if let Some(alias) = alias {
            check_table_alias(alias)?;
        }
        ControlFlow::Continue(())
    }
}

/// Check names produced by schema expansion as well, before adding internal columns.
pub(super) fn validate_plan(plan: &LogicalPlan) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        for field in node.schema().fields() {
            if is_reserved(field.name()) {
                return Err(DataFusionError::Plan(reserved_name(field.name())));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn plan_validation_covers_nested_and_expression_subquery_schemas() {
        let ctx = SessionContext::new();
        for sql in [
            "SELECT 1 AS dxt$hidden",
            "SELECT visible FROM (SELECT 1 AS dxt$hidden, 2 AS visible) AS t",
            "SELECT (SELECT 1 AS dxt$hidden) AS visible",
        ] {
            // Exercise the plan boundary independently of the SQL name validator.
            let plan = ctx.sql(sql).await.unwrap().into_unoptimized_plan();
            let error = validate_plan(&plan).unwrap_err();
            assert_eq!(error.code, 4);
            assert!(error.message.contains("reserved internal prefix 'dxt$'"));
        }
    }
}
