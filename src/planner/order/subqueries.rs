use super::{OrderPlanner, key_column};
use crate::error::Result;
use datafusion::{
    common::{
        Column, JoinType,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{
        Expr, LogicalPlan, LogicalPlanBuilder,
        expr::{Exists, InSubquery, Sort as SortExpr},
        lit,
        utils::conjunction,
    },
    optimizer::decorrelate::PullUpCorrelatedExpr,
};
use std::{collections::BTreeSet, sync::Arc};

// DF55 infers scalar nullability from the selected column, even though zero
// rows must produce NULL. An outer join against a single empty tuple makes
// that nullability explicit without hiding multi-row cardinality violations.
pub(super) fn nullable_scalar(expr: Expr) -> datafusion::error::Result<Transformed<Expr>> {
    let Expr::ScalarSubquery(mut subquery) = expr else {
        return Ok(Transformed::no(expr));
    };
    if !subquery.outer_ref_columns.is_empty() || subquery.subquery.schema().field(0).is_nullable() {
        return Ok(Transformed::no(Expr::ScalarSubquery(subquery)));
    }
    let columns = subquery
        .subquery
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column);
    let plan = LogicalPlanBuilder::empty(true)
        .join_on((*subquery.subquery).clone(), JoinType::Left, [lit(true)])?
        .project(columns)?
        .build()?;
    subquery.subquery = Arc::new(plan);
    Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
}

impl OrderPlanner {
    // DataFusion lowers predicate subqueries in filters, but not projections.
    // A mark join preserves every outer row, including duplicates, while
    // exposing the predicate result as a private column.
    pub(super) fn predicate_subqueries(
        &mut self,
        mut input: LogicalPlan,
        exprs: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        let exprs = exprs
            .into_iter()
            .map(|expr| {
                let original = expr.clone();
                let rewritten = expr.transform_up(|expr| {
                    let (subquery, predicate, negated, exists) = match expr {
                        Expr::Exists(Exists { subquery, negated }) => {
                            (subquery, None, negated, true)
                        }
                        Expr::InSubquery(InSubquery {
                            expr,
                            subquery,
                            negated,
                        }) => {
                            let column =
                                Expr::Column(subquery.subquery.schema().qualified_field(0).into());
                            (subquery, Some((*expr).eq(column)), negated, false)
                        }
                        _ => return Ok(Transformed::no(expr)),
                    };
                    if exactly_one_row(&subquery.subquery) {
                        let result = if let Some(Expr::BinaryExpr(binary)) = predicate {
                            (*binary.left).eq(Expr::ScalarSubquery(subquery))
                        } else {
                            lit(true)
                        };
                        return Ok(Transformed::yes(if negated { !result } else { result }));
                    }
                    let mut pull_up = PullUpCorrelatedExpr::new().with_exists_sub_query(exists);
                    let right = (*subquery.subquery).clone().rewrite(&mut pull_up)?.data;
                    if !pull_up.can_pull_up {
                        return Err(datafusion::error::DataFusionError::Plan(
                            "unsupported correlated predicate subquery".into(),
                        ));
                    }
                    if pull_up.pulled_up_scalar_agg {
                        return Err(datafusion::error::DataFusionError::Plan(
                            "correlated predicate subquery with a filtered scalar aggregate is not supported by DataFusion decorrelation".into(),
                        ));
                    }
                    let alias = self.name();
                    let columns: BTreeSet<_> = pull_up
                        .correlated_subquery_cols_map
                        .values()
                        .flatten()
                        .cloned()
                        .collect();
                    let mut filters = pull_up.join_filters;
                    for filter in &mut filters {
                        *filter = filter
                            .clone()
                            .transform_up(|e| match e {
                                Expr::Column(c) if columns.contains(&c) => Ok(Transformed::yes(
                                    Expr::Column(Column::new(Some(alias.clone()), c.name)),
                                )),
                                _ => Ok(Transformed::no(e)),
                            })?
                            .data;
                    }
                    let correlation = conjunction(filters).unwrap_or_else(|| lit(true));
                    let comparison = if let Some(Expr::BinaryExpr(binary)) = predicate {
                        let Expr::Column(column) = *binary.right else {
                            unreachable!()
                        };
                        Some(
                            (*binary.left)
                                .eq(Expr::Column(Column::new(Some(alias.clone()), column.name))),
                        )
                    } else {
                        None
                    };
                    let right = LogicalPlanBuilder::from(right).alias(&alias)?.build()?;
                    let matched = self.mark_subquery(
                        &mut input,
                        right.clone(),
                        &alias,
                        comparison
                            .clone()
                            .map_or_else(|| correlation.clone(), |eq| correlation.clone().and(eq)),
                    )?;
                    let result = if let Some(comparison) = comparison {
                        // DataFusion's mark joins report only true matches. A second
                        // marker detects UNKNOWN comparisons among rows that satisfy
                        // the correlation, preserving SQL's three-valued IN semantics.
                        let unknown = self.mark_subquery(
                            &mut input,
                            right,
                            &alias,
                            correlation.and(comparison.is_null()),
                        )?;
                        datafusion::logical_expr::when(matched, lit(true))
                            .when(unknown, lit(datafusion::common::ScalarValue::Boolean(None)))
                            .otherwise(lit(false))?
                    } else {
                        matched
                    };
                    Ok(Transformed::yes(if negated { !result } else { result }))
                })?;
                // Preserve expression-derived output names and explicit aliases.
                Ok(if rewritten.transformed {
                    rewritten
                        .data
                        .unalias()
                        .alias(original.schema_name().to_string())
                } else {
                    rewritten.data
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((input, exprs))
    }

    fn mark_subquery(
        &mut self,
        input: &mut LogicalPlan,
        right: LogicalPlan,
        alias: &str,
        predicate: Expr,
    ) -> datafusion::error::Result<Expr> {
        let mut cols: Vec<_> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let plan = LogicalPlanBuilder::from(input.clone())
            .join_on(right, JoinType::LeftMark, [predicate])?
            .build()?;
        let marker = self.name();
        cols.push(Expr::Column(Column::new(Some(alias), "mark")).alias(&marker));
        *input = LogicalPlanBuilder::from(plan).project(cols)?.build()?;
        Ok(key_column(input, &marker))
    }
}

fn exactly_one_row(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(p) => exactly_one_row(&p.input),
        LogicalPlan::SubqueryAlias(p) => exactly_one_row(&p.input),
        LogicalPlan::Sort(p) => exactly_one_row(&p.input),
        LogicalPlan::Window(p) => exactly_one_row(&p.input),
        LogicalPlan::Aggregate(p) => p.group_expr.is_empty(),
        LogicalPlan::EmptyRelation(p) => p.produce_one_row,
        _ => false,
    }
}

// Sort expressions can reference outer columns omitted from SELECT. DF55's
// SQL planner does not carry these correlation dependencies through Projection.
pub(super) fn sort_input(
    mut plan: LogicalPlan,
    exprs: &[SortExpr],
) -> datafusion::error::Result<LogicalPlan> {
    let mut columns = BTreeSet::new();
    for sort in exprs {
        sort.expr.apply(|expr| {
            let subquery = match expr {
                Expr::ScalarSubquery(s) => Some(s),
                Expr::Exists(e) => Some(&e.subquery),
                Expr::InSubquery(e) => Some(&e.subquery),
                _ => None,
            };
            if let Some(s) = subquery {
                for e in &s.outer_ref_columns {
                    if let Expr::OuterReferenceColumn(_, col) = e {
                        columns.insert(col.clone());
                    }
                }
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;
    }
    for column in columns {
        plan = expose_column(plan, &column)?;
    }
    Ok(plan)
}

fn expose_column(plan: LogicalPlan, column: &Column) -> datafusion::error::Result<LogicalPlan> {
    if plan.schema().has_column(column) {
        return Ok(plan);
    }
    if let LogicalPlan::Projection(projection) = plan {
        let input = expose_column((*projection.input).clone(), column)?;
        let mut exprs = projection.expr;
        exprs.push(Expr::Column(column.clone()));
        LogicalPlanBuilder::from(input).project(exprs)?.build()
    } else {
        Err(datafusion::error::DataFusionError::Plan(format!(
            "unavailable outer reference in ORDER BY: {column}"
        )))
    }
}
