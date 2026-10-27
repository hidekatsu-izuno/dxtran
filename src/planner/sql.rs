//! Validate allowed SQL and tag multiset operations before DataFusion planning.
use super::names::{self, INTERNAL_PREFIX};
use crate::error::{Error, Result};
use datafusion::sql::sqlparser::{
    ast::{
        ObjectName, Query, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, Visit,
        VisitMut, Visitor, VisitorMut,
    },
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::Tokenizer,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::ControlFlow,
};
fn normalize_ident(ident: &datafusion::sql::sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

#[derive(Default)]
struct Scope {
    names: BTreeSet<String>,
    ctes: BTreeMap<usize, String>,
}
#[derive(Default)]
struct References {
    scopes: Vec<Scope>,
    tables: BTreeSet<String>,
}
impl Visitor for References {
    type Break = String;
    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<String> {
        if q.with.as_ref().is_some_and(|w| w.recursive) {
            return ControlFlow::Break("recursive CTE is forbidden".into());
        }
        self.scopes.push(Scope {
            names: BTreeSet::new(),
            ctes: q
                .with
                .as_ref()
                .map(|w| {
                    w.cte_tables
                        .iter()
                        .map(|c| {
                            (
                                (&*c.query as *const Query) as usize,
                                normalize_ident(&c.alias.name),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        });
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, q: &Query) -> ControlFlow<String> {
        self.scopes.pop();
        if let Some(parent) = self.scopes.last_mut()
            && let Some(name) = parent.ctes.get(&((q as *const Query) as usize))
        {
            parent.names.insert(name.clone());
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<String> {
        if name.0.len() != 1 {
            return ControlFlow::Break("only configured table names are allowed".into());
        }
        let Some(ident) = name.0[0].as_ident() else {
            return ControlFlow::Break("unsupported table name".into());
        };
        let name = normalize_ident(ident);
        if !self.scopes.iter().rev().any(|s| s.names.contains(&name)) {
            self.tables.insert(name);
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<String> {
        match t {
            TableFactor::Table { args: Some(_), .. }
            | TableFactor::TableFunction { .. }
            | TableFactor::Function { .. } => {
                ControlFlow::Break("table functions are forbidden".into())
            }
            _ => ControlFlow::Continue(()),
        }
    }
    fn pre_visit_statement(&mut self, s: &Statement) -> ControlFlow<String> {
        if !matches!(s, Statement::Query(_)) {
            ControlFlow::Break("only SELECT queries are allowed".into())
        } else {
            ControlFlow::Continue(())
        }
    }
}
fn parse_user_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|e| Error::sql(e.to_string()))?;
    let statements = Parser::new(&dialect)
        .with_tokens_with_locations(tokens.clone())
        .parse_statements()
        .map_err(|e| Error::sql(e.to_string()))?;
    if statements.len() != 1 || !matches!(statements[0], Statement::Query(_)) {
        return Err(Error::sql("exactly one SELECT query is required"));
    }
    names::validate_sql(&statements, &tokens)?;
    Ok(statements)
}
pub fn references(sql: &str) -> Result<BTreeSet<String>> {
    let statements = parse_user_sql(sql)?;
    let mut refs = References::default();
    if let ControlFlow::Break(e) = statements.visit(&mut refs) {
        return Err(Error::sql(e));
    }
    Ok(refs.tables)
}
// DataFusion 55 lowers ALL set operations to semi/anti joins. Tag their left
// operands in the AST so order propagation can implement SQL multiset counts.
pub(super) fn prepare_sql(sql: &str, sets: &mut BTreeMap<String, bool>) -> Result<String> {
    let initial_count = sets.len();
    struct Tags<'a> {
        sets: &'a mut BTreeMap<String, bool>,
        original: &'a str,
    }
    impl Tags<'_> {
        fn body(&mut self, body: &mut SetExpr) -> std::result::Result<(), String> {
            if let SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } = body
            {
                self.body(left)?;
                self.body(right)?;
                if *set_quantifier == SetQuantifier::All
                    && matches!(op, SetOperator::Except | SetOperator::Intersect)
                {
                    let mut n = self.sets.len() + 1;
                    let marker = loop {
                        let name = format!("{INTERNAL_PREFIX}set_all_{n}");
                        if !self.sets.contains_key(&name) && !self.original.contains(&name) {
                            break name;
                        }
                        n += 1;
                    };
                    self.sets.insert(marker.clone(), *op == SetOperator::Except);
                    let wrapper = format!("SELECT * FROM ({left}) AS \"{marker}\"");
                    let mut parsed = Parser::parse_sql(&GenericDialect {}, &wrapper)
                        .map_err(|e| e.to_string())?;
                    let Statement::Query(query) = parsed.remove(0) else {
                        unreachable!()
                    };
                    *left = query.body;
                }
            }
            Ok(())
        }
    }
    impl VisitorMut for Tags<'_> {
        type Break = String;
        fn post_visit_query(&mut self, q: &mut Query) -> ControlFlow<String> {
            match self.body(&mut q.body) {
                Ok(()) => ControlFlow::Continue(()),
                Err(e) => ControlFlow::Break(e),
            }
        }
    }
    let mut statements = parse_user_sql(sql)?;
    if let ControlFlow::Break(e) = VisitMut::visit(
        &mut statements,
        &mut Tags {
            sets,
            original: sql,
        },
    ) {
        return Err(Error::sql(e));
    }
    Ok(if sets.len() == initial_count {
        sql.to_owned()
    } else {
        statements[0].to_string()
    })
}
