//! Recognize ${name} as SQL placeholder tokens, leaving quoted SQL and comments intact.
use crate::error::{Error, Result};
use datafusion::sql::sqlparser::{
    ast::{Expr, Statement},
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{Token, TokenWithSpan, Tokenizer},
};

fn tokens(sql: &str, dialect: &GenericDialect) -> Result<Vec<TokenWithSpan>> {
    let mut tokens = Tokenizer::new(dialect, sql)
        .tokenize_with_location()
        .map_err(|e| Error::sql(e.to_string()))?
        .into_iter()
        .peekable();
    let mut result = Vec::new();
    while let Some(mut token) = tokens.next() {
        if let Token::Placeholder(name) = &token.token {
            if name != "$" {
                return Err(Error::config(
                    "SQL parameters must use ${name}, not $name or positional placeholders",
                ));
            }
            let invalid = || Error::config("invalid SQL parameter reference; expected ${name}");
            let brace = tokens.next().ok_or_else(invalid)?;
            let name = tokens.next().ok_or_else(invalid)?;
            let end = tokens.next().ok_or_else(invalid)?;
            let Token::Word(word) = &name.token else {
                return Err(invalid());
            };
            if brace.token != Token::LBrace
                || end.token != Token::RBrace
                || word.quote_style.is_some()
                || !super::identifier(&word.value)
                || token.span.end != brace.span.start
                || brace.span.end != name.span.start
                || name.span.end != end.span.start
            {
                return Err(invalid());
            }
            token.token = Token::Placeholder(format!("${{{}}}", word.value));
            token.span.end = end.span.end;
        }
        result.push(token);
    }
    Ok(result)
}

pub(super) fn expression(sql: &str) -> Result<Expr> {
    let dialect = GenericDialect {};
    let tokens = tokens(sql, &dialect).map_err(|e| Error::config(e.message))?;
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let expr = parser
        .parse_expr()
        .map_err(|e| Error::config(e.to_string()))?;
    if parser.peek_token().token != Token::EOF {
        return Err(Error::config("expected exactly one SQL expression"));
    }
    Ok(expr)
}

pub(super) fn query(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    Parser::new(&dialect)
        .with_tokens_with_locations(tokens(sql, &dialect)?)
        .parse_statements()
        .map_err(|e| Error::sql(e.to_string()))
}
