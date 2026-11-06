//! Expand dataset paths before resolving their YAML or CLI relative base.
use super::Resolved;
use crate::{
    config::Job,
    error::{Error, Result},
};
use datafusion::arrow::datatypes::DataType;
use std::{collections::BTreeMap, path::PathBuf};

fn substitute(template: &str, values: &BTreeMap<String, Resolved>) -> Result<String> {
    let mut remaining = template;
    let mut expanded = String::with_capacity(template.len());
    while let Some(start) = remaining.find("${") {
        // Backslashes before ${ pair off; an odd final backslash escapes ${.
        // Backslashes elsewhere remain unchanged, including Windows separators.
        let prefix = &remaining[..start];
        let backslashes = prefix
            .bytes()
            .rev()
            .take_while(|byte| *byte == b'\\')
            .count();
        expanded.push_str(&prefix[..prefix.len() - backslashes]);
        expanded.extend(std::iter::repeat_n('\\', backslashes / 2));
        if !backslashes.is_multiple_of(2) {
            expanded.push_str("${");
            remaining = &remaining[start + 2..];
            continue;
        }
        let tail = &remaining[start + 2..];
        let end = tail
            .find('}')
            .ok_or_else(|| Error::config("unclosed ${...} in path"))?;
        let name = &tail[..end];
        if !super::identifier(name) {
            return Err(Error::config("invalid parameter name in path ${...}"));
        }
        let value = values
            .get(name)
            .ok_or_else(|| Error::config(format!("undeclared path parameter '{name}'")))?;
        if value.scalar.is_null() {
            return Err(Error::config(format!(
                "path parameter '{name}' must not be NULL"
            )));
        }
        let text = value.scalar.cast_to(&DataType::Utf8).map_err(|_| {
            Error::config(format!(
                "path parameter '{name}' cannot be converted to string"
            ))
        })?;
        expanded.push_str(
            text.try_as_str().flatten().ok_or_else(|| {
                Error::config(format!("path parameter '{name}' must be a string"))
            })?,
        );
        remaining = &tail[end + 1..];
    }
    expanded.push_str(remaining);
    if expanded.is_empty() || expanded.contains('\0') {
        return Err(Error::config(
            "expanded path must not be empty or contain NUL",
        ));
    }
    Ok(expanded)
}

pub(super) fn expand(
    job: &mut Job,
    values: &BTreeMap<String, Resolved>,
) -> Result<Vec<(String, String, PathBuf)>> {
    let mut expanded = Vec::new();
    for (side, datasets) in [("input", &mut job.input), ("output", &mut job.output)] {
        for dataset in datasets {
            let Some(template) = dataset.path_template.take() else {
                continue;
            };
            let path = substitute(&template.raw, values).map_err(|e| {
                e.context(format!("{side}.{}.path {}", dataset.id, template.origin))
            })?;
            dataset.path = template.base.join(path);
            expanded.push((side.to_owned(), dataset.id.clone(), dataset.path.clone()));
        }
    }
    Ok(expanded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        common::ScalarValue,
        sql::sqlparser::ast::{Expr, Value},
    };

    #[test]
    fn backslash_escaping_preserves_literals_and_windows_path_separators() {
        let values = BTreeMap::from([(
            "name".into(),
            Resolved {
                scalar: ScalarValue::Utf8(Some("expanded".into())),
                literal: Expr::Value(Value::SingleQuotedString("expanded".into()).into()),
            },
        )]);
        for (template, expected) in [
            (r"\${unknown}", "${unknown}"),
            (r"\\${name}", r"\expanded"),
            (r"\\\${unknown}", r"\${unknown}"),
            (r"C:\data\\${name}.csv", r"C:\data\expanded.csv"),
            (r"\${literal}_${name}", "${literal}_expanded"),
            ("$${name}", "$expanded"),
        ] {
            assert_eq!(
                substitute(template, &values).unwrap(),
                expected,
                "{template}"
            );
        }
    }
}
