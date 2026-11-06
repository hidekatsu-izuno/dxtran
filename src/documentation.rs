//! Markdown documentation from the expanded definition, without executing the job.
use crate::{
    config::{self, Job},
    error::Result,
};
use serde_json::Value;
use std::{fmt::Write, path::Path};

/// Read a job and generate its Markdown documentation, without reading data or running Lua/SQL.
pub fn generate(path: &Path) -> Result<String> {
    let job = config::load_for_documentation(
        path,
        &config::PathOverrides::default(),
        &mut config::ExitCodes::default(),
    )?;
    Ok(render(&job, path))
}

fn display(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
fn cell(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('|', "\\|")
        .replace("\r\n", "\n")
        .replace('\n', "<br>")
}
fn ticks(value: &str, minimum: usize) -> String {
    "`".repeat(
        value
            .split(|c| c != '`')
            .map(str::len)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(minimum),
    )
}
fn code(value: &str) -> String {
    let fence = ticks(value, 1);
    format!(
        "{fence} {} {fence}",
        value.replace('|', "\\|").replace(['\n', '\r'], " ")
    )
}
fn item(value: &Value, key: &str) -> String {
    value
        .get(key)
        .map(|value| code(&display(value)))
        .unwrap_or_else(|| "—".into())
}
fn description(value: &Value) -> &str {
    value
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
}
fn paragraph(out: &mut String, text: &str) {
    if !text.is_empty() {
        let _ = writeln!(out, "{}\n", text.trim());
    }
}
fn settings(out: &mut String, value: &Value, excluded: &[&str]) {
    let Some(map) = value.as_object() else {
        return;
    };
    let entries: Vec<_> = map
        .iter()
        .filter(|(key, _)| key.as_str() != "description" && !excluded.contains(&key.as_str()))
        .collect();
    if entries.is_empty() {
        return;
    }
    out.push_str("| Setting | Value |\n| --- | --- |\n");
    for (key, value) in entries {
        let _ = writeln!(out, "| {} | {} |", code(key), code(&display(value)));
    }
    out.push('\n');
}
fn sql(out: &mut String, value: &Value) {
    let Some(text) = value.get("sql").and_then(Value::as_str) else {
        return;
    };
    out.push_str("#### SQL\n\n");
    let fence = ticks(text, 3);
    let _ = writeln!(out, "{fence}sql\n{}\n{fence}\n", text.trim_end());
}
fn checks(out: &mut String, value: &Value) {
    for key in ["check", "before", "after"] {
        let Some(checks) = value.get(key) else {
            continue;
        };
        let checks = checks
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_else(|| std::slice::from_ref(checks));
        if checks.is_empty() {
            continue;
        }
        let _ = writeln!(out, "#### {}\n", code(key));
        out.push_str("| No. | From | Condition (when) | Level | Message | Description |\n| --- | --- | --- | --- | --- | --- |\n");
        for (index, check) in checks.iter().enumerate() {
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} |",
                index + 1,
                item(check, "from"),
                item(check, "when"),
                item(check, "level"),
                item(check, "message"),
                cell(description(check))
            );
        }
        out.push('\n');
    }
}
fn additional(field: &Value) -> String {
    let Some(settings) = field.as_object() else {
        return "—".into();
    };
    let entries: Vec<_> = settings
        .iter()
        .filter(|(key, _)| {
            ![
                "description",
                "name",
                "source_name",
                "type",
                "offset",
                "length",
                "nullable",
            ]
            .contains(&key.as_str())
        })
        .map(|(key, value)| code(&format!("{key}: {}", display(value))))
        .collect();
    if entries.is_empty() {
        "—".into()
    } else {
        entries.join("<br>")
    }
}

fn fields(out: &mut String, dataset: &Value, output: bool) {
    out.push_str("#### Fields\n\n");
    if output && dataset.get("format").and_then(Value::as_str) == Some("csv") {
        paragraph(
            out,
            "Per-field output settings. The SQL result determines the actual columns, column order, and types.",
        );
    }
    let Some(fields) = dataset
        .get("fields")
        .and_then(Value::as_array)
        .filter(|fields| !fields.is_empty())
    else {
        paragraph(
            out,
            if output {
                "Uses the SQL result columns and types. No per-field settings are defined."
            } else {
                "Uses the input file schema."
            },
        );
        return;
    };
    out.push_str("| No. | Name | Source name | Type | Offset | Length | Nullable | Additional | Description |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for (index, field) in fields.iter().enumerate() {
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("FILLER");
        let kind = field
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(if name == "FILLER" {
                "FILLER"
            } else if output {
                "Inherited from SQL result"
            } else {
                "string"
            });
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            index + 1,
            code(name),
            item(field, "source_name"),
            code(kind),
            item(field, "offset"),
            item(field, "length"),
            item(field, "nullable"),
            additional(field),
            cell(description(field))
        );
    }
    out.push('\n');
}

/// Render the source definition, keeping SQL references and path templates unbound.
pub fn render(job: &Job, source: &Path) -> String {
    let mut out = String::from("# Job Definition\n\n");
    let _ = writeln!(
        out,
        "Definition file: {}\n",
        code(&source.display().to_string())
    );
    paragraph(&mut out, job.description.as_deref().unwrap_or(""));
    let _ = writeln!(
        out,
        "Inputs: {} / Processes: {} / Outputs: {}\n",
        job.input.len(),
        job.process.len(),
        job.output.len()
    );
    paragraph(&mut out, "An em dash (—) indicates an unspecified setting.");
    out.push_str("## Parameters\n\n");
    if job.params.is_empty() {
        paragraph(&mut out, "None defined.");
    } else {
        out.push_str(
            "| Name | Type | Required | Default (SQL expression) | Description |\n| --- | --- | --- | --- | --- |\n",
        );
        for (name, param) in &job.params {
            let definition = &job.definition["params"][name];
            let kind = definition
                .get("type")
                .map(|v| code(&display(v)))
                .unwrap_or_else(|| "Unspecified (CLI: string; default: SQL result type)".into());
            let required = if param.default.is_none() { "Yes" } else { "No" };
            let default = param
                .default
                .as_deref()
                .map(code)
                .unwrap_or_else(|| "—".into());
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} | {} |",
                code(name),
                kind,
                required,
                default,
                cell(param.description.as_deref().unwrap_or(""))
            );
        }
        out.push('\n');
    }
    for (key, title, output) in [("input", "Inputs", false), ("output", "Outputs", true)] {
        let _ = writeln!(out, "## {title}\n");
        for (index, dataset) in job.definition[key].as_array().unwrap().iter().enumerate() {
            let _ = writeln!(out, "### {}. {}\n", index + 1, item(dataset, "id"));
            paragraph(&mut out, description(dataset));
            settings(
                &mut out,
                dataset,
                &[
                    "id", "fields", "sql", "check", "before", "after", "csv", "fixed", "lines",
                    "rdw", "parquet", "lua",
                ],
            );
            if dataset.get("path").is_none() {
                paragraph(&mut out, "path: supplied via the CLI.");
            }
            for block in ["csv", "fixed", "lines", "rdw", "parquet", "lua"] {
                if let Some(options) = dataset.get(block) {
                    let _ = writeln!(out, "#### {} settings\n", code(block));
                    paragraph(&mut out, description(options));
                    settings(&mut out, options, &[]);
                }
            }
            fields(&mut out, dataset, output);
            sql(&mut out, dataset);
            checks(&mut out, dataset);
        }
        if key == "input" {
            out.push_str("## Processes\n\n");
            if job.process.is_empty() {
                paragraph(&mut out, "None defined.");
            }
            if let Some(processes) = job.definition.get("process").and_then(Value::as_array) {
                for (index, process) in processes.iter().enumerate() {
                    let _ = writeln!(out, "### {}. {}\n", index + 1, item(process, "id"));
                    paragraph(&mut out, description(process));
                    sql(&mut out, process);
                    checks(&mut out, process);
                }
            }
        }
    }
    for (key, title) in [("lua", "Job Lua Settings"), ("exit_codes", "Exit Codes")] {
        if let Some(value) = job.definition.get(key) {
            let _ = writeln!(out, "## {title}\n");
            paragraph(&mut out, description(value));
            settings(&mut out, value, &[]);
        }
    }
    out
}
