//! SQL predicates select the physical fields that cover each record.
use super::RecordLayout;
use crate::{
    config::{Dataset, Format},
    encoding::TextEncoding,
    error::{Error, Result},
    field::{CompiledField, Direction, ValueSink},
};
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, AsArray},
        datatypes::{Field as ArrowField, Schema, SchemaRef},
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    common::{DFSchema, ScalarValue},
    physical_expr::PhysicalExpr,
    prelude::SessionContext,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
struct Condition {
    sql: String,
    origin: String,
    expression: Arc<dyn PhysicalExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct ConditionalLayout {
    conditions: Vec<Condition>,
    field_conditions: Vec<Option<usize>>,
    columns: Vec<Vec<usize>>,
    ranges: Vec<usize>,
    common: Vec<usize>,
    schema: SchemaRef,
    output: bool,
}

pub(crate) struct SelectedFields {
    pub fields: Vec<usize>,
    pub length: usize,
    rest: bool,
}

pub(super) fn compile(
    d: &Dataset,
    result: Option<SchemaRef>,
    encoding: TextEncoding,
    separator: Vec<u8>,
    max: usize,
) -> Result<RecordLayout> {
    if !matches!(d.format, Format::Fixed | Format::Rdw) {
        return Err(Error::config("field when is only supported for fixed/rdw"));
    }
    let bad = |message: &str| Error::config(message).context(&d.id);
    let output = result.is_some();
    let mut end = 0usize;
    let mut length = 0usize;
    let mut rest = false;
    let mut fields = Vec::new();
    for spec in &d.fields {
        let offset = spec.offset.unwrap_or(end);
        let field = CompiledField::compile(
            spec.clone(),
            d.format,
            if output {
                Direction::Output
            } else {
                Direction::Input
            },
            offset,
            encoding,
        )?;
        end = offset
            .checked_add(field.length.unwrap_or(0))
            .ok_or_else(|| bad("field position overflow"))?;
        length = length.max(end);
        rest |= field.length.is_none();
        fields.push(field);
    }
    if length > crate::framing::MAX_RECORD
        || d.format == Format::Fixed && length == 0
        || d.format == Format::Rdw && length + 4 > max
    {
        return Err(bad("record length outside supported range"));
    }
    if d.format == Format::Fixed
        && d.fixed
            .as_ref()
            .unwrap()
            .record_length
            .is_some_and(|n| n != length)
    {
        return Err(bad(
            "fixed.record_length must equal the fully covered layout length",
        ));
    }
    // Alternative definitions of the same column share one SQL column. Their
    // codecs and positions may differ, while their logical types must agree.
    let mut columns: Vec<ArrowField> = Vec::new();
    let mut indices: Vec<Vec<usize>> = Vec::new();
    let mut common = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let mut mapping = Vec::new();
        for name in field.names() {
            let column = if let Some(c) = columns.iter().position(|c| c.name() == &name) {
                if columns[c].data_type() != &field.data_type() {
                    return Err(bad(&format!(
                        "conditional column '{name}' must have the same logical type in every definition"
                    )));
                }
                c
            } else {
                columns.push(ArrowField::new(name, field.data_type(), false));
                columns.len() - 1
            };
            let nullable = columns[column].is_nullable()
                || field.spec.nullable == Some(true)
                || field.spec.when.is_some();
            columns[column] = columns[column].clone().with_nullable(nullable);
            mapping.push(column);
        }
        indices.push(mapping);
        if field.spec.when.is_none() {
            common.push(i);
        }
    }
    let schema = Arc::new(Schema::new(columns));
    let condition_schema = result.clone().unwrap_or_else(|| {
        Arc::new(Schema::new(
            common
                .iter()
                .flat_map(|&i| indices[i].iter().map(|&c| schema.field(c).clone()))
                .collect::<Vec<_>>(),
        ))
    });
    let df_schema =
        DFSchema::try_from(condition_schema.as_ref().clone()).map_err(|e| bad(&e.to_string()))?;
    let context = SessionContext::new();
    crate::functions::register(&context);
    let mut conditions: Vec<Condition> = Vec::new();
    let mut field_conditions = Vec::new();
    for field in &fields {
        field_conditions.push(if let Some(sql) = &field.spec.when {
            if let Some(i) = conditions.iter().position(|c| &c.sql == sql) {
                Some(i)
            } else {
                let origin = field
                    .spec
                    .origins
                    .get("when")
                    .map(ToString::to_string)
                    .unwrap_or_else(|| {
                        format!(
                            "{} field {}",
                            d.id,
                            field.spec.name.as_deref().unwrap_or("FILLER")
                        )
                    });
                let expression =
                    crate::predicate::compile(&context, sql, &df_schema).map_err(|e| {
                        Error::config(e.to_string()).context(format!("{origin} when: {sql}"))
                    })?;
                conditions.push(Condition {
                    sql: sql.clone(),
                    origin,
                    expression,
                });
                Some(conditions.len() - 1)
            }
        } else {
            None
        });
    }
    let mut ranges: Vec<_> = (0..fields.len()).collect();
    ranges.sort_by_key(|&i| (fields[i].offset, fields[i].length));
    // Reject conflicts that are certain without evaluating SQL. Predicates
    // with different text can overlap; those are checked for every record.
    for (position, &i) in ranges.iter().enumerate() {
        for &j in &ranges[position + 1..] {
            if fields[i].spec.when != fields[j].spec.when {
                continue;
            }
            let overlaps = fields[i]
                .length
                .is_none_or(|n| fields[i].offset + n > fields[j].offset);
            let duplicate = indices[i].iter().any(|c| indices[j].contains(c));
            if overlaps || duplicate {
                return Err(bad(
                    "fields active under the same condition have overlaps or duplicate column names",
                ));
            }
        }
    }
    let layout = RecordLayout {
        fields,
        schema,
        length,
        rest,
        separator,
        max_record_length: max,
        format: d.format,
        encoding,
        conditional: Some(ConditionalLayout {
            conditions,
            field_conditions,
            columns: indices,
            ranges,
            common,
            schema: condition_schema,
            output,
        }),
        checks: Default::default(),
    };
    if let Some(schema) = result {
        layout.validate_output(d, &schema)?;
    }
    Ok(layout)
}

impl ConditionalLayout {
    fn error(&self, message: impl Into<String>) -> Error {
        if self.output {
            Error::output(message)
        } else {
            Error::input(message)
        }
    }
    pub(crate) fn evaluate(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.conditions
            .iter()
            .map(|condition| {
                condition
                    .expression
                    .evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
                    .map_err(|e| {
                        self.error(e.to_string())
                            .context(format!("{} when: {}", condition.origin, condition.sql))
                    })
            })
            .collect()
    }
    pub(crate) fn select(
        &self,
        layout: &RecordLayout,
        predicates: &[ArrayRef],
        row: usize,
    ) -> Result<SelectedFields> {
        let mut selected = SelectedFields {
            fields: Vec::new(),
            length: 0,
            rest: false,
        };
        let mut columns = vec![false; layout.schema.fields().len()];
        for &i in &self.ranges {
            if let Some(condition) = self.field_conditions[i] {
                let values = predicates[condition].as_boolean();
                if values.is_null(row) || !values.value(row) {
                    continue;
                }
            }
            let field = &layout.fields[i];
            if selected.rest || field.offset != selected.length {
                return Err(self.error("selected record fields must cover the body without overlaps or gaps; length rest must be last"));
            }
            for &column in &self.columns[i] {
                if std::mem::replace(&mut columns[column], true) {
                    return Err(self.error(format!(
                        "duplicate active column '{}' in selected record fields",
                        layout.schema.field(column).name()
                    )));
                }
            }
            selected.length = field.offset + field.length.unwrap_or(0);
            selected.rest = field.length.is_none();
            selected.fields.push(i);
        }
        if selected.fields.is_empty()
            || layout.format == Format::Fixed && selected.length != layout.length
        {
            return Err(self.error("selected record fields do not cover the record length (no matching record condition or missing FILLER)"));
        }
        Ok(selected)
    }
}

fn decode_field(field: &CompiledField, body: &[u8], values: &mut impl ValueSink) -> Result<()> {
    let end = field.length.map(|n| field.offset + n).unwrap_or(body.len());
    let bytes = body
        .get(field.offset..end)
        .ok_or_else(|| Error::input("field lies outside record body"));
    bytes
        .and_then(|bytes| field.decode_to(bytes, values))
        .map_err(|e| {
            e.context(format!(
                "column {} offset {} width {}",
                field.spec.name.as_deref().unwrap_or("FILLER"),
                field.offset,
                end.saturating_sub(field.offset)
            ))
        })
}

impl RecordLayout {
    pub(crate) fn decode_physical(&self, body: &[u8], values: &mut impl ValueSink) -> Result<()> {
        let Some(conditional) = &self.conditional else {
            for field in &self.fields {
                decode_field(field, body, values)?;
            }
            return Ok(());
        };
        let mut row: Vec<_> = self
            .schema
            .fields()
            .iter()
            .map(|f| ScalarValue::try_from(f.data_type()).map_err(Error::from))
            .collect::<Result<_>>()?;
        let mut decoded = Vec::new();
        let mut decode = |i: usize| -> Result<()> {
            decoded.clear();
            decode_field(&self.fields[i], body, &mut decoded)?;
            for (column, value) in conditional.columns[i].iter().zip(decoded.drain(..)) {
                row[*column] = value;
            }
            Ok(())
        };
        for &i in &conditional.common {
            decode(i)?;
        }
        let arrays = conditional
            .common
            .iter()
            .flat_map(|&i| conditional.columns[i].iter())
            .map(|&c| row[c].to_array_of_size(1).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new_with_options(
            conditional.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| Error::input(e.to_string()))?;
        let predicates = conditional.evaluate(&batch)?;
        let selected = conditional.select(self, &predicates, 0)?;
        if body.len() < selected.length || !selected.rest && body.len() != selected.length {
            return Err(Error::input(
                "record body does not match selected record condition length",
            ));
        }
        for i in selected.fields {
            if self.fields[i].spec.when.is_none() {
                continue;
            }
            decoded.clear();
            decode_field(&self.fields[i], body, &mut decoded)?;
            for (column, value) in conditional.columns[i].iter().zip(decoded.drain(..)) {
                row[*column] = value;
            }
        }
        for value in row {
            values.scalar(value)?;
        }
        Ok(())
    }
}
