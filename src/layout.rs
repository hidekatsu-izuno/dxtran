mod conditional;
mod csv;
mod output;
mod repeat;
use crate::{
    config::{Dataset, Format},
    encoding::TextEncoding,
    error::{Error, Result},
    field::{CompiledField, Direction},
};
pub(crate) use conditional::{DecodeScratch, SelectionCache};
use datafusion::arrow::datatypes::{Field as ArrowField, Schema, SchemaRef};
use std::{collections::BTreeSet, sync::Arc};

#[derive(Clone, Debug)]
pub struct RecordLayout {
    pub fields: Vec<CompiledField>,
    pub schema: SchemaRef,
    pub length: usize,
    pub rest: bool,
    pub separator: Vec<u8>,
    pub max_record_length: usize,
    pub format: Format,
    pub encoding: TextEncoding,
    pub(crate) lines: Option<crate::framing::lines::LineFraming>,
    pub(crate) conditional: Option<conditional::ConditionalLayout>,
    pub(crate) repeated: Option<repeat::RepeatLayout>,
    pub(crate) checks: crate::check::Checks,
}
impl RecordLayout {
    pub fn input(dataset: &Dataset) -> Result<Self> {
        let mut layout = Self::compile(dataset, None)?;
        layout.checks = crate::check::Checks::compile(dataset, layout.schema.clone(), false)?;
        Ok(layout)
    }
    pub fn output(dataset: &Dataset, schema: SchemaRef) -> Result<Self> {
        let mut layout = Self::compile(dataset, Some(schema.clone()))?;
        layout.checks = crate::check::Checks::compile(dataset, schema, true)?;
        Ok(layout)
    }
    fn compile(d: &Dataset, result: Option<SchemaRef>) -> Result<Self> {
        let output = result.is_some();
        if d.apache.is_some() && d.format != Format::Apache {
            return Err(Error::config("apache block requires format: apache").context(&d.id));
        }
        if d.format == Format::Apache {
            if output {
                return Err(Error::config("apache is an input-only format"));
            }
            let options = d.apache.clone().unwrap_or_default();
            let count = options.compile()?.columns();
            if d.fields.len() != count {
                return Err(Error::config(format!(
                    "Apache {} input.fields must contain exactly {count} fields in log order",
                    options.format
                )));
            }
        }
        let repeated = d
            .fields
            .iter()
            .any(|f| f.repeat.is_some() || f.fields.is_some());
        if repeated && !matches!(d.format, Format::Fixed | Format::Lines | Format::Rdw) {
            return Err(Error::config(
                "repeat and nested fields require fixed/lines/rdw",
            ));
        }
        let mut external_names = BTreeSet::new();
        for field in &d.fields {
            if field.source_name.is_some()
                && (output
                    || d.format != Format::Csv
                    || !d.csv.as_ref().is_none_or(|csv| csv.header))
            {
                return Err(
                    Error::config("source_name requires CSV input with header: true").context(
                        field
                            .origin
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default(),
                    ),
                );
            }
            if !output
                && d.format == Format::Csv
                && let Some(name) = field.source_name.as_ref().or(field.name.as_ref())
                && !external_names.insert(name)
            {
                return Err(Error::config(format!("duplicate CSV source name '{name}'")));
            }
        }
        if d.format == Format::Lua {
            if let Some(schema) = result {
                return crate::sink::lua::layout(d, schema);
            }
            return crate::source::lua::layout(d);
        }
        if d.lua.is_some() {
            return Err(Error::config("lua block requires format: lua"));
        }
        if d.format == Format::Parquet {
            if d.compression.is_some() {
                return Err(Error::config(
                    "top-level compression is for csv/apache/fixed/lines/rdw; use parquet.compression for Parquet output",
                ));
            }
            return crate::parquet_io::layout(d, result);
        }
        let encoding = TextEncoding::resolve(&d.encoding)?;
        if (d.csv.is_some() && d.format != Format::Csv)
            || (d.fixed.is_some() && d.format != Format::Fixed)
            || (d.lines.is_some() && d.format != Format::Lines)
            || (d.rdw.is_some() && d.format != Format::Rdw)
            || d.parquet.is_some()
        {
            return Err(Error::config("format-specific block does not match format").context(&d.id));
        }
        if !matches!(
            d.mode.as_deref(),
            None | Some("error_if_exists" | "overwrite")
        ) {
            return Err(Error::config("invalid output mode").context(&d.id));
        }
        let csv = d.csv.clone().unwrap_or_default();
        for s in [&csv.delimiter, &csv.quote] {
            if s.len() != 1
                || !s.as_bytes()[0].is_ascii()
                || matches!(s.as_bytes()[0], b'\r' | b'\n' | 0)
            {
                return Err(Error::config(
                    "CSV delimiter/quote must be distinct ASCII characters other than CR/LF/NUL",
                ));
            }
        }
        if csv.delimiter == csv.quote {
            return Err(Error::config("CSV delimiter and quote must differ"));
        }
        if !matches!(csv.line_ending.as_deref(), None | Some("lf" | "crlf"))
            || !matches!(
                csv.quote_style.as_deref(),
                None | Some("always" | "necessary")
            )
            || !output && (csv.line_ending.is_some() || csv.quote_style.is_some())
        {
            return Err(Error::config("invalid or output-only CSV option"));
        }
        let rdw = d.rdw.clone().unwrap_or_default();
        rdw.validate()?;
        let max = rdw.record_limit();
        let separator = if d.format == Format::Fixed {
            let fixed = d
                .fixed
                .as_ref()
                .ok_or_else(|| Error::config("fixed block is required"))?;
            encoding
                .encode(match fixed.record_separator.as_str() {
                    "none" => "",
                    "lf" => "\n",
                    "crlf" => "\r\n",
                    _ => return Err(Error::config("invalid fixed.record_separator")),
                })
                .map_err(|e| Error::config(e.message))?
        } else {
            vec![]
        };
        if repeated {
            return repeat::compile(d, result, encoding, separator, max);
        }
        if d.fields.iter().any(|f| f.when.is_some()) {
            return conditional::compile(d, result, encoding, separator, max);
        }
        let specs = match (d.format, result.as_ref()) {
            (Format::Csv, Some(schema)) => csv::output_fields(d, schema)?,
            _ => d.fields.clone(),
        };
        if d.format != Format::Csv && specs.is_empty() {
            return Err(Error::config("physical fields must not be empty"));
        }
        let mut fields = vec![];
        let mut end = 0usize;
        let mut rest = false;
        let mut columns = vec![];
        let mut names = BTreeSet::new();
        let mut ranges = vec![];
        for (i, spec) in specs.into_iter().enumerate() {
            let offset = spec.offset.unwrap_or(end);
            let f = CompiledField::compile(
                spec,
                d.format,
                if output {
                    Direction::Output
                } else {
                    Direction::Input
                },
                offset,
                encoding,
            )?;
            if !matches!(d.format, Format::Csv | Format::Apache) {
                if let Some(n) = f.length {
                    end = offset
                        .checked_add(n)
                        .ok_or_else(|| Error::config("field position overflow"))?;
                    ranges.push((offset, end));
                } else {
                    if i + 1 != d.fields.len() {
                        return Err(Error::config("length rest must be the last field"));
                    }
                    rest = true;
                    ranges.push((offset, offset));
                    end = offset;
                }
            }
            for name in f.names() {
                if !names.insert(name.clone()) {
                    return Err(Error::config(format!("duplicate column '{name}'")));
                }
                columns.push(ArrowField::new(
                    name,
                    f.data_type(),
                    f.spec.nullable.unwrap_or(false),
                ));
            }
            fields.push(f);
        }
        let mut length = 0;
        ranges.sort();
        for (start, stop) in ranges {
            if start != length {
                return Err(Error::config(
                    "layout must cover the record without overlaps or gaps; use FILLER",
                ));
            }
            length = stop;
        }
        if d.format == Format::Fixed
            && let Some(n) = d.fixed.as_ref().unwrap().record_length
        {
            if n == 0 || n != length {
                return Err(Error::config(
                    "fixed.record_length must equal the fully covered layout length",
                ));
            }
            length = n;
        }
        if length > crate::framing::MAX_RECORD
            || d.format == Format::Fixed && length == 0
            || d.format == Format::Rdw && length + 4 > max
        {
            return Err(Error::config("record length outside supported range"));
        }
        let schema = Arc::new(Schema::new(columns));
        let lines = Self::line_framing(d, encoding, length, &fields)?;
        let layout = Self {
            fields,
            schema,
            length,
            rest,
            separator,
            max_record_length: max,
            format: d.format,
            encoding,
            lines,
            conditional: None,
            repeated: None,
            checks: Default::default(),
        };
        if let Some(result) = result {
            layout.validate_output(d, &result)?;
        }
        Ok(layout)
    }

    pub(super) fn line_framing(
        d: &Dataset,
        encoding: TextEncoding,
        length: usize,
        fields: &[CompiledField],
    ) -> Result<Option<crate::framing::lines::LineFraming>> {
        if d.format != Format::Lines {
            return Ok(None);
        }
        let options = d.lines.clone().unwrap_or_default();
        if length == 0 || options.record_length.is_some_and(|n| n != length) {
            return Err(Error::config(
                "lines.record_length must equal the positive, fully covered layout length",
            ));
        }
        let lines = crate::framing::lines::LineFraming::new(encoding, &options)?;
        lines.validate_width(length)?;
        for field in fields {
            lines.validate_width(field.offset)?;
            lines.validate_width(field.length.unwrap())?;
        }
        Ok(Some(lines))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Field, Length};
    #[test]
    fn multibyte_padding() {
        let f = CompiledField::compile(
            Field {
                name: Some("name".into()),
                length: Some(Length::Fixed(8)),
                space_char: Some("　".into()),
                ..Default::default()
            },
            Format::Fixed,
            crate::field::Direction::Output,
            0,
            TextEncoding::resolve("utf-16le").unwrap(),
        )
        .unwrap();
        assert_eq!(f.padded("A").unwrap(), [0x41, 0, 0, 0x30, 0, 0x30, 0, 0x30]);
    }
}
