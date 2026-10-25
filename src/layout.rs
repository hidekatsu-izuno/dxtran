use crate::{
    config::{Dataset, Field, Format},
    encoding::TextEncoding,
    error::{Error, Result},
    field::{CompiledField, FieldType},
};
use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};
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
}
impl RecordLayout {
    pub fn compile(d: &Dataset, output: bool, result: Option<SchemaRef>) -> Result<Self> {
        let encoding = TextEncoding::resolve(&d.encoding)?;
        if (d.csv.is_some() && d.format != Format::Csv)
            || (d.fixed.is_some() && d.format != Format::Fixed)
            || (d.rdw.is_some() && d.format != Format::Rdw)
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
        let max = d.rdw.as_ref().map(|r| r.max_record_length).unwrap_or(32760);
        if !(4..=65535).contains(&max) {
            return Err(Error::config("rdw.max_record_length must be 4..65535"));
        }
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
        let mut specs = d.fields.clone();
        if output && d.format == Format::Csv {
            let schema = result.as_ref().unwrap();
            let mut names = BTreeSet::new();
            for f in &specs {
                let name = f
                    .name
                    .as_deref()
                    .ok_or_else(|| Error::config("CSV output field name required"))?;
                if !names.insert(name) || schema.index_of(name).is_err() {
                    return Err(Error::config(format!(
                        "unknown or duplicate CSV output column '{name}'"
                    )));
                }
            }
            specs = schema
                .fields()
                .iter()
                .map(|column| {
                    let mut f = specs
                        .iter()
                        .find(|f| f.name.as_deref() == Some(column.name()))
                        .cloned()
                        .unwrap_or_else(|| Field {
                            name: Some(column.name().clone()),
                            ..Default::default()
                        });
                    let inferred = match column.data_type() {
                        DataType::Boolean => "boolean",
                        DataType::Date32 => "date",
                        DataType::Time64(_) => "time",
                        DataType::Timestamp(_, _) => "timestamp",
                        DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
                        | DataType::FixedSizeBinary(_) => "binary",
                        DataType::Decimal128(_, _) => "decimal",
                        _ => "string",
                    };
                    if f.kind.is_none() {
                        f.kind = Some(inferred.into());
                    }
                    if let DataType::Decimal128(p, s) = column.data_type()
                        && *s >= 0
                    {
                        f.precision.get_or_insert(*p);
                        f.scale.get_or_insert(*s as u8);
                    }
                    if let DataType::Time64(u) | DataType::Timestamp(u, _) = column.data_type()
                        && f.unit.is_none()
                    {
                        f.unit = Some(
                            match u {
                                TimeUnit::Second => "s",
                                TimeUnit::Millisecond => "ms",
                                TimeUnit::Microsecond => "us",
                                TimeUnit::Nanosecond => "ns",
                            }
                            .into(),
                        );
                    }
                    if let DataType::Timestamp(_, zone) = column.data_type()
                        && !f.timezone_set
                    {
                        f.timezone = zone.as_ref().map(ToString::to_string);
                        f.timezone_set = true;
                    }
                    f.nullable.get_or_insert(column.is_nullable());
                    f
                })
                .collect();
        }
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
            let f = CompiledField::compile(spec, d.format, output, offset, encoding)?;
            if d.format != Format::Csv {
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
        let layout = Self {
            fields,
            schema,
            length,
            rest,
            separator,
            max_record_length: max,
            format: d.format,
            encoding,
        };
        if let Some(result) = result {
            layout.validate_output(d, &result)?;
        }
        Ok(layout)
    }
    fn validate_output(&self, d: &Dataset, schema: &Schema) -> Result<()> {
        let mut names = BTreeSet::new();
        for f in schema.fields() {
            if !crate::config::identifier(f.name()) || !names.insert(f.name()) {
                return Err(Error::config(
                    "SQL output column names must be unique public identifiers",
                ));
            }
        }
        let physical: BTreeSet<_> = self.fields.iter().flat_map(CompiledField::names).collect();
        if d.format != Format::Csv && physical != names.iter().map(|s| (*s).clone()).collect() {
            return Err(Error::config(
                "physical output fields must match the SQL column name set exactly",
            ));
        }
        for f in &self.fields {
            for name in f.names() {
                let col = schema
                    .field_with_name(&name)
                    .map_err(|e| Error::config(e.to_string()))?;
                let explicit = d
                    .fields
                    .iter()
                    .find(|s| s.name.as_deref() == Some(&name))
                    .is_some_and(|s| s.kind.is_some());
                if let DataType::Timestamp(_, zone) = col.data_type()
                    && matches!(f.kind, FieldType::Timestamp)
                    && zone.is_some() != matches!(f.data_type(), DataType::Timestamp(_, Some(_)))
                {
                    return Err(Error::config(
                        "timestamp timezone presence requires explicit SQL conversion",
                    ));
                }
                if d.format == Format::Csv && !explicit {
                    if !csv_type(col.data_type()) {
                        return Err(Error::config(format!(
                            "unsupported CSV SQL type {:?}",
                            col.data_type()
                        )));
                    }
                    continue;
                }
                let t = col.data_type();
                use FieldType::*;
                let compatible = match f.kind {
                    String => {
                        matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
                    }
                    Decimal | Zoned | Lzoned | Tzoned | Uzoned | Packed | Upacked | Npacked => {
                        matches!(t, DataType::Decimal128(_, _))
                    }
                    Boolean | Bits => *t == DataType::Boolean,
                    Date => *t == DataType::Date32,
                    Time => matches!(t, DataType::Time64(_)),
                    Timestamp => {
                        matches!(t,DataType::Timestamp(_,z) if z.is_some()==matches!(f.data_type(),DataType::Timestamp(_,Some(_))))
                    }
                    Binary => matches!(
                        t,
                        DataType::Binary
                            | DataType::BinaryView
                            | DataType::LargeBinary
                            | DataType::FixedSizeBinary(_)
                    ),
                    IntLe | IntBe | UintLe | UintBe => t.is_integer(),
                    FloatLe | FloatBe | HfpLe | HfpBe => {
                        matches!(t, DataType::Float32 | DataType::Float64)
                    }
                    Filler => true,
                };
                if !compatible {
                    return Err(Error::config(format!(
                        "output column '{name}' type {t:?} is incompatible with {:?}; use SQL CAST",
                        f.kind
                    )));
                }
            }
        }
        Ok(())
    }
}
fn csv_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Date32
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Binary
            | DataType::BinaryView
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
            | DataType::Float32
            | DataType::Float64
    ) || t.is_integer()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Length;
    #[test]
    fn multibyte_padding() {
        let f = CompiledField::compile(
            Field {
                name: Some("name".into()),
                length: Some(Length::Fixed(8)),
                pad_char: Some("　".into()),
                ..Default::default()
            },
            Format::Fixed,
            true,
            0,
            TextEncoding::resolve("utf-16le").unwrap(),
        )
        .unwrap();
        assert_eq!(f.padded("A").unwrap(), [0x41, 0, 0, 0x30, 0, 0x30, 0, 0x30]);
    }
}
