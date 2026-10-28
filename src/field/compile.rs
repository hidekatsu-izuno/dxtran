//! Compile raw YAML field settings into validated conversion rules.
use super::{CompiledField, Direction, FieldType, Trim, datetime};
use crate::{
    config::{Field, Format, Length, identifier},
    encoding::TextEncoding,
    error::{Error, Result},
};
use regex::Regex;

fn invalid(spec: &Field, message: &str) -> Error {
    Error::config(message).context(format!(
        "{} field {}",
        spec.origin
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        spec.name.as_deref().unwrap_or("FILLER")
    ))
}
struct NumericLayout {
    precision: u8,
    scale: u8,
}
struct TextRules {
    pad: char,
    trim: Trim,
    null_regex: Option<Regex>,
    null_bytes: Option<Vec<u8>>,
}

impl CompiledField {
    pub fn compile(
        spec: Field,
        format: Format,
        direction: Direction,
        offset: usize,
        encoding: TextEncoding,
    ) -> Result<Self> {
        let (kind, length) = validate_storage(&spec, format)?;
        validate_options(&spec, kind, length, format, direction)?;
        let numeric = numeric_layout(&spec, kind, length)?;
        let TextRules {
            pad,
            trim,
            null_regex,
            null_bytes,
        } = text_rules(&spec, kind, length, format)?;
        let pattern = if matches!(
            kind,
            FieldType::Date | FieldType::Time | FieldType::Timestamp
        ) {
            Some(datetime::Pattern::compile(
                &spec,
                spec.kind.as_deref().unwrap(),
            )?)
        } else {
            None
        };
        let (true_values, false_values) = boolean_values(&spec, kind, pad, trim)?;
        validate_bits(&spec, kind, length)?;
        let mut f = Self {
            spec,
            kind,
            offset,
            length,
            precision: numeric.precision,
            scale: numeric.scale,
            encoding,
            pad,
            trim,
            null_bytes,
            null_regex,
            pattern,
            true_values,
            false_values,
            null_output: None,
        };
        if direction == Direction::Output {
            f.validate_output(format)?;
        }
        Ok(f)
    }
    fn validate_output(&mut self, format: Format) -> Result<()> {
        if format == Format::Rdw && self.spec.nullable == Some(true) {
            if self.spec.null_output.is_some() == self.null_bytes.is_some() {
                return Err(Error::config(
                    "rdw nullable output requires exactly one of null_output/null_bytes",
                ));
            }
            let bytes = if let Some(s) = &self.spec.null_output {
                self.padded(s).map_err(|e| Error::config(e.message))?
            } else {
                self.null_bytes.clone().unwrap()
            };
            if !self.is_null_bytes(&bytes)? {
                return Err(Error::config(
                    "rdw NULL output does not match its input NULL rules",
                ));
            }
            self.null_output = Some(bytes);
        }
        if format == Format::Fixed && self.kind != FieldType::Bits && self.kind != FieldType::Filler
        {
            self.fixed_null().map_err(|e| Error::config(e.message))?;
        }
        if self.kind == FieldType::Boolean {
            for (b, candidates) in [(true, &self.true_values), (false, &self.false_values)] {
                let text = self.boolean_text(b);
                let s = self.trim_output_text(&text);
                let bytes = if format == Format::Csv {
                    s.as_bytes().to_vec()
                } else {
                    self.padded(s).map_err(|e| Error::config(e.message))?
                };
                let decoded = if format == Format::Csv {
                    self.trim_text(s).to_owned()
                } else {
                    self.trim_text(&self.encoding.decode(&bytes)?).to_owned()
                };
                if !candidates.contains(&decoded)
                    || format != Format::Csv && self.is_null_bytes(&bytes)?
                {
                    return Err(Error::config(
                        "boolean output must roundtrip and not match a NULL rule",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn validate_storage(spec: &Field, format: Format) -> Result<(FieldType, Option<usize>)> {
    let bad = |message: &str| invalid(spec, message);
    let kind = if spec.name.is_none() && format != Format::Csv {
        FieldType::Filler
    } else {
        FieldType::parse(spec.kind.as_deref().unwrap_or("string"))?
    };
    if kind != FieldType::Filler && !spec.name.as_deref().is_some_and(identifier) {
        return Err(bad("invalid or missing column name"));
    }
    let length = match &spec.length {
        Some(Length::Fixed(n)) if *n > 0 => Some(*n),
        Some(Length::Rest(s))
            if s == "rest"
                && format == Format::Rdw
                && matches!(kind, FieldType::String | FieldType::Binary) =>
        {
            None
        }
        None if format == Format::Csv => None,
        _ => return Err(bad("invalid or missing length")),
    };
    if length.is_some_and(|n| n > crate::framing::MAX_RECORD) {
        return Err(bad("field exceeds 16 MiB record limit"));
    }
    if kind == FieldType::Filler {
        let mut allowed = spec.clone();
        allowed.offset = None;
        allowed.length = None;
        allowed.fill_byte = None;
        allowed.when = None;
        allowed.origin = None;
        allowed.origins.clear();
        if format == Format::Csv
            || format == Format::Rdw && length.is_none()
            || allowed != Field::default()
        {
            return Err(bad(
                "FILLER accepts only offset, length, fill_byte and when",
            ));
        }
    }
    if format == Format::Csv {
        if spec.when.is_some()
            || spec.length.is_some()
            || spec.offset.is_some()
            || spec.pad_char.is_some()
            || spec.null_if.is_some()
            || spec.null_if_pattern.is_some()
            || spec.null_bytes.is_some()
            || spec.null_output.is_some()
            || spec.max_length.is_some()
        {
            return Err(bad("physical layout and NULL markers are forbidden in CSV"));
        }
        if !matches!(
            kind,
            FieldType::String
                | FieldType::Decimal
                | FieldType::Boolean
                | FieldType::Date
                | FieldType::Time
                | FieldType::Timestamp
                | FieldType::Binary
        ) {
            return Err(bad("type is not available for CSV"));
        }
    }
    Ok((kind, length))
}

fn validate_options(
    spec: &Field,
    kind: FieldType,
    length: Option<usize>,
    format: Format,
    direction: Direction,
) -> Result<()> {
    let bad = |message: &str| invalid(spec, message);
    if spec
        .quote_style
        .as_deref()
        .is_some_and(|v| !matches!(v, "always" | "necessary"))
        || spec.quote_style.is_some() && !(direction == Direction::Output && format == Format::Csv)
    {
        return Err(bad("quote_style requires CSV output and always/necessary"));
    }
    if spec.fill_byte.is_some() && kind != FieldType::Filler {
        return Err(bad("fill_byte requires FILLER"));
    }
    if (spec.precision.is_some() || spec.scale.is_some()) && !kind.decimal() {
        return Err(bad("precision/scale require a decimal type"));
    }
    if spec.zone.is_some()
        && !matches!(
            kind,
            FieldType::Zoned | FieldType::Lzoned | FieldType::Tzoned | FieldType::Uzoned
        )
    {
        return Err(bad("zone requires a zoned type"));
    }
    if spec
        .zone
        .as_deref()
        .is_some_and(|s| !matches!(s, "ascii" | "ebcdic"))
    {
        return Err(bad("invalid zone"));
    }
    if (spec.trim.is_some()
        || spec.pad_char.is_some()
        || spec.null_if.is_some()
        || spec.null_if_pattern.is_some()
        || spec.null_output.is_some())
        && !kind.text()
    {
        return Err(bad("text options require a text storage type"));
    }
    if (spec.true_values.is_some()
        || spec.false_values.is_some()
        || spec.true_output.is_some()
        || spec.false_output.is_some())
        && kind != FieldType::Boolean
    {
        return Err(bad("boolean options require boolean"));
    }
    if (spec.datetime_pattern.is_some() || spec.two_digit_year_start.is_some())
        && !matches!(
            kind,
            FieldType::Date | FieldType::Time | FieldType::Timestamp
        )
    {
        return Err(bad("datetime options require a datetime type"));
    }
    if spec.unit.is_some() && !matches!(kind, FieldType::Time | FieldType::Timestamp)
        || spec.timezone_set && kind != FieldType::Timestamp
    {
        return Err(bad("unit/timezone used on incompatible type"));
    }
    if spec.binary_encoding.is_some() && !(kind == FieldType::Binary && format == Format::Csv) {
        return Err(bad("binary_encoding requires CSV binary"));
    }
    if spec
        .binary_encoding
        .as_deref()
        .is_some_and(|s| !matches!(s, "hex" | "base64"))
    {
        return Err(bad("invalid binary_encoding"));
    }
    if (spec.bits.is_some() || spec.bit_order.is_some()) && kind != FieldType::Bits {
        return Err(bad("bits options require bits"));
    }
    if spec.max_length.is_some() && length.is_some() {
        return Err(bad("max_length requires length rest"));
    }
    if spec.nullable != Some(true)
        && (spec.null_if.is_some()
            || spec.null_if_pattern.is_some()
            || spec.null_bytes.is_some()
            || spec.null_output.is_some())
    {
        return Err(bad("NULL rules require nullable true"));
    }
    Ok(())
}

fn numeric_layout(spec: &Field, kind: FieldType, length: Option<usize>) -> Result<NumericLayout> {
    let bad = |message: &str| invalid(spec, message);
    let len = length.unwrap_or(0);
    let valid_width = match kind {
        FieldType::IntLe | FieldType::IntBe | FieldType::UintLe | FieldType::UintBe => {
            (1..=8).contains(&len)
        }
        FieldType::FloatLe | FieldType::FloatBe | FieldType::HfloatLe => matches!(len, 4 | 8),
        FieldType::HfloatBe => matches!(len, 4 | 8 | 16),
        FieldType::Lzoned | FieldType::Tzoned => len >= 2,
        _ => true,
    };
    if !valid_width {
        return Err(bad("incompatible physical width"));
    }
    let capacity = match kind {
        FieldType::Zoned | FieldType::Uzoned => len,
        FieldType::Lzoned | FieldType::Tzoned => len - 1,
        FieldType::Packed | FieldType::Upacked => 2 * len - 1,
        FieldType::Npacked => 2 * len,
        _ => 38,
    };
    let precision = spec.precision.map(usize::from).unwrap_or(capacity);
    if kind.decimal() && (!(1..=38).contains(&precision) || precision > capacity) {
        return Err(bad(
            "decimal precision must be 1..38 and fit physical capacity",
        ));
    }
    let scale = spec.scale.unwrap_or(0);
    if kind.decimal() && scale as usize > precision {
        return Err(bad("scale exceeds precision"));
    }
    Ok(NumericLayout {
        precision: precision as u8,
        scale,
    })
}

fn text_rules(
    spec: &Field,
    kind: FieldType,
    length: Option<usize>,
    format: Format,
) -> Result<TextRules> {
    let bad = |message: &str| invalid(spec, message);
    let pad = spec.pad_char.as_deref().unwrap_or(" ");
    if pad.chars().count() != 1 {
        return Err(bad("pad_char must be one Unicode character"));
    }
    let pad = pad.chars().next().unwrap();
    let trim = spec.trim.as_deref().unwrap_or_else(|| {
        if !kind.text() || format == Format::Csv || length.is_none() {
            "none"
        } else if kind == FieldType::String {
            "right"
        } else {
            "both"
        }
    });
    let trim = match trim {
        "none" => Trim::None,
        "left" => Trim::Left,
        "right" => Trim::Right,
        "both" => Trim::Both,
        _ => return Err(bad("trim must be false, left, right or both")),
    };
    let null_regex = spec
        .null_if_pattern
        .as_ref()
        .map(|s| {
            Regex::new(&format!("\\A(?:{s})\\z")).map_err(|e| {
                bad(&format!("null_if_pattern: {e}")).context(
                    spec.origins
                        .get("null_if_pattern")
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                )
            })
        })
        .transpose()?;
    let null_bytes = spec
        .null_bytes
        .as_ref()
        .map(|s| hex::decode(s).map_err(|_| bad("invalid null_bytes hex")))
        .transpose()?;
    if null_bytes.as_ref().is_some_and(|b| {
        length.is_some_and(|n| b.len() != n) || spec.max_length.is_some_and(|n| b.len() > n)
    }) {
        return Err(bad("null_bytes width mismatch"));
    }
    Ok(TextRules {
        pad,
        trim,
        null_regex,
        null_bytes,
    })
}

fn boolean_values(
    spec: &Field,
    kind: FieldType,
    pad: char,
    trim: Trim,
) -> Result<(Vec<String>, Vec<String>)> {
    let bad = |message: &str| invalid(spec, message);
    let trim_value = |s: &str| trim.apply(s, pad).to_owned();
    let true_values: Vec<String> = spec
        .true_values
        .clone()
        .unwrap_or_else(|| vec!["true".into()])
        .iter()
        .map(|s| trim_value(s))
        .collect();
    let false_values: Vec<String> = spec
        .false_values
        .clone()
        .unwrap_or_else(|| vec!["false".into()])
        .iter()
        .map(|s| trim_value(s))
        .collect();
    if kind == FieldType::Boolean
        && (true_values.is_empty()
            || false_values.is_empty()
            || true_values.iter().any(|s| false_values.contains(s)))
    {
        return Err(bad(
            "boolean candidates must be nonempty and disjoint after trim",
        ));
    }
    Ok((true_values, false_values))
}

fn validate_bits(spec: &Field, kind: FieldType, length: Option<usize>) -> Result<()> {
    let bad = |message: &str| invalid(spec, message);
    let len = length.unwrap_or(0);
    if kind == FieldType::Bits {
        let bits = spec
            .bits
            .as_ref()
            .filter(|b| !b.is_empty())
            .ok_or_else(|| bad("bits must be nonempty"))?;
        let mut positions = std::collections::BTreeSet::new();
        let mut names = std::collections::BTreeSet::new();
        for b in bits {
            if b.bit >= len * 8
                || !identifier(&b.name)
                || !positions.insert(b.bit)
                || !names.insert(&b.name)
            {
                return Err(bad("invalid or duplicate bit number/name"));
            }
        }
        if spec
            .bit_order
            .as_deref()
            .is_some_and(|s| !matches!(s, "msb0" | "lsb0"))
        {
            return Err(bad("invalid bit_order"));
        }
    }
    Ok(())
}
