pub mod datetime;
use crate::{
    config::{Field, Format, Length, identifier},
    encoding::TextEncoding,
    error::{Error, Result},
};
use base64::Engine;
use datafusion::{arrow::datatypes::DataType, common::ScalarValue};
use regex::Regex;

const DECIMAL_POWERS: [u128; 39] = {
    let mut powers = [1; 39];
    let mut i = 1;
    while i < powers.len() {
        powers[i] = powers[i - 1] * 10;
        i += 1;
    }
    powers
};

fn pop_decimal_digit(value: &mut u128) -> u8 {
    if let Ok(small) = u64::try_from(*value) {
        *value = u128::from(small / 10);
        (small % 10) as u8
    } else {
        let digit = (*value % 10) as u8;
        *value /= 10;
        digit
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    String,
    Decimal,
    Boolean,
    Date,
    Time,
    Timestamp,
    Binary,
    Bits,
    IntLe,
    IntBe,
    UintLe,
    UintBe,
    FloatLe,
    FloatBe,
    HfloatLe,
    HfloatBe,
    Zoned,
    Lzoned,
    Tzoned,
    Uzoned,
    Packed,
    Upacked,
    Npacked,
    Filler,
}
impl FieldType {
    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "string" => Self::String,
            "decimal" => Self::Decimal,
            "boolean" => Self::Boolean,
            "date" => Self::Date,
            "time" => Self::Time,
            "timestamp" => Self::Timestamp,
            "binary" => Self::Binary,
            "bits" => Self::Bits,
            "int-le" => Self::IntLe,
            "int-be" => Self::IntBe,
            "uint-le" => Self::UintLe,
            "uint-be" => Self::UintBe,
            "float-le" => Self::FloatLe,
            "float-be" => Self::FloatBe,
            "hfloat-le" => Self::HfloatLe,
            "hfloat-be" => Self::HfloatBe,
            "zoned" => Self::Zoned,
            "lzoned" => Self::Lzoned,
            "tzoned" => Self::Tzoned,
            "uzoned" => Self::Uzoned,
            "packed" => Self::Packed,
            "upacked" => Self::Upacked,
            "npacked" => Self::Npacked,
            _ => return Err(Error::config(format!("unknown type '{s}'"))),
        })
    }
    pub fn text(self) -> bool {
        matches!(
            self,
            Self::String
                | Self::Decimal
                | Self::Boolean
                | Self::Date
                | Self::Time
                | Self::Timestamp
        )
    }
    pub fn decimal(self) -> bool {
        matches!(
            self,
            Self::Decimal
                | Self::Zoned
                | Self::Lzoned
                | Self::Tzoned
                | Self::Uzoned
                | Self::Packed
                | Self::Upacked
                | Self::Npacked
        )
    }
    fn little(self) -> bool {
        matches!(
            self,
            Self::IntLe | Self::UintLe | Self::FloatLe | Self::HfloatLe
        )
    }
}
#[derive(Clone, Debug)]
pub struct CompiledField {
    pub spec: Field,
    pub kind: FieldType,
    pub offset: usize,
    pub length: Option<usize>,
    pub precision: u8,
    pub scale: u8,
    pub encoding: TextEncoding,
    pub pad: char,
    pub trim: String,
    pub null_bytes: Option<Vec<u8>>,
    pub null_regex: Option<Regex>,
    pub pattern: Option<datetime::Pattern>,
    pub true_values: Vec<String>,
    pub false_values: Vec<String>,
    pub null_output: Option<Vec<u8>>,
}

/// Consumers can append borrowed variable-width values without allocating a
/// ScalarValue. Validation stays in CompiledField for both row and batch readers.
pub(crate) trait ValueSink {
    fn text(&mut self, value: Option<&str>) -> Result<()>;
    fn decimal(&mut self, value: Option<i128>, precision: u8, scale: i8) -> Result<()>;
    fn binary(&mut self, value: Option<&[u8]>) -> Result<()>;
    fn scalar(&mut self, value: ScalarValue) -> Result<()>;
}
impl ValueSink for Vec<ScalarValue> {
    fn text(&mut self, value: Option<&str>) -> Result<()> {
        self.push(ScalarValue::Utf8(value.map(str::to_owned)));
        Ok(())
    }
    fn decimal(&mut self, value: Option<i128>, precision: u8, scale: i8) -> Result<()> {
        self.push(ScalarValue::Decimal128(value, precision, scale));
        Ok(())
    }
    fn binary(&mut self, value: Option<&[u8]>) -> Result<()> {
        self.push(ScalarValue::Binary(value.map(<[u8]>::to_vec)));
        Ok(())
    }
    fn scalar(&mut self, value: ScalarValue) -> Result<()> {
        self.push(value);
        Ok(())
    }
}
impl CompiledField {
    pub fn compile(
        spec: Field,
        format: Format,
        output: bool,
        offset: usize,
        encoding: TextEncoding,
    ) -> Result<Self> {
        let bad = |s: &str| {
            Error::config(s).context(format!(
                "{} field {}",
                spec.origin
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                spec.name.as_deref().unwrap_or("FILLER")
            ))
        };
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
        let len = length.unwrap_or(0);
        if kind == FieldType::Filler {
            let mut allowed = spec.clone();
            allowed.offset = None;
            allowed.length = None;
            allowed.fill_byte = None;
            allowed.origin = None;
            allowed.origins.clear();
            if format == Format::Csv
                || format == Format::Rdw && length.is_none()
                || allowed != Field::default()
            {
                return Err(bad("FILLER accepts only offset, length and fill_byte"));
            }
        }
        if format == Format::Csv {
            if spec.length.is_some()
                || spec.offset.is_some()
                || spec.trim.is_some()
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
        if spec
            .quote_style
            .as_deref()
            .is_some_and(|v| !matches!(v, "always" | "necessary"))
            || spec.quote_style.is_some() && !(output && format == Format::Csv)
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
        let pad = spec.pad_char.as_deref().unwrap_or(" ");
        if pad.chars().count() != 1 {
            return Err(bad("pad_char must be one Unicode character"));
        }
        let pad = pad.chars().next().unwrap();
        let trim = spec.trim.clone().unwrap_or_else(|| {
            if !kind.text() || format == Format::Csv || length.is_none() {
                "none"
            } else if kind == FieldType::String {
                "right"
            } else {
                "both"
            }
            .into()
        });
        if !matches!(trim.as_str(), "none" | "right" | "both") {
            return Err(bad("invalid trim"));
        }
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
        let trim_value = |s: &str| match trim.as_str() {
            "both" => s.trim_matches(pad).to_string(),
            "right" => s.trim_end_matches(pad).to_string(),
            _ => s.to_owned(),
        };
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
        let mut f = Self {
            spec,
            kind,
            offset,
            length,
            precision: precision as u8,
            scale,
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
        if output && format == Format::Rdw && f.spec.nullable == Some(true) {
            if f.spec.null_output.is_some() == f.null_bytes.is_some() {
                return Err(Error::config(
                    "rdw nullable output requires exactly one of null_output/null_bytes",
                ));
            }
            let bytes = if let Some(s) = &f.spec.null_output {
                f.padded(s).map_err(|e| Error::config(e.message))?
            } else {
                f.null_bytes.clone().unwrap()
            };
            if !f.is_null_bytes(&bytes)? {
                return Err(Error::config(
                    "rdw NULL output does not match its input NULL rules",
                ));
            }
            f.null_output = Some(bytes);
        }
        if output && format == Format::Fixed && kind != FieldType::Bits && kind != FieldType::Filler
        {
            f.fixed_null().map_err(|e| Error::config(e.message))?;
        }
        if kind == FieldType::Boolean && output {
            for (b, candidates) in [(true, &f.true_values), (false, &f.false_values)] {
                let s = f.boolean_text(b);
                let bytes = if format == Format::Csv {
                    s.as_bytes().to_vec()
                } else {
                    f.padded(&s).map_err(|e| Error::config(e.message))?
                };
                let decoded = if format == Format::Csv {
                    s
                } else {
                    f.trim_text(&f.encoding.decode(&bytes)?).to_owned()
                };
                if !candidates.contains(&decoded)
                    || format != Format::Csv && f.is_null_bytes(&bytes)?
                {
                    return Err(Error::config(
                        "boolean output must roundtrip and not match a NULL rule",
                    ));
                }
            }
        }
        Ok(f)
    }
    pub fn names(&self) -> Vec<String> {
        if self.kind == FieldType::Filler {
            vec![]
        } else if self.kind == FieldType::Bits {
            self.spec
                .bits
                .as_ref()
                .unwrap()
                .iter()
                .map(|b| b.name.clone())
                .collect()
        } else {
            vec![self.spec.name.clone().unwrap()]
        }
    }
    pub fn data_type(&self) -> DataType {
        use FieldType::*;
        match self.kind {
            String => DataType::Utf8,
            Decimal | Zoned | Lzoned | Tzoned | Uzoned | Packed | Upacked | Npacked => {
                DataType::Decimal128(self.precision, self.scale as i8)
            }
            Boolean | Bits => DataType::Boolean,
            Date | Time | Timestamp => self.pattern.as_ref().unwrap().data_type(),
            Binary => DataType::Binary,
            IntLe | IntBe => DataType::Int64,
            UintLe | UintBe => DataType::UInt64,
            FloatLe | FloatBe if self.length == Some(4) => DataType::Float32,
            FloatLe | FloatBe | HfloatLe | HfloatBe => DataType::Float64,
            Filler => DataType::Null,
        }
    }
    pub fn null(&self) -> ScalarValue {
        ScalarValue::try_from(&self.data_type()).unwrap()
    }
    pub fn trim_text<'a>(&self, s: &'a str) -> &'a str {
        match self.trim.as_str() {
            "right" => s.trim_end_matches(self.pad),
            "both" => s.trim_matches(self.pad),
            _ => s,
        }
    }
    pub fn text_null(&self, s: &str) -> bool {
        self.spec
            .null_if
            .as_ref()
            .is_some_and(|vs| vs.iter().any(|v| v == s))
            || self.null_regex.as_ref().is_some_and(|r| r.is_match(s))
    }
    pub fn is_null_bytes(&self, b: &[u8]) -> Result<bool> {
        if self.null_bytes.as_deref() == Some(b) {
            return Ok(true);
        }
        if self.kind.text() && (self.spec.null_if.is_some() || self.null_regex.is_some()) {
            return Ok(self.text_null(self.trim_text(&self.encoding.decode_cow(b)?)));
        }
        Ok(false)
    }
    fn check_length(&self, b: &[u8]) -> Result<()> {
        if self.length.is_some_and(|n| b.len() != n)
            || self.spec.max_length.is_some_and(|n| b.len() > n)
        {
            return Err(Error::output("field byte width exceeds layout"));
        }
        Ok(())
    }
    pub fn padded(&self, s: &str) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.padded_into(s, &mut bytes)?;
        Ok(bytes)
    }
    fn padded_into(&self, s: &str, output: &mut Vec<u8>) -> Result<()> {
        output.clear();
        // UTF-8 is stateless: pad directly without repeatedly encoding candidate strings.
        if self.encoding.0 == encoding_rs::UTF_8 {
            let width = self.length.unwrap_or(s.len());
            if s.len() > width {
                return Err(Error::output("encoded string exceeds field width"));
            }
            let padding = width - s.len();
            let mut buf = [0; 4];
            let pad = self.pad.encode_utf8(&mut buf).as_bytes();
            if !padding.is_multiple_of(pad.len()) {
                return Err(Error::output("pad_char cannot fill exact byte width"));
            }
            output.reserve(width);
            if self.kind != FieldType::Decimal {
                output.extend_from_slice(s.as_bytes());
            }
            for _ in 0..padding / pad.len() {
                output.extend_from_slice(pad);
            }
            if self.kind == FieldType::Decimal {
                output.extend_from_slice(s.as_bytes());
            }
            return self.check_length(output);
        }
        *output = self.padded_encoded(s)?;
        Ok(())
    }
    fn padded_encoded(&self, s: &str) -> Result<Vec<u8>> {
        let bytes = self.encoding.encode(s)?;
        let Some(width) = self.length else {
            self.check_length(&bytes)?;
            return Ok(bytes);
        };
        if bytes.len() > width {
            return Err(Error::output("encoded string exceeds field width"));
        }
        if bytes.len() == width {
            return Ok(bytes);
        }
        let mut low = 0;
        let mut high = width + 1;
        while low < high {
            let mid = (low + high) / 2;
            let pad = self.pad.to_string().repeat(mid);
            let text = if self.kind == FieldType::Decimal {
                format!("{pad}{s}")
            } else {
                format!("{s}{pad}")
            };
            let bytes = self.encoding.encode(&text)?;
            match bytes.len().cmp(&width) {
                std::cmp::Ordering::Equal => return Ok(bytes),
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        Err(Error::output("pad_char cannot fill exact byte width"))
    }
    pub fn parse_text(&self, s: Option<&str>) -> Result<ScalarValue> {
        let Some(s) = s else {
            if self.spec.nullable != Some(true) {
                return Err(Error::input("NULL in non-nullable column"));
            }
            return Ok(self.null());
        };
        use FieldType::*;
        Ok(match self.kind {
            String => ScalarValue::Utf8(Some(s.into())),
            Decimal => ScalarValue::Decimal128(
                Some(parse_decimal(s, self.precision, self.scale)?),
                self.precision,
                self.scale as i8,
            ),
            Boolean => ScalarValue::Boolean(Some(if self.true_values.iter().any(|v| v == s) {
                true
            } else if self.false_values.iter().any(|v| v == s) {
                false
            } else {
                return Err(Error::input("invalid boolean representation"));
            })),
            Date | Time | Timestamp => return self.pattern.as_ref().unwrap().parse(s),
            Binary => ScalarValue::Binary(Some(
                if self.spec.binary_encoding.as_deref() == Some("base64") {
                    base64::engine::general_purpose::STANDARD
                        .decode(s)
                        .map_err(|_| Error::input("invalid base64"))?
                } else {
                    hex::decode(s).map_err(|_| Error::input("invalid binary hex"))?
                },
            )),
            _ => return Err(Error::input("invalid text storage type")),
        })
    }
    pub fn decode(&self, bytes: &[u8]) -> Result<Vec<ScalarValue>> {
        let mut values = Vec::new();
        self.decode_into(bytes, &mut values)?;
        Ok(values)
    }
    /// Append to a reusable row; bits may append more than one value.
    pub fn decode_into(&self, bytes: &[u8], values: &mut Vec<ScalarValue>) -> Result<()> {
        self.decode_to(bytes, values)
    }
    pub(crate) fn parse_text_to(
        &self,
        text: Option<&str>,
        values: &mut impl ValueSink,
    ) -> Result<()> {
        if text.is_none() && self.spec.nullable != Some(true) {
            return Err(Error::input("NULL in non-nullable column"));
        }
        match self.kind {
            FieldType::String => values.text(text),
            FieldType::Decimal => values.decimal(
                text.map(|s| parse_decimal(s, self.precision, self.scale))
                    .transpose()?,
                self.precision,
                self.scale as i8,
            ),
            _ => values.scalar(self.parse_text(text)?),
        }
    }
    pub(crate) fn decode_to(&self, bytes: &[u8], values: &mut impl ValueSink) -> Result<()> {
        use FieldType::*;
        if self.kind == Filler {
            return Ok(());
        }
        if self.spec.max_length.is_some_and(|n| bytes.len() > n) {
            return Err(Error::input("rest field exceeds max_length"));
        }
        if self.null_bytes.as_deref() == Some(bytes) {
            if self.spec.nullable != Some(true) {
                return Err(Error::input("NULL in non-nullable field"));
            }
            let count = self.spec.bits.as_ref().map_or(1, Vec::len);
            for _ in 0..count {
                match self.kind {
                    String => values.text(None)?,
                    Binary => values.binary(None)?,
                    Decimal | Zoned | Lzoned | Tzoned | Uzoned | Packed | Upacked | Npacked => {
                        values.decimal(None, self.precision, self.scale as i8)?
                    }
                    _ => values.scalar(self.null())?,
                }
            }
            return Ok(());
        }
        if self.kind.text() {
            let decoded = self.encoding.decode_cow(bytes)?;
            let text = self.trim_text(&decoded);
            return self.parse_text_to((!self.text_null(text)).then_some(text), values);
        }
        if self.kind == Bits {
            for b in self.spec.bits.as_ref().unwrap() {
                values.scalar(ScalarValue::Boolean(Some(
                    bytes[b.bit / 8] & self.mask(b.bit) != 0,
                )))?;
            }
            return Ok(());
        }
        if self.kind == Binary {
            return values.binary(Some(bytes));
        }
        if self.kind.decimal() {
            return values.decimal(
                Some(self.decode_digits(bytes)?),
                self.precision,
                self.scale as i8,
            );
        }
        let mut reversed = [0; 16];
        let b = if self.kind.little() {
            let b = &mut reversed[..bytes.len()];
            for (dest, src) in b.iter_mut().zip(bytes.iter().rev()) {
                *dest = *src;
            }
            &*b
        } else {
            bytes
        };
        let value = match self.kind {
            IntLe | IntBe => {
                let mut raw = if b[0] & 0x80 != 0 { [255; 8] } else { [0; 8] };
                raw[8 - b.len()..].copy_from_slice(b);
                ScalarValue::Int64(Some(i64::from_be_bytes(raw)))
            }
            UintLe | UintBe => {
                let mut raw = [0; 8];
                raw[8 - b.len()..].copy_from_slice(b);
                ScalarValue::UInt64(Some(u64::from_be_bytes(raw)))
            }
            FloatLe | FloatBe if b.len() == 4 => ScalarValue::Float32(Some(f32::from_bits(
                u32::from_be_bytes(b.try_into().unwrap()),
            ))),
            FloatLe | FloatBe => ScalarValue::Float64(Some(f64::from_bits(u64::from_be_bytes(
                b.try_into().unwrap(),
            )))),
            HfloatLe | HfloatBe => ScalarValue::Float64(Some(decode_hfp(b))),
            _ => unreachable!(),
        };
        values.scalar(value)
    }
    pub fn mask(&self, bit: usize) -> u8 {
        1 << if self.spec.bit_order.as_deref() == Some("lsb0") {
            bit % 8
        } else {
            7 - bit % 8
        }
    }
    fn decode_digits(&self, b: &[u8]) -> Result<i128> {
        use FieldType::*;
        let bad = || Error::input("invalid decimal digit, zone, sign or precision");
        // At most 19 unsigned decimal digits fit in u64. Validate the entire
        // zone/digit byte, then accumulate without wide checked arithmetic.
        if self.kind == Uzoned && b.len() <= 19 {
            let zero = if self.spec.zone.as_deref() == Some("ascii") {
                b'0'
            } else {
                0xf0
            };
            let mut value = 0u64;
            for byte in b {
                let digit = byte.wrapping_sub(zero);
                if digit > 9 {
                    return Err(bad());
                }
                value = value * 10 + u64::from(digit);
            }
            let value = i128::from(value);
            check_precision(value, self.precision).map_err(|_| bad())?;
            return Ok(value);
        }
        let mut value = 0i128;
        let mut digit = |d: u8| -> Result<()> {
            if d > 9 {
                return Err(bad());
            }
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(i128::from(d)))
                .ok_or_else(bad)?;
            Ok(())
        };
        let mut negative = false;
        if matches!(self.kind, Packed | Upacked | Npacked) {
            for (i, byte) in b.iter().enumerate() {
                digit(byte >> 4)?;
                if self.kind != Npacked && i == b.len() - 1 {
                    negative = match byte & 15 {
                        0xb | 0xd if self.kind == Packed => true,
                        0xa | 0xc | 0xe if self.kind == Packed => false,
                        0xf => false,
                        _ => return Err(bad()),
                    };
                } else {
                    digit(byte & 15)?;
                }
            }
        } else {
            let ascii = self.spec.zone.as_deref() == Some("ascii");
            let zone = if ascii { 3 } else { 15 };
            let mut start = 0;
            let mut end = b.len();
            if matches!(self.kind, Lzoned | Tzoned) {
                let s = if self.kind == Lzoned {
                    start = 1;
                    b[0]
                } else {
                    end -= 1;
                    b[end]
                };
                negative = if s == if ascii { 0x2d } else { 0x60 } {
                    true
                } else if s == if ascii { 0x2b } else { 0x4e } {
                    false
                } else {
                    return Err(bad());
                };
            }
            for (i, byte) in b.iter().enumerate().take(end).skip(start) {
                let z = byte >> 4;
                if self.kind == Zoned && i == end - 1 {
                    negative = if ascii {
                        match z {
                            3 => false,
                            7 => true,
                            _ => return Err(bad()),
                        }
                    } else {
                        match z {
                            0xa | 0xc | 0xe | 0xf => false,
                            0xb | 0xd => true,
                            _ => return Err(bad()),
                        }
                    };
                } else if z != zone {
                    return Err(bad());
                }
                digit(byte & 15)?;
            }
        }
        check_precision(value, self.precision).map_err(|_| bad())?;
        Ok(if negative { -value } else { value })
    }
    fn encode_digits(&self, value: i128) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.encode_digits_into(value, &mut bytes)?;
        Ok(bytes)
    }
    fn encode_digits_into(&self, value: i128, bytes: &mut Vec<u8>) -> Result<()> {
        use FieldType::*;
        check_precision(value, self.precision)?;
        let len = self.length.unwrap();
        let negative = value < 0;
        if negative && matches!(self.kind, Uzoned | Upacked | Npacked) {
            return Err(Error::output("negative value in unsigned decimal"));
        }
        bytes.clear();
        let mut magnitude = value.unsigned_abs();
        if matches!(self.kind, Packed | Upacked | Npacked) {
            bytes.resize(len, 0);
            let mut nibbles = len * 2;
            if self.kind != Npacked {
                nibbles -= 1;
                bytes[len - 1] = if self.kind == Upacked {
                    15
                } else if negative {
                    13
                } else {
                    12
                };
            }
            // The compiled precision fits the field; zero padding is already in place.
            while magnitude != 0 {
                nibbles -= 1;
                bytes[nibbles / 2] |= pop_decimal_digit(&mut magnitude)
                    << if nibbles.is_multiple_of(2) { 4 } else { 0 };
            }
        } else {
            let ascii = self.spec.zone.as_deref() == Some("ascii");
            let zone = if ascii { 0x30 } else { 0xf0 };
            bytes.resize(len, zone);
            let mut end = if self.kind == Tzoned { len - 1 } else { len };
            while magnitude != 0 {
                end -= 1;
                bytes[end] |= pop_decimal_digit(&mut magnitude);
            }
            if self.kind == Zoned {
                bytes[len - 1] = (bytes[len - 1] & 15)
                    | if ascii {
                        if negative { 0x70 } else { 0x30 }
                    } else if negative {
                        0xd0
                    } else {
                        0xc0
                    };
            }
            let sign = if ascii {
                if negative { 0x2d } else { 0x2b }
            } else if negative {
                0x60
            } else {
                0x4e
            };
            if self.kind == Lzoned {
                bytes[0] = sign;
            } else if self.kind == Tzoned {
                bytes[len - 1] = sign;
            }
        }
        Ok(())
    }
    pub fn boolean_text(&self, value: bool) -> String {
        if value {
            self.spec.true_output.clone().unwrap_or_else(|| {
                self.spec
                    .true_values
                    .as_ref()
                    .and_then(|v| v.first())
                    .cloned()
                    .unwrap_or_else(|| "true".into())
            })
        } else {
            self.spec.false_output.clone().unwrap_or_else(|| {
                self.spec
                    .false_values
                    .as_ref()
                    .and_then(|v| v.first())
                    .cloned()
                    .unwrap_or_else(|| "false".into())
            })
        }
    }
    pub fn format_text(&self, value: &ScalarValue) -> Result<String> {
        Ok(match value {
            ScalarValue::Utf8(Some(s))
            | ScalarValue::Utf8View(Some(s))
            | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
            ScalarValue::Boolean(Some(v)) => self.boolean_text(*v),
            ScalarValue::Decimal128(Some(v), _, s) => {
                let v = rescale(*v, *s as i32, self.scale as i32)?;
                check_precision(v, self.precision)?;
                decimal_text(v, self.scale)
            }
            ScalarValue::Binary(Some(b))
            | ScalarValue::BinaryView(Some(b))
            | ScalarValue::LargeBinary(Some(b))
            | ScalarValue::FixedSizeBinary(_, Some(b)) => {
                if self.spec.binary_encoding.as_deref() == Some("base64") {
                    base64::engine::general_purpose::STANDARD.encode(b)
                } else {
                    hex::encode(b)
                }
            }
            _ if self.pattern.is_some() => return self.pattern.as_ref().unwrap().format(value),
            ScalarValue::Float32(Some(v)) => float_text(f64::from(*v), Some(*v)),
            ScalarValue::Float64(Some(v)) => float_text(*v, None),
            _ => value.to_string(),
        })
    }
    pub fn fixed_null(&self) -> Result<Vec<u8>> {
        use FieldType::*;
        match self.kind {
            String | Boolean | Date | Time | Timestamp => self.padded(""),
            Decimal => self.padded(&decimal_text(0, self.scale)),
            Zoned | Lzoned | Tzoned | Uzoned | Packed | Upacked | Npacked => self.encode_digits(0),
            _ => Ok(vec![0; self.length.unwrap()]),
        }
    }
    pub fn encode(&self, values: &[ScalarValue], format: Format) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.encode_into(values, format, &mut bytes)?;
        Ok(bytes)
    }
    /// Encode into reusable field storage, retaining all output validation.
    pub fn encode_into(
        &self,
        values: &[ScalarValue],
        format: Format,
        bytes: &mut Vec<u8>,
    ) -> Result<()> {
        use FieldType::*;
        bytes.clear();
        if self.kind == Filler {
            bytes.resize(self.length.unwrap(), self.spec.fill_byte.unwrap_or(32));
            return Ok(());
        }
        let nulls = values.iter().filter(|v| v.is_null()).count();
        if self.kind == Bits {
            if nulls > 0 && format == Format::Rdw {
                if nulls != values.len() {
                    return Err(Error::output(
                        "mixed NULL bits cannot be represented in RDW",
                    ));
                }
                let null = self
                    .null_output
                    .as_ref()
                    .ok_or_else(|| Error::output("NULL in non-nullable RDW bits"));
                bytes.extend_from_slice(null?);
                return Ok(());
            }
            bytes.resize(self.length.unwrap(), 0);
            for (value, bit) in values.iter().zip(self.spec.bits.as_ref().unwrap()) {
                match value {
                    ScalarValue::Boolean(Some(true)) => bytes[bit.bit / 8] |= self.mask(bit.bit),
                    ScalarValue::Boolean(_) => (),
                    _ => return Err(Error::output("bits require Boolean columns")),
                }
            }
            if nulls == 0 && self.is_null_bytes(bytes)? {
                return Err(Error::output("non-NULL bits collide with NULL marker"));
            }
            return Ok(());
        }
        if nulls > 0 {
            if format == Format::Fixed {
                *bytes = self.fixed_null()?;
            } else {
                bytes.extend_from_slice(
                    self.null_output
                        .as_ref()
                        .ok_or_else(|| Error::output("NULL in non-nullable RDW column"))?,
                );
            }
            return Ok(());
        }
        let value = &values[0];
        if self.kind.text() {
            match value {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => self.padded_into(s, bytes)?,
                _ => self.padded_into(&self.format_text(value)?, bytes)?,
            }
        } else if matches!(
            self.kind,
            Zoned | Lzoned | Tzoned | Uzoned | Packed | Upacked | Npacked
        ) {
            let ScalarValue::Decimal128(Some(v), _, s) = value else {
                return Err(Error::output("decimal storage requires SQL DECIMAL"));
            };
            return self.encode_decimal_into(*v, *s, bytes);
        } else {
            *bytes = match self.kind {
                Binary => match value {
                    ScalarValue::Binary(Some(b))
                    | ScalarValue::BinaryView(Some(b))
                    | ScalarValue::LargeBinary(Some(b))
                    | ScalarValue::FixedSizeBinary(_, Some(b)) => b.clone(),
                    _ => return Err(Error::output("binary requires a binary SQL value")),
                },
                IntLe | IntBe => {
                    let v = signed(value)?;
                    let bits = self.length.unwrap() * 8;
                    if bits < 64 && (v < -(1i64 << (bits - 1)) || v >= (1i64 << (bits - 1))) {
                        return Err(Error::output("signed integer overflow"));
                    }
                    v.to_be_bytes()[8 - self.length.unwrap()..].to_vec()
                }
                UintLe | UintBe => {
                    let v = unsigned(value)?;
                    let bits = self.length.unwrap() * 8;
                    if bits < 64 && v >= (1u64 << bits) {
                        return Err(Error::output("unsigned integer overflow"));
                    }
                    v.to_be_bytes()[8 - self.length.unwrap()..].to_vec()
                }
                FloatLe | FloatBe | HfloatLe | HfloatBe => {
                    let v = match value {
                        ScalarValue::Float32(Some(v)) => f64::from(*v),
                        ScalarValue::Float64(Some(v)) => *v,
                        _ => return Err(Error::output("floating storage requires SQL float")),
                    };
                    if matches!(self.kind, HfloatLe | HfloatBe) {
                        encode_hfp(v, self.length.unwrap())?
                    } else if self.length == Some(4) {
                        let f = v as f32;
                        if v.is_finite() && (!f.is_finite() || v != 0.0 && f == 0.0) {
                            return Err(Error::output("IEEE float overflow or underflow"));
                        }
                        f.to_bits().to_be_bytes().to_vec()
                    } else {
                        v.to_bits().to_be_bytes().to_vec()
                    }
                }
                _ => unreachable!(),
            };
        }
        if self.kind.little() {
            bytes.reverse();
        }
        self.check_length(bytes)?;
        if self.is_null_bytes(bytes)? {
            return Err(Error::output("non-NULL value collides with NULL rule"));
        }
        Ok(())
    }
    pub fn encode_string_into(&self, value: &str, bytes: &mut Vec<u8>) -> Result<()> {
        self.padded_into(value, bytes)?;
        if self.is_null_bytes(bytes)? {
            return Err(Error::output("non-NULL value collides with NULL rule"));
        }
        Ok(())
    }
    /// Non-NULL zoned/packed Decimal128 values can be taken straight from Arrow.
    pub(crate) fn encode_decimal_into(
        &self,
        value: i128,
        scale: i8,
        bytes: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_digits_into(
            rescale(value, i32::from(scale), i32::from(self.scale))?,
            bytes,
        )?;
        self.check_length(bytes)?;
        if self.is_null_bytes(bytes)? {
            return Err(Error::output("non-NULL value collides with NULL rule"));
        }
        Ok(())
    }
}
pub fn check_precision(v: i128, p: u8) -> Result<()> {
    if DECIMAL_POWERS
        .get(usize::from(p))
        .is_none_or(|limit| v.unsigned_abs() >= *limit)
    {
        Err(Error::output("decimal precision overflow"))
    } else {
        Ok(())
    }
}
pub fn rescale(v: i128, from: i32, to: i32) -> Result<i128> {
    if from == to {
        return Ok(v);
    }
    let overflow = || Error::output("decimal overflow or scale would lose nonzero digits");
    let delta = to - from;
    if delta.unsigned_abs() > 38 {
        if v == 0 {
            return Ok(0);
        }
        return Err(overflow());
    }
    let power = DECIMAL_POWERS[delta.unsigned_abs() as usize] as i128;
    if delta >= 0 {
        v.checked_mul(power).ok_or_else(overflow)
    } else if v % power == 0 {
        Ok(v / power)
    } else {
        Err(overflow())
    }
}
pub fn parse_decimal(s: &str, p: u8, scale: u8) -> Result<i128> {
    let bad =
        || Error::input("invalid decimal, precision overflow or nonzero excess fractional digits");
    let neg = s.starts_with('-');
    let s = s.strip_prefix(['-', '+']).unwrap_or(s);
    let mut parts = s.split('.');
    let integer = parts.next().unwrap();
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|c| c.is_ascii_digit())
        || !fraction.bytes().all(|c| c.is_ascii_digit())
    {
        return Err(bad());
    }
    let excess = fraction.get(scale as usize..).unwrap_or("");
    if !excess.bytes().all(|c| c == b'0') {
        return Err(bad());
    }
    let fraction = &fraction[..fraction.len().min(scale as usize)];
    let mut value = 0i128;
    for c in integer
        .bytes()
        .chain(fraction.bytes())
        .chain(std::iter::repeat_n(b'0', scale as usize - fraction.len()))
    {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i128::from(c - b'0')))
            .ok_or_else(bad)?;
    }
    check_precision(value, p).map_err(|_| bad())?;
    Ok(if neg { -value } else { value })
}
pub fn decimal_text(v: i128, scale: u8) -> String {
    let mut out = String::new();
    decimal_text_into(v, scale, &mut out);
    out
}
pub(crate) fn decimal_text_into(v: i128, scale: u8, out: &mut String) {
    // The public formatter accepts any u8 scale; SQL Decimal128 uses at most 38.
    let mut digits = [b'0'; 256];
    let mut start = digits.len();
    let mut n = v.unsigned_abs();
    loop {
        start -= 1;
        digits[start] = b'0' + pop_decimal_digit(&mut n);
        if n == 0 {
            break;
        }
    }
    start = start.min(digits.len() - usize::from(scale) - 1);
    let digits = std::str::from_utf8(&digits[start..]).expect("ASCII digits");
    if v < 0 {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(digits);
    } else {
        let i = digits.len() - scale as usize;
        out.push_str(&digits[..i]);
        out.push('.');
        out.push_str(&digits[i..]);
    }
}
fn signed(v: &ScalarValue) -> Result<i64> {
    match v {
        ScalarValue::Int8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => {
            i64::try_from(*v).map_err(|_| Error::output("integer overflow"))
        }
        _ => Err(Error::output("integer requires SQL integer")),
    }
}
fn unsigned(v: &ScalarValue) -> Result<u64> {
    if let ScalarValue::UInt64(Some(v)) = v {
        Ok(*v)
    } else {
        u64::try_from(signed(v)?).map_err(|_| Error::output("negative unsigned integer"))
    }
}
fn float_text(v: f64, f: Option<f32>) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "Infinity".into()
    } else if v == f64::NEG_INFINITY {
        "-Infinity".into()
    } else if let Some(f) = f {
        f.to_string()
    } else {
        v.to_string()
    }
}
pub fn decode_hfp(b: &[u8]) -> f64 {
    let mut fraction = 0u128;
    for (i, byte) in b.iter().enumerate().skip(1) {
        if b.len() == 16 && i == 8 {
            continue;
        }
        fraction = (fraction << 8) | u128::from(*byte);
    }
    let bits = if b.len() == 16 {
        112
    } else {
        (b.len() - 1) * 8
    };
    let exponent = i32::from(b[0] & 127) - 64;
    let value = (fraction as f64) * 2f64.powi(exponent * 4 - bits as i32);
    if b[0] & 128 != 0 { -value } else { value }
}
pub fn encode_hfp(value: f64, len: usize) -> Result<Vec<u8>> {
    if !value.is_finite() {
        return Err(Error::output("HFP cannot represent NaN or infinity"));
    }
    if value == 0.0 {
        return Ok(vec![0; len]);
    }
    let mut fraction = value.abs();
    let mut characteristic: i32 = 64;
    while fraction >= 1.0 {
        fraction /= 16.0;
        characteristic += 1;
    }
    while fraction < 0.0625 {
        fraction *= 16.0;
        characteristic -= 1;
    }
    if !(0..=127).contains(&characteristic) {
        return Err(Error::output("HFP overflow or underflow"));
    }
    let bits = if len == 16 { 112 } else { (len - 1) * 8 };
    let mut integer = (fraction * 2f64.powi(bits as i32)).round_ties_even() as u128;
    if integer >= 1u128 << bits {
        integer >>= 4;
        characteristic += 1;
        if characteristic > 127 {
            return Err(Error::output("HFP overflow"));
        }
    }
    if integer == 0 {
        return Err(Error::output("HFP underflow"));
    }
    let sign = if value.is_sign_negative() { 128 } else { 0 };
    let mut out = vec![0; len];
    out[0] = sign | characteristic as u8;
    for i in (1..len).rev() {
        if len == 16 && i == 8 {
            continue;
        }
        out[i] = integer as u8;
        integer >>= 8;
    }
    if len == 16 {
        out[8] = sign | ((characteristic - 14).rem_euclid(128) as u8);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_format_preserves_extreme_values_and_scales() {
        for value in [i128::MIN, i128::MAX, -123456789, -1, 0, 1, 123456789] {
            for scale in [0, 1, 2, 18, 38, 255] {
                let digits = format!(
                    "{:0width$}",
                    value.unsigned_abs(),
                    width = scale as usize + 1
                );
                let sign = if value < 0 { "-" } else { "" };
                let expected = if scale == 0 {
                    format!("{sign}{digits}")
                } else {
                    let split = digits.len() - scale as usize;
                    format!("{sign}{}.{}", &digits[..split], &digits[split..])
                };
                assert_eq!(decimal_text(value, scale), expected);
            }
        }
    }
    fn field(t: &str, n: usize) -> CompiledField {
        CompiledField::compile(
            Field {
                name: Some("v".into()),
                kind: Some(t.into()),
                length: Some(Length::Fixed(n)),
                ..Default::default()
            },
            Format::Fixed,
            true,
            0,
            TextEncoding::resolve("utf-8").unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn known_numeric_bytes() {
        for (t, b, v) in [
            ("int-be", vec![255, 254], ScalarValue::Int64(Some(-2))),
            ("int-le", vec![254, 255], ScalarValue::Int64(Some(-2))),
            ("uint-be", vec![1, 2], ScalarValue::UInt64(Some(258))),
            (
                "float-be",
                vec![63, 128, 0, 0],
                ScalarValue::Float32(Some(1.0)),
            ),
            (
                "hfloat-be",
                vec![65, 16, 0, 0],
                ScalarValue::Float64(Some(1.0)),
            ),
            (
                "hfloat-le",
                vec![0, 0, 16, 65],
                ScalarValue::Float64(Some(1.0)),
            ),
        ] {
            let f = field(t, b.len());
            assert_eq!(f.decode(&b).unwrap(), vec![v.clone()]);
            assert_eq!(f.encode(&[v], Format::Fixed).unwrap(), b);
        }
    }
    #[test]
    fn packed_bytes() {
        for (t, b, n) in [
            ("packed", vec![0x12, 0x3c], 123),
            ("packed", vec![0x12, 0x3d], -123),
            ("upacked", vec![0x12, 0x3f], 123),
            ("npacked", vec![0x01, 0x23], 123),
        ] {
            let f = field(t, 2);
            assert_eq!(
                f.decode(&b).unwrap()[0],
                ScalarValue::Decimal128(Some(n), f.precision, 0)
            );
            assert_eq!(
                f.encode(
                    &[ScalarValue::Decimal128(Some(n), f.precision, 0)],
                    Format::Fixed
                )
                .unwrap(),
                b
            );
        }
    }
    #[test]
    fn fixed_null_defaults() {
        for (t, b) in [
            ("zoned", vec![0xf0, 0xf0, 0xc0]),
            ("lzoned", vec![0x4e, 0xf0, 0xf0]),
            ("tzoned", vec![0xf0, 0xf0, 0x4e]),
            ("uzoned", vec![0xf0, 0xf0, 0xf0]),
            ("packed", vec![0, 0x0c]),
            ("upacked", vec![0, 0x0f]),
            ("npacked", vec![0, 0]),
        ] {
            let f = field(t, b.len());
            assert_eq!(f.encode(&[f.null()], Format::Fixed).unwrap(), b);
        }
    }
    #[test]
    fn no_silent_numeric_loss() {
        assert!(
            field("int-be", 1)
                .encode(&[ScalarValue::Int64(Some(128))], Format::Fixed)
                .is_err()
        );
        assert!(parse_decimal("1.234", 5, 2).is_err());
        assert_eq!(parse_decimal("1.230", 5, 2).unwrap(), 123);
        assert!(field("packed", 2).decode(&[0x1a, 0x3c]).is_err());
    }
    #[test]
    fn hfp_extended() {
        let expected = [0x41, 0x10, 0, 0, 0, 0, 0, 0, 0x33, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(encode_hfp(1.0, 16).unwrap(), expected);
        assert_eq!(decode_hfp(&expected), 1.0);
        assert!(encode_hfp(f64::INFINITY, 4).is_err());
    }
}
