mod compile;
pub mod datetime;
mod decimal;
mod float;
pub(crate) use decimal::decimal_text_into;
pub use decimal::{check_precision, decimal_text, parse_decimal, rescale};
pub use float::{decode_hfp, encode_hfp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Input,
    Output,
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Trim {
    None,
    Right,
    Both,
}

use crate::{
    config::{Field, Format},
    encoding::TextEncoding,
    error::{Error, Result},
};
use base64::Engine;
use datafusion::{arrow::datatypes::DataType, common::ScalarValue};
use regex::Regex;

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
    pub(crate) spec: Field,
    pub(crate) kind: FieldType,
    pub(crate) offset: usize,
    pub(crate) length: Option<usize>,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    pub(crate) encoding: TextEncoding,
    pub(crate) pad: char,
    pub(crate) trim: Trim,
    pub(crate) null_bytes: Option<Vec<u8>>,
    pub(crate) null_regex: Option<Regex>,
    pub(crate) pattern: Option<datetime::Pattern>,
    pub(crate) true_values: Vec<String>,
    pub(crate) false_values: Vec<String>,
    pub(crate) null_output: Option<Vec<u8>>,
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
    pub fn kind(&self) -> FieldType {
        self.kind
    }
    pub fn precision(&self) -> u8 {
        self.precision
    }
    pub fn offset(&self) -> usize {
        self.offset
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
        match self.trim {
            Trim::Right => s.trim_end_matches(self.pad),
            Trim::Both => s.trim_matches(self.pad),
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
        if self.encoding.is_utf8() {
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Length;
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
            crate::field::Direction::Output,
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
