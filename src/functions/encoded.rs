//! Encoding-aware string operations with byte-based lengths and Binary results.
use super::arguments::{Arguments, Cache};
use crate::{
    encoding::{TextEncoding, encoded_space, pad_encoded, trim_encoded},
    framing::MAX_RECORD,
};
use datafusion::{
    arrow::{array::new_empty_array, datatypes::DataType},
    common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use std::borrow::Cow;

pub(super) fn register(context: &SessionContext) {
    use Kind::*;
    for kind in [
        Length, BitLength, Substr, Left, Right, Trim, Ltrim, Rtrim, Lpad, Rpad, Upper, Lower,
        Replace, StartsWith, EndsWith, Contains, Strpos, Repeat,
    ] {
        context.register_udf(ScalarUDF::from(EncodedFunction {
            kind,
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: match kind {
                Length => vec!["icu_octet_length".into()],
                Substr => vec!["icu_substring".into()],
                Trim => vec!["icu_btrim".into()],
                Strpos => vec!["icu_instr".into(), "icu_position".into()],
                _ => vec![],
            },
        }));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Length,
    BitLength,
    Substr,
    Left,
    Right,
    Trim,
    Ltrim,
    Rtrim,
    Lpad,
    Rpad,
    Upper,
    Lower,
    Replace,
    StartsWith,
    EndsWith,
    Contains,
    Strpos,
    Repeat,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Length => "icu_length",
            Self::BitLength => "icu_bit_length",
            Self::Substr => "icu_substr",
            Self::Left => "icu_left",
            Self::Right => "icu_right",
            Self::Trim => "icu_trim",
            Self::Ltrim => "icu_ltrim",
            Self::Rtrim => "icu_rtrim",
            Self::Lpad => "icu_lpad",
            Self::Rpad => "icu_rpad",
            Self::Upper => "icu_upper",
            Self::Lower => "icu_lower",
            Self::Replace => "icu_replace",
            Self::StartsWith => "icu_starts_with",
            Self::EndsWith => "icu_ends_with",
            Self::Contains => "icu_contains",
            Self::Strpos => "icu_strpos",
            Self::Repeat => "icu_repeat",
        }
    }

    fn data_type(self) -> DataType {
        match self {
            Self::Length | Self::BitLength | Self::Strpos => DataType::Int64,
            Self::StartsWith | Self::EndsWith | Self::Contains => DataType::Boolean,
            _ => DataType::Binary,
        }
    }

    fn arity(self) -> std::ops::RangeInclusive<usize> {
        match self {
            Self::Length | Self::BitLength | Self::Upper | Self::Lower => 2..=2,
            Self::Trim | Self::Ltrim | Self::Rtrim => 2..=3,
            Self::Substr | Self::Lpad | Self::Rpad => 3..=4,
            Self::Replace => 4..=4,
            _ => 3..=3,
        }
    }

    fn integer_argument(self, index: usize) -> bool {
        match self {
            Self::Substr => index == 2 || index == 3,
            Self::Left | Self::Right | Self::Lpad | Self::Rpad | Self::Repeat => index == 2,
            _ => false,
        }
    }

    fn uses_default_space(self, arity: usize) -> bool {
        matches!(self, Self::Trim | Self::Ltrim | Self::Rtrim) && arity == 2
            || matches!(self, Self::Lpad | Self::Rpad) && arity == 3
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct EncodedFunction {
    kind: Kind,
    signature: Signature,
    aliases: Vec<String>,
}

fn bytes_type(t: &DataType) -> bool {
    super::charset::binary_type(t) || super::string_type(t)
}

struct Encoding {
    charset: TextEncoding,
    space: Vec<u8>,
}

impl ScalarUDFImpl for EncodedFunction {
    fn name(&self) -> &str {
        self.kind.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if !self.kind.arity().contains(&types.len()) {
            return Err(plan_datafusion_err!(
                "{} requires value, encoding and the function's arguments",
                self.name()
            ));
        }
        for (index, t) in types.iter().enumerate() {
            let valid = if index == 1 {
                super::string_type(t)
            } else if self.kind.integer_argument(index) {
                t.is_integer() || *t == DataType::Null
            } else {
                bytes_type(t)
            };
            if !valid {
                return Err(plan_datafusion_err!(
                    "{} argument {} has unsupported type {t}",
                    self.name(),
                    index + 1
                ));
            }
        }
        Ok(self.kind.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result_type = self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let arguments = Arguments::new(&args.args)?;
        let rows = arguments.rows();
        let mut encodings = Cache::default();
        let mut values = Vec::with_capacity(rows);
        let null = ScalarValue::try_from(&result_type)?;
        for row in 0..rows {
            if arguments.is_null(row) {
                values.push(null.clone());
                continue;
            }
            let name = arguments.text(1, row)?;
            let encoding = encodings.get(name, |name| {
                let charset = TextEncoding::resolve(name)
                    .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?;
                let space = if self.kind.uses_default_space(arguments.len()) {
                    encoded_space(charset, " ")
                        .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?
                } else {
                    Vec::new()
                };
                Ok(Encoding { charset, space })
            })?;
            let row = Row {
                arguments: &arguments,
                index: row,
                encoding,
            };
            values.push(
                row.evaluate(self.kind)
                    .map_err(|e| exec_datafusion_err!("{}: {e}", self.name()))?,
            );
        }
        let array = if rows == 0 {
            new_empty_array(&result_type)
        } else {
            ScalarValue::iter_to_array(values)?
        };
        arguments.finish(array)
    }
}

struct Row<'a, 'b> {
    arguments: &'b Arguments<'a>,
    index: usize,
    encoding: &'b Encoding,
}

impl Row<'_, '_> {
    fn bytes(&self, index: usize) -> Result<Cow<'_, [u8]>> {
        let bytes = self
            .arguments
            .bytes(index, self.index, self.encoding.charset)?;
        check_size(bytes.len())?;
        Ok(bytes)
    }

    fn integer(&self, index: usize) -> Result<i128> {
        self.arguments.integer(index, self.index)
    }

    fn pattern(&self, index: usize) -> Result<Cow<'_, [u8]>> {
        let bytes = if self.arguments.len() > index {
            self.bytes(index)?
        } else {
            Cow::Borrowed(self.encoding.space.as_slice())
        };
        if bytes.is_empty() {
            return Err(exec_datafusion_err!(
                "trim/padding pattern must not be empty"
            ));
        }
        Ok(bytes)
    }

    fn evaluate(&self, kind: Kind) -> Result<ScalarValue> {
        use Kind::*;
        let bytes = self.bytes(0)?;
        let output = match kind {
            Length => return Ok(ScalarValue::Int64(Some(bytes.len() as i64))),
            BitLength => return Ok(ScalarValue::Int64(Some(bytes.len() as i64 * 8))),
            Substr => {
                // PostgreSQL/DataFusion substr rules, applied to bytes. A
                // position before 1 reduces the requested count accordingly.
                let start = self.integer(2)? - 1;
                let end = if self.arguments.len() == 4 {
                    let count = self.integer(3)?;
                    if count < 0 {
                        return Err(exec_datafusion_err!(
                            "substring length must not be negative"
                        ));
                    }
                    start + count
                } else {
                    bytes.len() as i128
                };
                let limit = bytes.len() as i128;
                bytes[start.clamp(0, limit) as usize..end.clamp(0, limit) as usize].to_vec()
            }
            Left | Right => {
                let count = self.integer(2)?;
                let len = bytes.len() as i128;
                let n = if count >= 0 {
                    count.min(len)
                } else {
                    (len + count).max(0)
                } as usize;
                if kind == Left {
                    bytes[..n].to_vec()
                } else {
                    bytes[bytes.len() - n..].to_vec()
                }
            }
            Trim | Ltrim | Rtrim => {
                let pattern = self.pattern(2)?;
                trim_encoded(
                    &bytes,
                    &pattern,
                    self.encoding.charset,
                    kind != Rtrim,
                    kind != Ltrim,
                )
                .map_err(|e| exec_datafusion_err!("{e}"))?
            }
            Lpad | Rpad => {
                let width = usize::try_from(self.integer(2)?)
                    .ok()
                    .filter(|n| *n <= MAX_RECORD)
                    .ok_or_else(|| {
                        exec_datafusion_err!("padding byte length must be between 0 and 16 MiB")
                    })?;
                let pattern = self.pattern(3)?;
                pad_encoded(&bytes, width, &pattern, self.encoding.charset, kind == Lpad)
                    .map_err(|e| exec_datafusion_err!("{e}"))?
            }
            Upper | Lower => {
                let text = self
                    .encoding
                    .charset
                    .decode(&bytes)
                    .map_err(|e| exec_datafusion_err!("{e}"))?;
                let mapped = if kind == Upper {
                    text.to_uppercase()
                } else {
                    text.to_lowercase()
                };
                self.encoding
                    .charset
                    .encode(&mapped)
                    .map_err(|e| exec_datafusion_err!("{e}"))?
            }
            Replace => {
                let from = self.bytes(2)?;
                let to = self.bytes(3)?;
                replace(&bytes, &from, &to)?
            }
            StartsWith | EndsWith | Contains | Strpos => {
                let pattern = self.bytes(2)?;
                return Ok(match kind {
                    StartsWith => ScalarValue::Boolean(Some(bytes.starts_with(&pattern))),
                    EndsWith => ScalarValue::Boolean(Some(bytes.ends_with(&pattern))),
                    Contains => {
                        ScalarValue::Boolean(Some(memchr::memmem::find(&bytes, &pattern).is_some()))
                    }
                    _ => ScalarValue::Int64(Some(
                        memchr::memmem::find(&bytes, &pattern).map_or(0, |n| n as i64 + 1),
                    )),
                });
            }
            Repeat => {
                let count = self.integer(2)?.max(0);
                if bytes.is_empty() {
                    Vec::new()
                } else {
                    let count = usize::try_from(count)
                        .map_err(|_| exec_datafusion_err!("repeat result exceeds 16 MiB"))?;
                    let size = bytes
                        .len()
                        .checked_mul(count)
                        .ok_or_else(|| exec_datafusion_err!("repeat result exceeds 16 MiB"))?;
                    check_size(size)?;
                    bytes.repeat(count)
                }
            }
        };
        check_size(output.len())?;
        Ok(ScalarValue::Binary(Some(output)))
    }
}

fn check_size(size: usize) -> Result<()> {
    if size > MAX_RECORD {
        return Err(exec_datafusion_err!(
            "encoded string exceeds 16 MiB byte limit"
        ));
    }
    Ok(())
}

fn replace(bytes: &[u8], from: &[u8], to: &[u8]) -> Result<Vec<u8>> {
    if from.is_empty() {
        return Ok(bytes.to_vec());
    }
    let count = memchr::memmem::find_iter(bytes, from).count();
    let size = bytes.len() - count * from.len();
    let size = to
        .len()
        .checked_mul(count)
        .and_then(|n| size.checked_add(n))
        .ok_or_else(|| exec_datafusion_err!("replacement result exceeds 16 MiB"))?;
    check_size(size)?;
    let mut output = Vec::with_capacity(size);
    let mut offset = 0;
    for position in memchr::memmem::find_iter(bytes, from) {
        output.extend_from_slice(&bytes[offset..position]);
        output.extend_from_slice(to);
        offset = position + from.len();
    }
    output.extend_from_slice(&bytes[offset..]);
    Ok(output)
}
