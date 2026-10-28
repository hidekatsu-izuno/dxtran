//! Fixed-point numeric PICTURE editing, following a COBOL numeric-edited MOVE.
use datafusion::{
    arrow::{array::StringBuilder, datatypes::DataType},
    common::{Result, ScalarValue, exec_datafusion_err, plan_datafusion_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    },
    prelude::SessionContext,
};
use std::collections::{HashMap, hash_map::Entry};

pub(super) fn register(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(CobolToChar {
        signature: Signature::any(2, Volatility::Immutable),
    }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CobolToChar {
    signature: Signature,
}

impl ScalarUDFImpl for CobolToChar {
    fn name(&self) -> &str {
        "cobol_to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if types.len() != 2 {
            return Err(plan_datafusion_err!(
                "cobol_to_char requires a numeric value and a PICTURE string"
            ));
        }
        if !matches!(
            types[0],
            DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        ) || !super::string_type(&types[1])
        {
            return Err(plan_datafusion_err!(
                "cobol_to_char requires a numeric value and a PICTURE string, got {} and {}",
                types[0],
                types[1]
            ));
        }
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut output = StringBuilder::new();
        let mut pictures = HashMap::new();
        for row in 0..arrays[0].len() {
            if arrays
                .iter()
                .any(|a| a.data_type() == &DataType::Null || a.is_null(row))
            {
                output.append_null();
                continue;
            }
            let pattern = match ScalarValue::try_from_array(&arrays[1], row)? {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => s,
                _ => {
                    return Err(exec_datafusion_err!(
                        "cobol_to_char requires a PICTURE string"
                    ));
                }
            };
            // Dynamic formats must not grow the batch-local cache without bound.
            if pictures.len() == 128 && !pictures.contains_key(&pattern) {
                pictures.clear();
            }
            let picture = match pictures.entry(pattern) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let picture = Picture::compile(entry.key())?;
                    entry.insert(picture)
                }
            };
            let number = Number::from_scalar(ScalarValue::try_from_array(&arrays[0], row)?)?;
            output.append_value(picture.format(&number));
        }
        let output = output.finish();
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(std::sync::Arc::new(output)))
        }
    }
}

/// Decimal coefficient and scale; no floating-point arithmetic in the editor.
struct Number {
    digits: Vec<u8>,
    scale: i32,
    negative: bool,
}

impl Number {
    fn from_scalar(value: ScalarValue) -> Result<Self> {
        let (text, scale) = match value {
            ScalarValue::Int8(Some(n)) => (n.to_string(), 0),
            ScalarValue::Int16(Some(n)) => (n.to_string(), 0),
            ScalarValue::Int32(Some(n)) => (n.to_string(), 0),
            ScalarValue::Int64(Some(n)) => (n.to_string(), 0),
            ScalarValue::UInt8(Some(n)) => (n.to_string(), 0),
            ScalarValue::UInt16(Some(n)) => (n.to_string(), 0),
            ScalarValue::UInt32(Some(n)) => (n.to_string(), 0),
            ScalarValue::UInt64(Some(n)) => (n.to_string(), 0),
            ScalarValue::Decimal32(Some(n), _, s) => (n.to_string(), i32::from(s)),
            ScalarValue::Decimal64(Some(n), _, s) => (n.to_string(), i32::from(s)),
            ScalarValue::Decimal128(Some(n), _, s) => (n.to_string(), i32::from(s)),
            ScalarValue::Decimal256(Some(n), _, s) => (n.to_string(), i32::from(s)),
            // Use the shortest round-trip decimal of a float, rather than
            // multiplying it by powers of ten (which can lose another digit).
            ScalarValue::Float32(Some(n)) if n.is_finite() => (n.to_string(), 0),
            ScalarValue::Float64(Some(n)) if n.is_finite() => (n.to_string(), 0),
            _ => {
                return Err(exec_datafusion_err!(
                    "cobol_to_char requires a finite numeric value"
                ));
            }
        };
        Ok(Self::decimal(&text, scale))
    }

    fn decimal(text: &str, scale: i32) -> Self {
        // This input is produced by Rust's numeric Display implementations.
        let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
        let fraction = mantissa.split_once('.').map_or(0, |(_, s)| s.len() as i32);
        let digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
        let negative = text.starts_with('-') && digits.iter().any(|d| *d != b'0');
        Self {
            digits,
            scale: scale + fraction - exponent.parse::<i32>().unwrap(),
            negative,
        }
    }

    /// Align the implied decimal points, truncating excess digits on either
    /// side, as a MOVE into a numeric-edited receiving item does.
    fn aligned(&self, count: usize, scale: usize) -> Vec<u8> {
        (0..count)
            .map(|index| {
                let from_right = (count - index - 1) as i32 + self.scale - scale as i32;
                if from_right >= 0 && (from_right as usize) < self.digits.len() {
                    self.digits[self.digits.len() - from_right as usize - 1]
                } else {
                    b'0'
                }
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Symbol {
    Nine,
    Zero,
    Star,
    Decimal,
    Implied,
    Insert(u8),
    Plus,
    Minus,
    Currency,
    Credit,
    Debit,
}

impl Symbol {
    fn digit(self) -> bool {
        matches!(self, Self::Nine | Self::Zero | Self::Star)
    }

    fn repeatable(self) -> bool {
        !matches!(
            self,
            Self::Decimal | Self::Implied | Self::Credit | Self::Debit
        )
    }

    fn width(self) -> usize {
        match self {
            Self::Implied => 0,
            Self::Credit | Self::Debit => 2,
            _ => 1,
        }
    }

    fn sign(self, negative: bool) -> &'static [u8] {
        match (self, negative) {
            (Self::Plus, false) => b"+",
            (Self::Plus | Self::Minus, true) => b"-",
            (Self::Credit, true) => b"CR",
            (Self::Debit, true) => b"DB",
            (Self::Credit | Self::Debit, false) => b"  ",
            (Self::Currency, _) => b"$",
            _ => b" ",
        }
    }
}

struct Picture {
    symbols: Vec<Symbol>,
    floating: Option<Symbol>,
    replacement: Option<u8>,
    all_suppressed: bool,
    digits: usize,
    scale: usize,
    width: usize,
}

fn invalid(message: &str) -> datafusion::error::DataFusionError {
    exec_datafusion_err!("cobol_to_char: invalid PICTURE: {message}")
}

impl Picture {
    fn compile(pattern: &str) -> Result<Self> {
        use Symbol::*;
        let symbols = Self::expand(pattern)?;
        let count = |s| symbols.iter().filter(|t| **t == s).count();
        if count(Decimal) + count(Implied) > 1 {
            return Err(invalid("only one decimal point (. or V) is allowed"));
        }
        if count(Zero) > 0 && count(Star) > 0 {
            return Err(invalid("Z and * cannot be combined"));
        }
        if count(Plus) > 0 && count(Minus) > 0
            || count(Credit) + count(Debit) > 1
            || count(Credit) + count(Debit) > 0 && count(Plus) + count(Minus) > 0
        {
            return Err(invalid("only one kind of editing sign is allowed"));
        }
        let floating: Vec<_> = [Plus, Minus, Currency]
            .into_iter()
            .filter(|s| count(*s) > 1)
            .collect();
        if floating.len() > 1 || !floating.is_empty() && count(Zero) + count(Star) > 0 {
            return Err(invalid(
                "only one kind of floating or suppression symbol is allowed",
            ));
        }
        let floating = floating.first().copied();
        let boundary = symbols
            .iter()
            .position(|s| matches!(s, Decimal | Implied))
            .unwrap_or(symbols.len());
        if symbols[boundary..].iter().any(|s| matches!(s, Zero | Star)) && symbols.contains(&Nine) {
            return Err(invalid(
                "fractional Z or * requires suppression of every digit position",
            ));
        }
        if let Some(s) = floating {
            let first = symbols.iter().position(|t| *t == s).unwrap();
            let last = symbols.iter().rposition(|t| *t == s).unwrap();
            if first >= boundary
                || symbols[..first].iter().any(|t| t.digit())
                || symbols[first..=last]
                    .iter()
                    .any(|t| *t != s && !matches!(t, Insert(_) | Decimal | Implied))
                || last > boundary && symbols.iter().any(|t| t.digit())
            {
                return Err(invalid(
                    "floating symbols must form the leading digit positions",
                ));
            }
        }
        let mut mandatory = false;
        let mut digits = 0;
        let mut scale = 0;
        let mut first_float = true;
        for (index, s) in symbols.iter().copied().enumerate() {
            if s == Nine {
                mandatory = true;
            } else if matches!(s, Zero | Star) && mandatory {
                return Err(invalid("Z and * must precede all mandatory 9 positions"));
            }
            if matches!(s, Plus | Minus)
                && Some(s) != floating
                && index != 0
                && index + 1 != symbols.len()
            {
                return Err(invalid("a fixed sign must be first or last"));
            }
            if matches!(s, Credit | Debit) && index + 1 != symbols.len() {
                return Err(invalid("CR and DB must be last"));
            }
            if s == Currency
                && floating != Some(Currency)
                && index != 0
                && !(index == 1 && matches!(symbols[0], Plus | Minus))
            {
                return Err(invalid(
                    "a fixed $ must be first or immediately after a leading sign",
                ));
            }
            let is_digit = if Some(s) == floating {
                !std::mem::take(&mut first_float)
            } else {
                s.digit()
            };
            if is_digit {
                digits += 1;
                if index > boundary {
                    scale += 1;
                }
            }
        }
        if digits == 0 {
            return Err(invalid("at least one digit position is required"));
        }
        let replacement = if count(Zero) > 0 || floating.is_some() {
            Some(b' ')
        } else if count(Star) > 0 {
            Some(b'*')
        } else {
            None
        };
        let all_suppressed = !symbols.contains(&Nine) && replacement.is_some();
        let width = symbols.iter().map(|s| s.width()).sum();
        Ok(Self {
            symbols,
            floating,
            replacement,
            all_suppressed,
            digits,
            scale,
            width,
        })
    }

    fn expand(pattern: &str) -> Result<Vec<Symbol>> {
        use Symbol::*;
        const MAX_WIDTH: usize = 4096;
        if pattern.is_empty() || pattern.len() > MAX_WIDTH || !pattern.is_ascii() {
            return Err(invalid(
                "the format must contain 1 to 4096 ASCII characters",
            ));
        }
        let input = pattern.to_ascii_uppercase();
        let input = input.as_bytes();
        let mut symbols = Vec::new();
        let mut index = 0;
        let mut width = 0;
        while index < input.len() {
            let s = match input[index] {
                b'9' => Nine,
                b'Z' => Zero,
                b'*' => Star,
                b'.' => Decimal,
                b'V' => Implied,
                b'B' => Insert(b' '),
                b'0' | b'/' | b',' => Insert(input[index]),
                b'+' => Plus,
                b'-' => Minus,
                b'$' => Currency,
                b'C' if input.get(index + 1) == Some(&b'R') => {
                    index += 1;
                    Credit
                }
                b'D' if input.get(index + 1) == Some(&b'B') => {
                    index += 1;
                    Debit
                }
                _ => {
                    return Err(invalid(
                        "unsupported symbol (use numeric editing symbols only)",
                    ));
                }
            };
            index += 1;
            let mut repetitions = 1usize;
            if input.get(index) == Some(&b'(') {
                if !s.repeatable() {
                    return Err(invalid("this symbol cannot have a repetition count"));
                }
                index += 1;
                let start = index;
                repetitions = 0;
                while let Some(d) = input.get(index).filter(|d| d.is_ascii_digit()) {
                    repetitions = repetitions
                        .checked_mul(10)
                        .and_then(|n| n.checked_add(usize::from(*d - b'0')))
                        .filter(|n| *n <= MAX_WIDTH)
                        .ok_or_else(|| invalid("expanded PICTURE exceeds 4096 positions"))?;
                    index += 1;
                }
                if index == start || repetitions == 0 || input.get(index) != Some(&b')') {
                    return Err(invalid(
                        "a repetition count must be a positive integer in parentheses",
                    ));
                }
                index += 1;
            }
            width += s.width() * repetitions;
            if width > MAX_WIDTH || symbols.len() + repetitions > MAX_WIDTH {
                return Err(invalid("expanded PICTURE exceeds 4096 positions"));
            }
            symbols.extend(std::iter::repeat_n(s, repetitions));
        }
        Ok(symbols)
    }

    fn format(&self, number: &Number) -> String {
        use Symbol::*;
        let digits = number.aligned(self.digits, self.scale);
        if self.all_suppressed && digits.iter().all(|d| *d == b'0') {
            let replacement = self.replacement.unwrap();
            let mut output = Vec::with_capacity(self.width);
            for s in &self.symbols {
                let fill = if *s == Decimal && replacement == b'*' {
                    b'.'
                } else {
                    replacement
                };
                output.extend(std::iter::repeat_n(fill, s.width()));
            }
            return String::from_utf8(output).unwrap();
        }
        let mut output = Vec::with_capacity(self.width);
        let mut digits = digits.into_iter();
        let mut leading = true;
        let mut started = false;
        let mut float_seen = false;
        for s in self.symbols.iter().copied() {
            let floating = Some(s) == self.floating;
            if floating && !float_seen {
                float_seen = true;
                started = true;
                output.push(b' ');
            } else if s.digit() || floating {
                let digit = digits.next().unwrap();
                let suppressible = s != Nine;
                started |= suppressible;
                if leading && suppressible && digit == b'0' {
                    output.push(self.replacement.unwrap());
                } else {
                    if leading && let Some(symbol) = self.floating {
                        *output.last_mut().unwrap() = symbol.sign(number.negative)[0];
                    }
                    leading = false;
                    output.push(digit);
                }
            } else {
                match s {
                    Decimal | Implied => {
                        if leading && let Some(symbol) = self.floating {
                            *output.last_mut().unwrap() = symbol.sign(number.negative)[0];
                        }
                        leading = false;
                        if s == Decimal {
                            output.push(b'.');
                        }
                    }
                    Insert(c) => output.push(if leading && started {
                        self.replacement.unwrap_or(c)
                    } else {
                        c
                    }),
                    Plus | Minus | Currency | Credit | Debit => {
                        output.extend_from_slice(s.sign(number.negative));
                    }
                    _ => unreachable!(),
                }
            }
        }
        String::from_utf8(output).unwrap()
    }
}
