//! Exact decimal text, scaling, and zoned/packed storage codecs.
use super::{CompiledField, FieldType};
use crate::error::{Error, Result};

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

impl CompiledField {
    pub(super) fn decode_digits(&self, b: &[u8]) -> Result<i128> {
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
    pub(super) fn encode_digits(&self, value: i128) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.encode_digits_into(value, &mut bytes)?;
        Ok(bytes)
    }
    pub(super) fn encode_digits_into(&self, value: i128, bytes: &mut Vec<u8>) -> Result<()> {
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
