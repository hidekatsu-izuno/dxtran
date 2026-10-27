//! IBM hexadecimal floating-point storage codecs.
use crate::error::{Error, Result};
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
