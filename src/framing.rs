use crate::error::{Error, Result};
use std::io::{Read, Write};
pub const MAX_RECORD: usize = 16 * 1024 * 1024;
fn exact<R: Read>(reader: &mut R, buf: &mut [u8], allow_eof: bool) -> Result<bool> {
    let mut n = 0;
    while n < buf.len() {
        match reader.read(&mut buf[n..]) {
            Ok(0) if n == 0 && allow_eof => return Ok(false),
            Ok(0) => return Err(Error::input("truncated record")),
            Ok(k) => n += k,
            Err(e) => return Err(Error::input(e.to_string())),
        }
    }
    Ok(true)
}
pub fn fixed<R: Read>(reader: &mut R, length: usize, separator: &[u8]) -> Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    Ok(fixed_into(reader, length, separator, &mut body)?.then_some(body))
}
pub fn fixed_into<R: Read>(
    reader: &mut R,
    length: usize,
    separator: &[u8],
    body: &mut Vec<u8>,
) -> Result<bool> {
    body.resize(length, 0);
    if !exact(reader, body, true)? {
        body.clear();
        return Ok(false);
    }
    if !separator.is_empty() {
        let mut actual = vec![0; separator.len()];
        if exact(reader, &mut actual, true)? && actual != separator {
            return Err(Error::input("fixed record separator mismatch"));
        }
    }
    Ok(true)
}
pub fn rdw<R: Read>(reader: &mut R, max: usize) -> Result<Option<Vec<u8>>> {
    let mut body = Vec::new();
    Ok(rdw_into(reader, max, &mut body)?.then_some(body))
}
pub fn rdw_into<R: Read>(reader: &mut R, max: usize, body: &mut Vec<u8>) -> Result<bool> {
    let mut header = [0; 4];
    if !exact(reader, &mut header, true)? {
        body.clear();
        return Ok(false);
    }
    let length = u16::from_be_bytes([header[0], header[1]]) as usize;
    if length < 4 || length > max || header[2..] != [0, 0] {
        return Err(Error::input(format!(
            "invalid RDW declared_length={length}, reserved bytes must be zero"
        )));
    }
    body.resize(length - 4, 0);
    exact(reader, body, false)?;
    Ok(true)
}
pub fn write_rdw<W: Write>(writer: &mut W, body: &[u8], max: usize) -> Result<()> {
    let n = body
        .len()
        .checked_add(4)
        .filter(|n| *n <= max && *n <= 65535)
        .ok_or_else(|| Error::output("RDW maximum length exceeded"))?;
    let [a, b] = (n as u16).to_be_bytes();
    writer
        .write_all(&[a, b, 0, 0])
        .and_then(|_| writer.write_all(body))
        .map_err(|e| Error::output(e.to_string()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn boundaries() {
        let mut r = &b"a\nb\n"[..];
        assert_eq!(fixed(&mut r, 2, b"\n").unwrap_err().code, 3);
        let mut r = &b"ab\ncd"[..];
        assert_eq!(fixed(&mut r, 2, b"\n").unwrap().unwrap(), b"ab");
        assert_eq!(fixed(&mut r, 2, b"\n").unwrap().unwrap(), b"cd");
        assert!(fixed(&mut r, 2, b"\n").unwrap().is_none());
    }
    #[test]
    fn rdw_bytes() {
        let mut w = vec![];
        write_rdw(&mut w, b"abc", 32760).unwrap();
        assert_eq!(w, b"\0\x07\0\0abc");
        for v in [
            &b"\0\x03\0\0"[..],
            &b"\0\x04\0\x01"[..],
            &b"\0\x06\0\0a"[..],
            &b"\0\x04\0"[..],
        ] {
            assert!(rdw(&mut &*v, 32760).is_err());
        }
    }
}
