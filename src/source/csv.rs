//! Streaming CSV syntax with reusable record storage and bulk byte searches.
use super::Cell;
use crate::{
    error::{Error, Result},
    framing,
};
use std::io::{BufRead, BufReader, Read};

#[derive(Default)]
pub(super) struct CsvRecord {
    pub(super) text: String,
    pub(super) ends: Vec<(usize, bool)>,
}
impl CsvRecord {
    pub(super) fn len(&self) -> usize {
        self.ends.len()
    }
    pub(super) fn bytes(&self) -> usize {
        self.text.len()
    }
    pub(super) fn cells(&self) -> impl Iterator<Item = (&str, bool)> {
        let mut start = 0;
        self.ends.iter().map(move |&(end, quoted)| {
            let value = &self.text[start..end];
            start = end;
            (value, quoted)
        })
    }
}

pub struct CsvReader<R> {
    reader: BufReader<R>,
    delimiter: u8,
    quote: u8,
    pub record: u64,
    pub line: u64,
}
impl<R: Read> CsvReader<R> {
    pub fn new(reader: R, delimiter: u8, quote: u8) -> Self {
        Self {
            reader: BufReader::new(reader),
            delimiter,
            quote,
            record: 0,
            line: 1,
        }
    }
    fn byte(&mut self) -> Result<Option<u8>> {
        let buf = self
            .reader
            .fill_buf()
            .map_err(|e| Error::input(e.to_string()))?;
        if buf.is_empty() {
            return Ok(None);
        }
        let b = buf[0];
        self.reader.consume(1);
        if b == b'\n' {
            self.line += 1;
        }
        Ok(Some(b))
    }
    pub fn next_record(&mut self) -> Result<Option<Vec<Cell>>> {
        let mut record = CsvRecord::default();
        Ok(self.read_record(&mut record)?.then(|| {
            record
                .cells()
                .map(|(value, was_quoted)| Cell {
                    value: value.into(),
                    was_quoted,
                })
                .collect()
        }))
    }
    pub(super) fn read_record(&mut self, record: &mut CsvRecord) -> Result<bool> {
        let mut value = std::mem::take(&mut record.text).into_bytes();
        value.clear();
        record.ends.clear();
        let (mut quoted, mut inside, mut closed, mut began) = (false, false, false, false);
        let mut size = 0usize;
        let mut field_start = 0;
        loop {
            // Only syntax characters take the state-machine path. In quoted
            // fields, CR/LF and delimiters are ordinary data, including across
            // buffer boundaries. Keep the physical line counter independent.
            if !closed {
                let buf = self
                    .reader
                    .fill_buf()
                    .map_err(|e| Error::input(e.to_string()))?;
                let n = if inside {
                    memchr::memchr(self.quote, buf).unwrap_or(buf.len())
                } else {
                    let n = memchr::memchr3(self.delimiter, self.quote, b'\n', buf)
                        .unwrap_or(buf.len());
                    memchr::memchr(b'\r', &buf[..n]).unwrap_or(n)
                };
                if n > 0 {
                    size += n;
                    if size > framing::MAX_RECORD {
                        return Err(Error::input("CSV record exceeds 16 MiB"));
                    }
                    self.line += memchr::memchr_iter(b'\n', &buf[..n]).count() as u64;
                    value.extend_from_slice(&buf[..n]);
                    self.reader.consume(n);
                    began = true;
                    continue;
                }
            }
            let next = self.byte()?;
            size += usize::from(next.is_some());
            if size > framing::MAX_RECORD {
                return Err(Error::input("CSV record exceeds 16 MiB"));
            }
            match next {
                None => {
                    if inside {
                        return Err(Error::input("unclosed CSV quote"));
                    }
                    if !began && record.ends.is_empty() && value.is_empty() {
                        record.text = String::from_utf8(value).expect("empty record");
                        return Ok(false);
                    }
                    break;
                }
                Some(b) if inside => {
                    debug_assert_eq!(b, self.quote);
                    inside = false;
                    closed = true;
                    began = true;
                }
                Some(b) if closed && b == self.quote => {
                    value.push(b);
                    inside = true;
                    closed = false;
                }
                Some(b) if b == self.delimiter => {
                    record.ends.push((value.len(), quoted));
                    field_start = value.len();
                    quoted = false;
                    closed = false;
                    began = true;
                }
                Some(b'\n') => break,
                Some(b'\r') => {
                    if self.byte()? != Some(b'\n') {
                        return Err(Error::input("CSV line ending must be LF or CRLF"));
                    }
                    if size == framing::MAX_RECORD {
                        return Err(Error::input("CSV record exceeds 16 MiB"));
                    }
                    break;
                }
                Some(_) if closed => {
                    return Err(Error::input("invalid character after closing CSV quote"));
                }
                Some(b) if b == self.quote => {
                    if value.len() != field_start {
                        return Err(Error::input("quote in an unquoted CSV field"));
                    }
                    quoted = true;
                    inside = true;
                    began = true;
                }
                Some(_) => unreachable!("ordinary bytes were consumed in bulk"),
            }
        }
        record.ends.push((value.len(), quoted));
        record.text =
            String::from_utf8(value).map_err(|_| Error::input("invalid decoded UTF-8"))?;
        // Concatenation must not join invalid fragments into one code point
        // across a delimiter: the public CsvReader also accepts raw bytes.
        if record
            .ends
            .iter()
            .any(|&(end, _)| !record.text.is_char_boundary(end))
        {
            return Err(Error::input("invalid decoded UTF-8"));
        }
        self.record += 1;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_and_utf8_survive_every_small_buffer_boundary() {
        let input = "a,,\"\",\"日本,語\",\"a\"\"b\",\"x\r\ny\nz\"\r\n\nlast,";
        for capacity in 1..=32 {
            let mut reader = CsvReader::new(input.as_bytes(), b',', b'"');
            reader.reader = BufReader::with_capacity(capacity, input.as_bytes());
            let mut record = CsvRecord::default();
            assert!(reader.read_record(&mut record).unwrap());
            assert_eq!(
                record.cells().collect::<Vec<_>>(),
                [
                    ("a", false),
                    ("", false),
                    ("", true),
                    ("日本,語", true),
                    ("a\"b", true),
                    ("x\r\ny\nz", true),
                ]
            );
            assert_eq!(reader.line, 4);
            assert!(reader.read_record(&mut record).unwrap());
            assert_eq!(record.cells().collect::<Vec<_>>(), [("", false)]);
            assert!(reader.read_record(&mut record).unwrap());
            assert_eq!(
                record.cells().collect::<Vec<_>>(),
                [("last", false), ("", false)]
            );
            assert_eq!((reader.record, reader.line), (3, 5));
            assert!(!reader.read_record(&mut record).unwrap());
        }
    }

    #[test]
    fn malformed_csv_is_rejected_across_buffer_boundaries() {
        for input in [
            &b"a\"b\n"[..],
            b"\"a\"x\n",
            b"a\rb",
            b"\"a\"\"",
            b"\xff,\n",
            b"\xc2,\xa2\n",
        ] {
            for capacity in 1..=8 {
                let mut reader = CsvReader::new(input, b',', b'"');
                reader.reader = BufReader::with_capacity(capacity, input);
                assert!(
                    reader.next_record().is_err(),
                    "{input:?}, capacity {capacity}"
                );
            }
        }
    }

    #[test]
    fn record_limit_counts_quotes_and_both_crlf_bytes() {
        for ending in ["", "\n", "\r\n"] {
            let mut input = vec![b'x'; framing::MAX_RECORD - ending.len()];
            input.extend_from_slice(ending.as_bytes());
            assert!(
                CsvReader::new(input.as_slice(), b',', b'"')
                    .next_record()
                    .unwrap()
                    .is_some()
            );
            input.insert(0, b'x');
            assert!(
                CsvReader::new(input.as_slice(), b',', b'"')
                    .next_record()
                    .unwrap_err()
                    .message
                    .contains("16 MiB")
            );
        }
        let input = format!("\"{}\"", "\"\"".repeat(framing::MAX_RECORD / 2));
        assert!(
            CsvReader::new(input.as_bytes(), b',', b'"')
                .next_record()
                .unwrap_err()
                .message
                .contains("16 MiB")
        );
    }
}
