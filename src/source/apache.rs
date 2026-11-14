//! Bounded, one-line Apache access log records.
use super::csv::CsvRecord;
use crate::{
    apache::LogFormat,
    encoding::TextEncoding,
    error::{Error, Result},
    framing::MAX_RECORD,
};
use std::io::{BufRead, BufReader, Read};

pub(super) struct ApacheReader<R> {
    reader: BufReader<R>,
    format: LogFormat,
    encoding: TextEncoding,
    line: Vec<u8>,
    position: u64,
}

impl<R: Read> ApacheReader<R> {
    pub(super) fn new(reader: R, format: LogFormat, encoding: TextEncoding) -> Self {
        Self {
            reader: BufReader::new(reader),
            format,
            encoding,
            line: Vec::new(),
            position: 0,
        }
    }

    // Positions refer to the decoded UTF-8 stream, as syntax is read after
    // the shared streaming decoder (including BOM handling).
    pub(super) fn position(&self) -> u64 {
        self.position
    }

    pub(super) fn read_record(&mut self, record: &mut CsvRecord) -> Result<bool> {
        self.line.clear();
        loop {
            let bytes = self
                .reader
                .fill_buf()
                .map_err(|e| Error::input(e.to_string()))?;
            if bytes.is_empty() {
                if self.line.is_empty() {
                    return Ok(false);
                }
                break;
            }
            let newline = memchr::memchr(b'\n', bytes);
            let count = newline.map_or(bytes.len(), |i| i + 1);
            if self.line.len() + count > MAX_RECORD {
                return Err(Error::input("Apache log record exceeds 16 MiB"));
            }
            self.line.extend_from_slice(&bytes[..count]);
            self.reader.consume(count);
            self.position += count as u64;
            if newline.is_some() {
                self.line.pop();
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                break;
            }
        }
        let line = std::str::from_utf8(&self.line)
            .map_err(|_| Error::input("invalid decoded UTF-8 in Apache log"))?;
        if line.bytes().any(|b| b.is_ascii_control() && b != b'\t') {
            return Err(Error::input("unescaped control character in Apache log"));
        }
        match &mut self.format {
            LogFormat::Common => parse(line, 7, self.encoding, record)?,
            LogFormat::Combined => parse(line, 9, self.encoding, record)?,
            LogFormat::Custom(format) => {
                record.text.clear();
                record.ends.clear();
                format.parse(line, |value, escaped, present| {
                    if escaped {
                        unescape(value, self.encoding, &mut record.text)?;
                    } else {
                        record.text.push_str(value);
                    }
                    record.ends.push((record.text.len(), present));
                    Ok(())
                })?;
            }
        }
        Ok(true)
    }
}

fn parse(line: &str, columns: usize, encoding: TextEncoding, record: &mut CsvRecord) -> Result<()> {
    record.text.clear();
    record.ends.clear();
    let mut rest = line;
    for column in 0..columns {
        if column > 0 {
            let trimmed = rest.trim_start_matches([' ', '\t']);
            if trimmed.len() == rest.len() {
                return Err(Error::input("missing Apache log field separator"));
            }
            rest = trimmed;
        }
        let (value, remaining) = match column {
            3 => delimited(rest, b'[', b']')?,
            4 | 7 | 8 => delimited(rest, b'"', b'"')?,
            _ => {
                let end = rest.find([' ', '\t']).unwrap_or(rest.len());
                let value = &rest[..end];
                if value.is_empty() || find_unescaped(value.as_bytes(), b'"').is_some() {
                    return Err(Error::input("invalid or missing Apache log field"));
                }
                (value, &rest[end..])
            }
        };
        // Reuse the cell flag as presence metadata, recording the raw dash
        // before unescaping so a literal \x2d remains a non-NULL string.
        unescape(value, encoding, &mut record.text)?;
        record.ends.push((record.text.len(), value != "-"));
        rest = remaining;
    }
    if !rest.trim_matches([' ', '\t']).is_empty() {
        return Err(Error::input("unexpected trailing Apache log fields"));
    }
    Ok(())
}

fn delimited(text: &str, open: u8, close: u8) -> Result<(&str, &str)> {
    if text.as_bytes().first() != Some(&open) {
        return Err(Error::input(if open == b'[' {
            "Apache log timestamp must be enclosed in brackets"
        } else {
            "Apache log request/header must be enclosed in quotes"
        }));
    }
    let content = &text[1..];
    if let Some(end) = find_unescaped(content.as_bytes(), close) {
        return Ok((&content[..end], &content[end + 1..]));
    }
    Err(Error::input("unclosed Apache log quote or bracket"))
}

fn find_unescaped(bytes: &[u8], delimiter: u8) -> Option<usize> {
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == delimiter {
            return Some(i);
        }
        i += if bytes[i] == b'\\' { 2 } else { 1 };
    }
    None
}

fn unescape(mut text: &str, encoding: TextEncoding, output: &mut String) -> Result<()> {
    if !text.contains('\\') {
        output.push_str(text);
        return Ok(());
    }
    // Reconstruct the encoded bytes before decoding. A multibyte character
    // can mix hex escapes and literal ASCII bytes (e.g. CP932 \x96{ = 本).
    let encode = |s: &str| encoding.encode(s).map_err(|e| Error::input(e.message));
    let mut bytes = Vec::new();
    while let Some(index) = text.find('\\') {
        bytes.extend_from_slice(&encode(&text[..index])?);
        text = &text[index + 1..];
        let byte = *text
            .as_bytes()
            .first()
            .ok_or_else(|| Error::input("truncated Apache log escape"))?;
        if byte == b'x' {
            let digits = text
                .as_bytes()
                .get(1..3)
                .ok_or_else(|| Error::input("truncated Apache log hex escape"))?;
            let hex = |b: u8| {
                (b as char)
                    .to_digit(16)
                    .ok_or_else(|| Error::input("invalid Apache log hex escape"))
            };
            bytes.push(((hex(digits[0])? << 4) | hex(digits[1])?) as u8);
            text = &text[3..];
        } else {
            let character = match byte {
                b'"' => '"',
                b'\\' => '\\',
                b'a' => '\u{7}',
                b'b' => '\u{8}',
                b'f' => '\u{c}',
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'v' => '\u{b}',
                _ => return Err(Error::input("invalid Apache log escape")),
            };
            bytes.extend_from_slice(&encode(character.encode_utf8(&mut [0; 4]))?);
            text = &text[1..];
        }
    }
    bytes.extend_from_slice(&encode(text)?);
    output.push_str(&encoding.decode(&bytes)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMON: &str =
        "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";

    #[test]
    fn syntax_survives_small_buffers_and_preserves_missing_and_empty_values() {
        let input =
            format!("{COMMON} \"-\" \"a\\\"b\\\\c\\t\\xE6\\x97\\xA5\"\r\n{COMMON} \"\" \"\\x2d\"");
        for capacity in 1..=32 {
            let mut reader =
                ApacheReader::new(input.as_bytes(), LogFormat::Combined, TextEncoding::UTF_8);
            reader.reader = BufReader::with_capacity(capacity, input.as_bytes());
            let mut record = CsvRecord::default();
            assert!(reader.read_record(&mut record).unwrap());
            let cells = record.cells().collect::<Vec<_>>();
            assert_eq!(cells[1], ("-", false));
            assert_eq!(cells[3], ("10/Oct/2000:13:55:36 -0700", true));
            assert_eq!(cells[4], ("GET /apache_pb.gif HTTP/1.0", true));
            assert_eq!(cells[7], ("-", false));
            assert_eq!(cells[8], ("a\"b\\c\t日", true));
            assert!(reader.read_record(&mut record).unwrap());
            let cells = record.cells().collect::<Vec<_>>();
            assert_eq!(cells[7], ("", true));
            assert_eq!(cells[8], ("-", true));
            assert!(!reader.read_record(&mut record).unwrap());
        }
    }

    #[test]
    fn malformed_records_are_rejected() {
        for input in [
            String::new(),
            COMMON.replace("[10/", "10/"),
            COMMON.replace("]", ""),
            COMMON.replace("\"GET", "GET"),
            format!("{COMMON} extra"),
            COMMON.replace("GET", "\\xGG"),
            COMMON.replace("GET", "\\q"),
            COMMON.replace("GET", "\\xFF"),
            COMMON.replace("GET", "\\x1"),
            COMMON.replace("GET", "\\x日"),
        ] {
            assert!(
                parse(&input, 7, TextEncoding::UTF_8, &mut CsvRecord::default()).is_err(),
                "{input}"
            );
        }
    }

    #[test]
    fn bare_values_allow_brackets_and_escaped_quotes() {
        for (logged, expected) in [(r#"a\"b"#, "a\"b"), ("f[rank]", "f[rank]")] {
            let mut record = CsvRecord::default();
            parse(
                &COMMON.replace("frank", logged),
                7,
                TextEncoding::UTF_8,
                &mut record,
            )
            .unwrap();
            assert_eq!(record.cells().nth(2).unwrap().0, expected);
        }
    }

    #[test]
    fn line_limit_counts_both_crlf_bytes_and_rejects_raw_controls() {
        for ending in ["", "\n", "\r\n"] {
            let input = format!(
                "{COMMON}{}{ending}",
                " ".repeat(MAX_RECORD - COMMON.len() - ending.len())
            );
            let mut reader =
                ApacheReader::new(input.as_bytes(), LogFormat::Common, TextEncoding::UTF_8);
            assert!(reader.read_record(&mut CsvRecord::default()).unwrap());
            let input = format!(" {input}");
            let mut reader =
                ApacheReader::new(input.as_bytes(), LogFormat::Common, TextEncoding::UTF_8);
            assert!(
                reader
                    .read_record(&mut CsvRecord::default())
                    .unwrap_err()
                    .message
                    .contains("16 MiB")
            );
        }
        for input in [format!("{COMMON}\r"), format!("{COMMON}\0\n"), "\n".into()] {
            let mut reader =
                ApacheReader::new(input.as_bytes(), LogFormat::Common, TextEncoding::UTF_8);
            assert!(reader.read_record(&mut CsvRecord::default()).is_err());
        }
    }
}
