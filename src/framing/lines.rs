//! Bounded, encoding-aware line records with COBOL-style space padding.
use crate::{
    config::Lines,
    encoding::TextEncoding,
    error::{Error, Result},
};
use std::io::{BufRead, Write};

#[derive(Clone, Debug)]
pub(crate) struct LineFraming {
    newline: Vec<u8>,
    carriage_return: Vec<u8>,
    space: Vec<u8>,
    separator: Vec<u8>,
    unit: usize,
    trim_trailing_spaces: bool,
}

impl LineFraming {
    pub(crate) fn new(encoding: TextEncoding, options: &Lines) -> Result<Self> {
        let ending = match options.line_ending.as_str() {
            "lf" => "\n",
            "crlf" => "\r\n",
            _ => return Err(Error::config("lines.line_ending must be lf or crlf")),
        };
        let encode = |text| {
            encoding.encode(text).map_err(|e| {
                Error::config(e.message).context("lines requires encoded CR, LF and U+0020 space")
            })
        };
        let unit = if matches!(encoding.name(), "UTF-16LE" | "UTF-16BE") {
            2
        } else {
            1
        };
        let framing = Self {
            newline: encode("\n")?,
            carriage_return: encode("\r")?,
            space: encode(" ")?,
            separator: encode(ending)?,
            unit,
            trim_trailing_spaces: options.trim_trailing_spaces,
        };
        if [&framing.newline, &framing.carriage_return, &framing.space]
            .iter()
            .any(|bytes| bytes.len() != unit)
        {
            return Err(Error::config("unsupported lines delimiter encoding"));
        }
        Ok(framing)
    }

    pub(crate) fn validate_width(&self, width: usize) -> Result<()> {
        if !width.is_multiple_of(self.unit) {
            return Err(Error::config(
                "lines byte positions and lengths must align with encoded characters",
            ));
        }
        Ok(())
    }

    fn has_line_break(&self, body: &[u8]) -> bool {
        body.chunks_exact(self.unit)
            .any(|unit| unit == self.newline || unit == self.carriage_return)
    }

    /// Return the physical byte count before padding, including the delimiter.
    pub(crate) fn read_into(
        &self,
        reader: &mut impl BufRead,
        length: usize,
        body: &mut Vec<u8>,
    ) -> Result<Option<usize>> {
        body.clear();
        let physical;
        loop {
            let buffer = reader.fill_buf().map_err(|e| Error::input(e.to_string()))?;
            if buffer.is_empty() {
                if body.is_empty() {
                    return Ok(None);
                }
                physical = body.len();
                break;
            }
            let end = memchr::memchr_iter(*self.newline.last().unwrap(), buffer).find(|&i| {
                (body.len() + i + 1).is_multiple_of(self.unit)
                    && (self.unit == 1
                        || if i == 0 {
                            body.last().copied()
                        } else {
                            Some(buffer[i - 1])
                        } == Some(self.newline[0]))
            });
            let consumed = end.map_or(buffer.len(), |i| i + 1);
            if body.len() + consumed > length + self.separator.len() {
                return Err(Error::input("lines record exceeds layout length"));
            }
            body.extend_from_slice(&buffer[..consumed]);
            reader.consume(consumed);
            if end.is_some() {
                physical = body.len();
                if !body.ends_with(&self.separator) {
                    return Err(Error::input("lines record separator mismatch"));
                }
                body.truncate(body.len() - self.separator.len());
                break;
            }
        }
        if body.len() > length {
            return Err(Error::input("lines record exceeds layout length"));
        }
        if !body.len().is_multiple_of(self.unit) {
            return Err(Error::input("incomplete encoded character in lines record"));
        }
        if self.has_line_break(body) {
            return Err(Error::input("lines record contains an unexpected CR/LF"));
        }
        while body.len() < length {
            body.extend_from_slice(&self.space);
        }
        Ok(Some(physical))
    }

    pub(crate) fn write(&self, writer: &mut impl Write, mut body: &[u8]) -> Result<()> {
        if self.has_line_break(body) {
            return Err(Error::output("lines record contains CR/LF"));
        }
        if self.trim_trailing_spaces {
            while let Some(rest) = body.strip_suffix(self.space.as_slice()) {
                body = rest;
            }
        }
        writer
            .write_all(body)
            .and_then(|_| writer.write_all(&self.separator))
            .map_err(|e| Error::output(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    #[test]
    fn encoded_delimiters_survive_every_buffer_alignment() {
        for label in ["utf-8", "utf-16le", "utf-16be", "cp932", "cp939"] {
            let encoding = TextEncoding::resolve(label).unwrap();
            let text = if label.starts_with("utf") {
                "Ċਁ"
            } else {
                "日本"
            };
            for ending in ["lf", "crlf"] {
                let delimiter = if ending == "lf" { "\n" } else { "\r\n" };
                let framing = LineFraming::new(
                    encoding,
                    &Lines {
                        line_ending: ending.into(),
                        ..Lines::default()
                    },
                )
                .unwrap();
                let data = encoding
                    .encode(&format!("{text}{delimiter}{delimiter}A"))
                    .unwrap();
                for capacity in 1..=9 {
                    let mut reader = BufReader::with_capacity(capacity, Cursor::new(&data));
                    let mut body = Vec::new();
                    for (value, separator) in [(text, delimiter), ("", delimiter), ("A", "")] {
                        let physical = encoding
                            .encode(&format!("{value}{separator}"))
                            .unwrap()
                            .len();
                        assert_eq!(
                            framing.read_into(&mut reader, 32, &mut body).unwrap(),
                            Some(physical),
                            "{label}/{ending}/{capacity}"
                        );
                        assert_eq!(body.len(), 32);
                        assert_eq!(encoding.decode(&body).unwrap().trim_end_matches(' '), value);
                    }
                    assert_eq!(framing.read_into(&mut reader, 32, &mut body).unwrap(), None);
                }
            }
        }
    }

    #[test]
    fn length_limits_and_partial_encoded_characters_are_rejected() {
        let framing = LineFraming::new(TextEncoding::UTF_8, &Lines::default()).unwrap();
        for data in [&b"123456789\n"[..], &b"123456789"[..]] {
            let mut reader = BufReader::with_capacity(1, data);
            assert!(
                framing
                    .read_into(&mut reader, 4, &mut Vec::new())
                    .unwrap_err()
                    .message
                    .contains("exceeds layout length")
            );
        }
        let framing = LineFraming::new(
            TextEncoding::resolve("utf-16le").unwrap(),
            &Lines::default(),
        )
        .unwrap();
        let mut reader = BufReader::with_capacity(1, &b"A"[..]);
        assert!(
            framing
                .read_into(&mut reader, 4, &mut Vec::new())
                .unwrap_err()
                .message
                .contains("incomplete encoded character")
        );
    }
}
