use crate::error::{Error, Result};
use encoding_rs::{
    Decoder, DecoderResult, Encoder, EncoderResult, Encoding, UTF_8, UTF_16BE, UTF_16LE,
};
use std::borrow::Cow;
use std::io::{self, Read, Write};

#[derive(Clone, Copy, Debug)]
pub struct TextEncoding(pub &'static Encoding);
impl TextEncoding {
    pub fn resolve(label: &str) -> Result<Self> {
        let label = if label.trim().eq_ignore_ascii_case("cp932") {
            "shift_jis"
        } else {
            label
        };
        Encoding::for_label(label.as_bytes())
            .map(Self)
            .ok_or_else(|| Error::config(format!("unknown encoding '{label}'")))
    }
    pub fn decode(self, bytes: &[u8]) -> Result<String> {
        self.decode_cow(bytes).map(Cow::into_owned)
    }
    pub fn decode_cow(self, bytes: &[u8]) -> Result<Cow<'_, str>> {
        self.0
            .decode_without_bom_handling_and_without_replacement(bytes)
            .ok_or_else(|| Error::input("invalid encoded byte sequence"))
    }
    pub fn encode(self, s: &str) -> Result<Vec<u8>> {
        if self.0 == UTF_16LE || self.0 == UTF_16BE {
            return Ok(s
                .encode_utf16()
                .flat_map(|c| {
                    if self.0 == UTF_16LE {
                        c.to_le_bytes()
                    } else {
                        c.to_be_bytes()
                    }
                })
                .collect());
        }
        let (bytes, _, errors) = self.0.encode(s);
        if errors {
            Err(Error::output("character cannot be represented in encoding"))
        } else {
            Ok(bytes.into_owned())
        }
    }
}

pub struct TextReader<R> {
    reader: R,
    decoder: Decoder,
    input: Vec<u8>,
    position: usize,
    output: Vec<u8>,
    out_pos: usize,
    eof: bool,
    done: bool,
    first: bool,
    encoding: TextEncoding,
}
impl<R: Read> TextReader<R> {
    pub fn new(reader: R, encoding: TextEncoding) -> Self {
        Self {
            reader,
            decoder: encoding.0.new_decoder_without_bom_handling(),
            input: vec![],
            position: 0,
            output: vec![],
            out_pos: 0,
            eof: false,
            done: false,
            first: true,
            encoding,
        }
    }
    fn pump(&mut self) -> io::Result<()> {
        self.output.clear();
        self.out_pos = 0;
        while self.output.is_empty() && !self.done {
            if self.position == self.input.len() && !self.eof {
                let mut buf = [0; 8192];
                let count = self.reader.read(&mut buf)?;
                self.input = buf[..count].to_vec();
                self.position = 0;
                self.eof = count == 0;
                if self.first {
                    // Read enough bytes to distinguish the three accepted BOMs, even from a short reader.
                    while self.input.len() < 3 && !self.eof {
                        let n = self.reader.read(&mut buf[..3 - self.input.len()])?;
                        self.eof = n == 0;
                        self.input.extend_from_slice(&buf[..n]);
                    }
                    let bom = if self.input.starts_with(&[0xef, 0xbb, 0xbf]) {
                        Some((UTF_8, 3))
                    } else if self.input.starts_with(&[0xff, 0xfe]) {
                        Some((UTF_16LE, 2))
                    } else if self.input.starts_with(&[0xfe, 0xff]) {
                        Some((UTF_16BE, 2))
                    } else {
                        None
                    };
                    if let Some((enc, len)) = bom {
                        if enc != self.encoding.0 {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "BOM does not match configured encoding",
                            ));
                        }
                        self.position = len;
                    }
                    self.first = false;
                }
            }
            let mut out = [0; 32768];
            let (result, read, written) = self.decoder.decode_to_utf8_without_replacement(
                &self.input[self.position..],
                &mut out,
                self.eof,
            );
            self.position += read;
            self.output.extend_from_slice(&out[..written]);
            match result {
                DecoderResult::Malformed(..) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid encoded byte sequence",
                    ));
                }
                DecoderResult::InputEmpty if self.eof => self.done = true,
                _ => (),
            }
        }
        Ok(())
    }
}
impl<R: Read> Read for TextReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.out_pos == self.output.len() {
            self.pump()?;
        }
        let n = buf.len().min(self.output.len() - self.out_pos);
        buf[..n].copy_from_slice(&self.output[self.out_pos..self.out_pos + n]);
        self.out_pos += n;
        Ok(n)
    }
}
pub struct TextWriter<W> {
    pub inner: W,
    encoding: TextEncoding,
    encoder: Option<Encoder>,
}
impl<W: Write> TextWriter<W> {
    pub fn new(inner: W, encoding: TextEncoding) -> Self {
        let encoder = if encoding.0 == UTF_16LE || encoding.0 == UTF_16BE {
            None
        } else {
            Some(encoding.0.new_encoder())
        };
        Self {
            inner,
            encoding,
            encoder,
        }
    }
    pub fn text(&mut self, s: &str) -> Result<()> {
        self.write_text(s, false)
    }
    fn write_text(&mut self, mut s: &str, last: bool) -> Result<()> {
        if let Some(encoder) = self.encoder.as_mut() {
            loop {
                let mut out = [0; 16384];
                let (result, read, written) =
                    encoder.encode_from_utf8_without_replacement(s, &mut out, last);
                self.inner
                    .write_all(&out[..written])
                    .map_err(|e| Error::output(e.to_string()))?;
                s = &s[read..];
                match result {
                    EncoderResult::InputEmpty => break,
                    EncoderResult::Unmappable(_) => {
                        return Err(Error::output("character cannot be represented in encoding"));
                    }
                    EncoderResult::OutputFull => (),
                }
            }
        } else {
            self.inner
                .write_all(&self.encoding.encode(s)?)
                .map_err(|e| Error::output(e.to_string()))?;
        }
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        self.write_text("", true)?;
        self.inner.flush().map_err(|e| Error::output(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn utf16_bytes() {
        assert_eq!(
            TextEncoding(UTF_16LE).encode("A　").unwrap(),
            [0x41, 0, 0, 0x30]
        );
        assert_eq!(
            TextEncoding(UTF_16BE).encode("A　").unwrap(),
            [0, 0x41, 0x30, 0]
        );
    }
    #[test]
    fn stream_state() {
        let e = TextEncoding::resolve("iso-2022-jp").unwrap();
        let mut w = TextWriter::new(Vec::new(), e);
        w.text("日").unwrap();
        w.text("本\n").unwrap();
        w.finish().unwrap();
        let mut s = String::new();
        TextReader::new(w.inner.as_slice(), e)
            .read_to_string(&mut s)
            .unwrap();
        assert_eq!(s, "日本\n");
    }
    #[test]
    fn strict_bom() {
        let mut s = String::new();
        assert!(
            TextReader::new(&b"\xff\xfeA\0"[..], TextEncoding(UTF_8))
                .read_to_string(&mut s)
                .is_err()
        );
        assert!(TextEncoding(UTF_8).decode(b"\xff").is_err());
    }
    #[test]
    fn decoder_retains_state_across_single_bytes() {
        struct One<'a>(&'a [u8]);
        impl Read for One<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if self.0.is_empty() || out.is_empty() {
                    return Ok(0);
                }
                out[0] = self.0[0];
                self.0 = &self.0[1..];
                Ok(1)
            }
        }
        for label in ["utf-8", "shift_jis", "iso-2022-jp", "utf-16le", "utf-16be"] {
            let encoding = TextEncoding::resolve(label).unwrap();
            let bytes = encoding.encode("日本,\"東京\"\n").unwrap();
            let mut output = String::new();
            TextReader::new(One(&bytes), encoding)
                .read_to_string(&mut output)
                .unwrap();
            assert_eq!(output, "日本,\"東京\"\n", "{label}");
        }
    }
}
