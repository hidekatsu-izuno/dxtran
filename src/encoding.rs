use crate::error::{Error, Result};
use encoding_rs::{Decoder, DecoderResult, Encoder, EncoderResult, UTF_8, UTF_16BE, UTF_16LE};
use jtc_utils::charset::{self, Charset};
use std::borrow::Cow;
use std::io::{self, Read, Write};

#[derive(Clone, Copy, Debug)]
pub struct TextEncoding(pub Charset);
impl TextEncoding {
    pub const UTF_8: Self = Self(charset::UTF_8);

    pub fn resolve(label: &str) -> Result<Self> {
        Charset::for_label(label)
            .map(Self)
            .ok_or_else(|| Error::config(format!("unknown encoding '{label}'")))
    }
    pub fn name(self) -> &'static str {
        self.0.name()
    }
    pub fn output_name(self) -> &'static str {
        match self.0 {
            Charset::Standard(enc) if enc != UTF_16LE && enc != UTF_16BE => {
                enc.output_encoding().name()
            }
            _ => self.name(),
        }
    }
    pub fn is_utf8(self) -> bool {
        self.0 == charset::UTF_8
    }
    pub(crate) fn max_utf8_length(self, width: usize) -> usize {
        match self.0 {
            Charset::Standard(enc) if enc == UTF_8 => width,
            Charset::Standard(enc) => enc
                .new_decoder_without_bom_handling()
                .max_utf8_buffer_length_without_replacement(width)
                .unwrap_or(usize::MAX),
            // Legacy single-byte characters need at most four UTF-8 bytes.
            // JEF may map two bytes to two Unicode scalars (at most eight bytes).
            _ => width.saturating_mul(4),
        }
    }
    pub fn decode(self, bytes: &[u8]) -> Result<String> {
        self.decode_cow(bytes).map(Cow::into_owned)
    }
    pub fn decode_cow(self, bytes: &[u8]) -> Result<Cow<'_, str>> {
        let Charset::Standard(enc) = self.0 else {
            return self
                .0
                .decode(bytes)
                .map(Cow::Owned)
                .map_err(|e| Error::input(format!("invalid encoded byte sequence: {e}")));
        };
        enc.decode_without_bom_handling_and_without_replacement(bytes)
            .ok_or_else(|| Error::input("invalid encoded byte sequence"))
    }
    pub fn encode(self, s: &str) -> Result<Vec<u8>> {
        let Charset::Standard(enc) = self.0 else {
            return self.0.encode(s).map_err(|e| {
                Error::output(format!("character cannot be represented in encoding: {e}"))
            });
        };
        if enc == UTF_16LE || enc == UTF_16BE {
            return Ok(s
                .encode_utf16()
                .flat_map(|c| {
                    if enc == UTF_16LE {
                        c.to_le_bytes()
                    } else {
                        c.to_be_bytes()
                    }
                })
                .collect());
        }
        let (bytes, _, errors) = enc.encode(s);
        if errors {
            Err(Error::output("character cannot be represented in encoding"))
        } else {
            Ok(bytes.into_owned())
        }
    }
}

enum StreamDecoder {
    Standard(Decoder),
    Legacy(charset::Decoder),
}

pub struct TextReader<R> {
    reader: R,
    decoder: StreamDecoder,
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
            decoder: match encoding.0 {
                Charset::Standard(enc) => {
                    StreamDecoder::Standard(enc.new_decoder_without_bom_handling())
                }
                enc => StreamDecoder::Legacy(enc.decoder()),
            },
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
                self.input.clear();
                self.input.extend_from_slice(&buf[..count]);
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
                        if Charset::Standard(enc) != self.encoding.0 {
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
            if let StreamDecoder::Legacy(decoder) = &mut self.decoder {
                self.output = decoder
                    .decode(&self.input[self.position..], self.eof)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .into_bytes();
                self.position = self.input.len();
                self.done = self.eof;
                continue;
            }
            let StreamDecoder::Standard(decoder) = &mut self.decoder else {
                unreachable!()
            };
            let mut out = [0; 32768];
            let (result, read, written) = decoder.decode_to_utf8_without_replacement(
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
enum StreamEncoder {
    Standard(Encoder),
    Legacy(charset::Encoder),
}

pub struct TextWriter<W> {
    pub inner: W,
    encoding: TextEncoding,
    encoder: Option<StreamEncoder>,
}
impl<W: Write> TextWriter<W> {
    pub fn new(inner: W, encoding: TextEncoding) -> Self {
        let encoder = match encoding.0 {
            Charset::Standard(enc) if enc == UTF_16LE || enc == UTF_16BE => None,
            Charset::Standard(enc) => Some(StreamEncoder::Standard(enc.new_encoder())),
            enc => Some(StreamEncoder::Legacy(enc.encoder())),
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
        // Rust strings are already valid UTF-8, which has no encoder state.
        if self.encoding.is_utf8() {
            return self
                .inner
                .write_all(s.as_bytes())
                .map_err(|e| Error::output(e.to_string()));
        }
        if let Some(StreamEncoder::Legacy(encoder)) = self.encoder.as_mut() {
            let bytes = encoder.encode(s, last).map_err(|e| {
                Error::output(format!("character cannot be represented in encoding: {e}"))
            })?;
            self.inner
                .write_all(&bytes)
                .map_err(|e| Error::output(e.to_string()))?;
        } else if let Some(StreamEncoder::Standard(encoder)) = self.encoder.as_mut() {
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

    #[test]
    fn legacy_labels_and_known_bytes() {
        for (label, canonical, text, bytes) in [
            (" JEF\t", "x-fujitsu-jef", "日本", "c6fccbdc"),
            ("X-FUJITSU-JEF", "x-fujitsu-jef", "あ", "a4a2"),
            (
                "x-fujitsu-ebcdic-ascii",
                "x-fujitsu-ebcdic-ascii",
                "Abc",
                "c18283",
            ),
            (
                "x-fujitsu-ebcdic-kana",
                "x-fujitsu-ebcdic-kana",
                "ABC",
                "c1c2c3",
            ),
            (
                "x-fujitsu-ebcdic-lower",
                "x-fujitsu-ebcdic-lower",
                "Abc",
                "c18283",
            ),
            ("cp930", "cp930", "A日本B", "c10e456245660fc2"),
            ("cp939", "cp939", "A日本B", "c10e456245660fc2"),
            ("cp942", "cp942", "Aあ\\", "4182a0fe"),
            ("cp942c", "cp942c", "Aあ\\", "4182a05c"),
            ("cp943c", "cp943c", "A〜\\", "4181605c"),
        ] {
            let encoding = TextEncoding::resolve(label).unwrap();
            let bytes = hex::decode(bytes).unwrap();
            assert_eq!(encoding.name(), canonical, "{label}");
            assert_eq!(encoding.output_name(), canonical, "{label}");
            assert_eq!(encoding.encode(text).unwrap(), bytes, "{label}");
            assert_eq!(encoding.decode(&bytes).unwrap(), text, "{label}");
            assert!(encoding.max_utf8_length(bytes.len()) >= text.len());
        }
        for code in ["930", "939", "942", "942c", "943c"] {
            for alias in [format!("IBM{code}"), format!(" IBM-{code}\n")] {
                assert_eq!(
                    TextEncoding::resolve(&alias).unwrap().name(),
                    format!("cp{code}")
                );
            }
        }
        for label in ["cp932", "windows-31j", "windows31j"] {
            assert_eq!(TextEncoding::resolve(label).unwrap().0, charset::SHIFT_JIS);
        }
        assert_eq!(
            TextEncoding::resolve("cp943c")
                .unwrap()
                .decode(&[0x81, 0x60])
                .unwrap(),
            "〜"
        );
        assert_eq!(
            TextEncoding::resolve("cp932")
                .unwrap()
                .decode(&[0x81, 0x60])
                .unwrap(),
            "～"
        );
        assert_eq!(TextEncoding::resolve("unknown").unwrap_err().code, 2);
    }
    #[test]
    fn utf16_bytes() {
        assert_eq!(
            TextEncoding(charset::UTF_16LE).encode("A　").unwrap(),
            [0x41, 0, 0, 0x30]
        );
        assert_eq!(
            TextEncoding(charset::UTF_16BE).encode("A　").unwrap(),
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
            TextReader::new(&b"\xff\xfeA\0"[..], TextEncoding::UTF_8)
                .read_to_string(&mut s)
                .is_err()
        );
        assert!(TextEncoding::UTF_8.decode(b"\xff").is_err());
    }
    #[test]
    fn decoder_retains_state_across_single_bytes() {
        for (label, text) in [
            ("utf-8", "日本,\"東京\"\n"),
            ("shift_jis", "日本,\"東京\"\n"),
            ("iso-2022-jp", "日本,\"東京\"\n"),
            ("utf-16le", "日本,\"東京\"\n"),
            ("utf-16be", "日本,\"東京\"\n"),
            ("jef", "あ海\u{2000b}\u{1b019}\u{3099}\u{e000}"),
            ("x-fujitsu-ebcdic-ascii", "ABC,\"def\"\n"),
            ("x-fujitsu-ebcdic-kana", "ABC,\"ｱｲｳ\"\n"),
            ("x-fujitsu-ebcdic-lower", "ABC,\"def\"\n"),
            ("cp930", "A日本,\"東京\"\n海"),
            ("cp939", "A日本,\"東京\"\n海"),
            ("cp942", "A日本,\"東京\"\n"),
            ("cp942c", "A日本,\"東京\"\n"),
            ("cp943c", "A日本,\"東京\"\n"),
        ] {
            let encoding = TextEncoding::resolve(label).unwrap();
            let bytes = encoding.encode(text).unwrap();
            let mut output = String::new();
            TextReader::new(One(&bytes), encoding)
                .read_to_string(&mut output)
                .unwrap();
            assert_eq!(output, text, "{label}");

            let mut writer = TextWriter::new(Vec::new(), encoding);
            for c in text.chars() {
                writer.text(c.encode_utf8(&mut [0; 4])).unwrap();
                writer.text("").unwrap();
            }
            writer.finish().unwrap();
            assert_eq!(writer.inner, bytes, "{label}");
        }
    }

    #[test]
    fn legacy_errors_are_strict_in_fields_and_streams() {
        for (label, bytes) in [
            ("jef", &b"\xa4"[..]),
            ("jef", &b"\0\0"[..]),
            ("cp939", &b"\xc1\x0e\x44"[..]),
            ("cp942", &b"\x82"[..]),
        ] {
            let encoding = TextEncoding::resolve(label).unwrap();
            assert_eq!(encoding.decode(bytes).unwrap_err().code, 3);
            let error = TextReader::new(One(bytes), encoding)
                .read_to_string(&mut String::new())
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}");
        }
        for label in [
            "jef",
            "cp930",
            "cp939",
            "cp942",
            "cp942c",
            "cp943c",
            "x-fujitsu-ebcdic-ascii",
            "x-fujitsu-ebcdic-kana",
            "x-fujitsu-ebcdic-lower",
        ] {
            let encoding = TextEncoding::resolve(label).unwrap();
            assert_eq!(encoding.encode("😀").unwrap_err().code, 5, "{label}");
            let mut writer = TextWriter::new(Vec::new(), encoding);
            assert_eq!(writer.text("😀").unwrap_err().code, 5, "{label}");
        }
    }

    #[test]
    fn bom_and_replacement_behavior_is_preserved() {
        for label in ["utf-8", "utf-16le", "utf-16be"] {
            let encoding = TextEncoding::resolve(label).unwrap();
            let bytes = encoding.encode("\u{feff}A").unwrap();
            assert_eq!(encoding.decode(&bytes).unwrap(), "\u{feff}A");
            let mut output = String::new();
            TextReader::new(One(&bytes), encoding)
                .read_to_string(&mut output)
                .unwrap();
            assert_eq!(output, "A");
            let legacy = TextEncoding::resolve("cp939").unwrap();
            let error = TextReader::new(One(&bytes), legacy)
                .read_to_string(&mut String::new())
                .unwrap_err();
            assert!(error.to_string().contains("BOM does not match"));
        }
        let encoding = TextEncoding::resolve("replacement").unwrap();
        assert!(encoding.decode(b"text").is_err());
        assert_eq!(encoding.output_name(), "UTF-8");
        assert_eq!(encoding.encode("日本").unwrap(), "日本".as_bytes());
        let mut writer = TextWriter::new(Vec::new(), encoding);
        writer.text("日本").unwrap();
        writer.finish().unwrap();
        assert_eq!(writer.inner, "日本".as_bytes());
        assert!(matches!(
            TextEncoding::UTF_8.decode_cow(b"text").unwrap(),
            Cow::Borrowed("text")
        ));
    }
}
