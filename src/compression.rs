//! File compression outside the record framing and character encoding layers.
use crate::config::Compression;
use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
};

pub(crate) enum Input {
    Plain(File),
    Gzip(Box<flate2::read::MultiGzDecoder<File>>),
    Zstd(Box<zstd::stream::read::Decoder<'static, BufReader<File>>>),
}
impl Input {
    pub(crate) fn new(file: File, compression: Option<Compression>) -> io::Result<Self> {
        Ok(match compression {
            None => Self::Plain(file),
            Some(Compression::Gzip) => {
                Self::Gzip(Box::new(flate2::read::MultiGzDecoder::new(file)))
            }
            // The default decoder reads every concatenated frame and verifies
            // the checksum when present. Do not stop at the first frame.
            Some(Compression::Zstd) => {
                Self::Zstd(Box::new(zstd::stream::read::Decoder::new(file)?))
            }
        })
    }
}
fn context(kind: &str, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{kind}: {error}"))
}
impl Read for Input {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(reader) => reader.read(buf),
            Self::Gzip(reader) => reader
                .read(buf)
                .map_err(|e| context("gzip decompression", e)),
            Self::Zstd(reader) => reader
                .read(buf)
                .map_err(|e| context("zstd decompression", e)),
        }
    }
}

pub(crate) enum Output {
    Plain(BufWriter<File>),
    Gzip(Box<Chunks<flate2::write::GzEncoder<BufWriter<File>>>>),
    Zstd(Box<Chunks<zstd::stream::write::Encoder<'static, BufWriter<File>>>>),
}
// Some compressors choose different blocks for different write sizes. Feed a
// fixed chunk size so parallel Arrow batches and per-record writes yield the
// same compressed bytes. BufWriter alone bypasses buffering for large writes.
pub(crate) struct Chunks<W: Write> {
    writer: W,
    pending: Vec<u8>,
}
impl<W: Write> Chunks<W> {
    const SIZE: usize = 64 * 1024;
    fn new(writer: W) -> Self {
        Self {
            writer,
            pending: Vec::with_capacity(Self::SIZE),
        }
    }
    fn drain(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            self.writer.write_all(&self.pending)?;
            self.pending.clear();
        }
        Ok(())
    }
    fn into_inner(mut self) -> io::Result<W> {
        self.drain()?;
        Ok(self.writer)
    }
}
impl<W: Write> Write for Chunks<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        if self.pending.len() == Self::SIZE {
            self.drain()?;
        }
        let n = bytes.len().min(Self::SIZE - self.pending.len());
        self.pending.extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.drain()?;
        self.writer.flush()
    }
}
impl Output {
    pub(crate) fn new(file: File, compression: Option<Compression>) -> io::Result<Self> {
        let file = BufWriter::new(file);
        Ok(match compression {
            None => Self::Plain(file),
            Some(Compression::Gzip) => {
                // No timestamp or filename, so identical jobs remain reproducible.
                let encoder = flate2::GzBuilder::new()
                    .mtime(0)
                    .operating_system(255)
                    .write(file, flate2::Compression::default());
                Self::Gzip(Box::new(Chunks::new(encoder)))
            }
            Some(Compression::Zstd) => {
                let mut encoder = zstd::stream::write::Encoder::new(file, 0)?;
                encoder.include_checksum(true)?;
                Self::Zstd(Box::new(Chunks::new(encoder)))
            }
        })
    }
    pub(crate) fn finish(self) -> io::Result<()> {
        // Finish every codec before syncing/publishing. Dropping an encoder or
        // flushing alone is insufficient to emit its trailer/final frame.
        let mut file = match self {
            Self::Plain(file) => file,
            Self::Gzip(writer) => (*writer)
                .into_inner()?
                .finish()
                .map_err(|e| context("gzip finalization", e))?,
            Self::Zstd(writer) => (*writer)
                .into_inner()?
                .finish()
                .map_err(|e| context("zstd finalization", e))?,
        };
        file.flush()?;
        file.get_ref().sync_all()
    }
}
impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(writer) => writer.write(buf),
            Self::Gzip(writer) => writer
                .write(buf)
                .map_err(|e| context("gzip compression", e)),
            Self::Zstd(writer) => writer
                .write(buf)
                .map_err(|e| context("zstd compression", e)),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(writer) => writer.flush(),
            Self::Gzip(writer) => writer.flush().map_err(|e| context("gzip compression", e)),
            Self::Zstd(writer) => writer.flush().map_err(|e| context("zstd compression", e)),
        }
    }
}
