//! Stateful RDW/SDW streams, with optional nonextended BDW blocks.
use crate::{
    config::Rdw,
    error::{Error, Result},
};
use std::io::{Read, Write};

const MAX_SEGMENT: usize = 32756;

#[derive(Clone, Copy, Default)]
pub(crate) struct Location {
    pub(crate) byte: u64,
    block: Option<(u64, u64)>,
}
impl Location {
    fn context(self, error: Error) -> Error {
        match self.block {
            Some((number, start)) => error.context(format!(
                "block {number} byte {start}, RDW/SDW byte {}",
                self.byte
            )),
            None => error.context(format!("RDW/SDW byte {}", self.byte)),
        }
    }
}

pub(crate) struct RdwReader<R> {
    reader: R,
    options: Rdw,
    position: u64,
    remaining: usize,
    block_number: u64,
    block_start: u64,
    record_location: Location,
}
impl<R: Read> RdwReader<R> {
    pub(crate) fn new(reader: R, options: Rdw) -> Self {
        Self {
            reader,
            options,
            position: 0,
            remaining: 0,
            block_number: 0,
            block_start: 0,
            record_location: Location::default(),
        }
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    pub(crate) fn record_context(&self, error: Error) -> Error {
        if self.options.bdw {
            self.record_location.context(error)
        } else {
            error
        }
    }

    fn exact(&mut self, bytes: &mut [u8], allow_eof: bool) -> Result<bool> {
        let mut read = 0;
        while read < bytes.len() {
            match self.reader.read(&mut bytes[read..]) {
                Ok(0) if read == 0 && allow_eof => return Ok(false),
                Ok(0) => return Err(Error::input("truncated record")),
                Ok(n) => {
                    read += n;
                    self.position += n as u64;
                }
                Err(e) => return Err(Error::input(e.to_string())),
            }
        }
        Ok(true)
    }

    fn header(&mut self) -> Result<Option<(usize, u8, Location)>> {
        if self.options.bdw && self.remaining == 0 {
            let start = self.position;
            let number = self.block_number + 1;
            let context = |error: Error| error.context(format!("block {number} BDW byte {start}"));
            let mut header = [0; 4];
            if !self.exact(&mut header, true).map_err(context)? {
                return Ok(None);
            }
            if header[0] & 0x80 != 0 {
                return Err(context(Error::input("extended BDW is not supported")));
            }
            let length = u16::from_be_bytes([header[0], header[1]]) as usize;
            if !(8..=self.options.max_block_length).contains(&length) || header[2..] != [0, 0] {
                return Err(context(Error::input(format!(
                    "invalid BDW declared_length={length}, reserved bytes must be zero"
                ))));
            }
            self.block_number = number;
            self.block_start = start;
            self.remaining = length - 4;
        }
        let location = Location {
            byte: self.position,
            block: self
                .options
                .bdw
                .then_some((self.block_number, self.block_start)),
        };
        if self.options.bdw && self.remaining < 4 {
            return Err(location.context(Error::input("BDW block ends inside an RDW/SDW header")));
        }
        let mut header = [0; 4];
        if !self
            .exact(&mut header, !self.options.bdw)
            .map_err(|e| location.context(e))?
        {
            return Ok(None);
        }
        let length = u16::from_be_bytes([header[0], header[1]]) as usize;
        let flag = header[2];
        let minimum = if self.options.spanned { 5 } else { 4 };
        let maximum = if self.options.spanned {
            MAX_SEGMENT
        } else {
            65535
        };
        if !(minimum..=maximum).contains(&length)
            || header[3] != 0
            || flag & !3 != 0
            || !self.options.spanned && flag != 0
        {
            return Err(location.context(Error::input(format!(
                "invalid RDW/SDW declared_length={length}, invalid segment code or reserved bytes"
            ))));
        }
        if self.options.bdw {
            if length > self.remaining {
                return Err(location.context(Error::input(format!(
                    "RDW/SDW declared_length={length} exceeds remaining BDW block length {}",
                    self.remaining
                ))));
            }
            self.remaining -= 4;
        }
        Ok(Some((length - 4, flag, location)))
    }

    pub(crate) fn read_into(&mut self, body: &mut Vec<u8>) -> Result<Option<Location>> {
        body.clear();
        let mut assembling = false;
        loop {
            let Some((length, flag, location)) = self.header()? else {
                return if assembling {
                    Err(self
                        .record_location
                        .context(Error::input("unterminated spanned record at EOF")))
                } else {
                    Ok(None)
                };
            };
            if assembling && matches!(flag, 0 | 1) || !assembling && matches!(flag, 2 | 3) {
                return Err(location.context(Error::input(format!(
                    "invalid spanned record sequence: segment code {flag}, pending_record={assembling}"
                ))));
            }
            if !assembling {
                self.record_location = location;
            }
            let start = body.len();
            let end = start
                .checked_add(length)
                .filter(|n| *n <= self.options.max_record_length - 4 && *n <= super::MAX_RECORD)
                .ok_or_else(|| {
                    location.context(Error::input("RDW logical record maximum length exceeded"))
                })?;
            body.resize(end, 0);
            self.exact(&mut body[start..], false)
                .map_err(|e| location.context(e))?;
            if self.options.bdw {
                self.remaining -= length;
            }
            if matches!(flag, 0 | 2) {
                return Ok(Some(self.record_location));
            }
            assembling = true;
        }
    }
}

pub(crate) struct RdwWriter {
    options: Rdw,
    block: Vec<u8>,
}
impl RdwWriter {
    pub(crate) fn new(options: Rdw) -> Self {
        Self {
            options,
            block: Vec::new(),
        }
    }

    pub(crate) fn finish(&mut self, writer: &mut impl Write) -> Result<()> {
        if !self.block.is_empty() {
            super::write_rdw(writer, &self.block, self.options.max_block_length)?;
            self.block.clear();
        }
        Ok(())
    }

    fn segment(&mut self, writer: &mut impl Write, body: &[u8], flag: u8) -> Result<()> {
        let length = body.len() + 4;
        let [a, b] = (length as u16).to_be_bytes();
        if self.options.bdw {
            self.block.extend_from_slice(&[a, b, flag, 0]);
            self.block.extend_from_slice(body);
        } else {
            writer
                .write_all(&[a, b, flag, 0])
                .and_then(|_| writer.write_all(body))
                .map_err(|e| Error::output(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn write_record(&mut self, writer: &mut impl Write, body: &[u8]) -> Result<()> {
        let length = body
            .len()
            .checked_add(4)
            .filter(|n| *n <= self.options.max_record_length && *n - 4 <= super::MAX_RECORD)
            .ok_or_else(|| Error::output("RDW logical record maximum length exceeded"))?;
        if !self.options.spanned {
            if !self.options.bdw {
                return super::write_rdw(writer, body, self.options.max_record_length);
            }
            if length + 4 > self.options.max_block_length {
                return Err(Error::output("RDW record does not fit in a BDW block"));
            }
            if self.block.len() + length + 4 > self.options.max_block_length {
                self.finish(writer)?;
            }
            return self.segment(writer, body, 0);
        }
        if body.is_empty() {
            return Err(Error::output(
                "spanned records require at least one payload byte",
            ));
        }
        let mut rest = body;
        let mut first = true;
        while !rest.is_empty() {
            let mut room = if self.options.bdw {
                self.options.max_block_length - 4 - self.block.len()
            } else {
                MAX_SEGMENT
            };
            if room < 5 {
                self.finish(writer)?;
                room = self.options.max_block_length - 4;
            }
            let size = rest.len().min(room - 4);
            let last = size == rest.len();
            let flag = match (first, last) {
                (true, true) => 0,
                (true, false) => 1,
                (false, true) => 2,
                (false, false) => 3,
            };
            self.segment(writer, &rest[..size], flag)?;
            rest = &rest[size..];
            first = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn segment(flag: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = ((body.len() + 4) as u16).to_be_bytes().to_vec();
        bytes.extend_from_slice(&[flag, 0]);
        bytes.extend_from_slice(body);
        bytes
    }

    fn block(segments: &[Vec<u8>]) -> Vec<u8> {
        segment(0, &segments.concat())
    }

    fn options(bdw: bool, spanned: bool) -> Rdw {
        Rdw {
            bdw,
            spanned,
            max_block_length: 16,
            ..Rdw::default()
        }
    }

    struct Fragmented<'a>(&'a [u8], usize);
    impl Read for Fragmented<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let length = buffer.len().min(self.1);
            self.0.read(&mut buffer[..length])
        }
    }

    fn decode(bytes: &[u8], options: Rdw, chunk: usize) -> Result<Vec<Vec<u8>>> {
        let mut reader = RdwReader::new(Fragmented(bytes, chunk), options);
        let mut records = Vec::new();
        let mut body = Vec::new();
        while reader.read_into(&mut body)?.is_some() {
            records.push(body.clone());
        }
        assert_eq!(reader.position(), bytes.len() as u64);
        Ok(records)
    }

    #[test]
    fn blocked_output_matches_independent_vb_and_vbs_bytes() {
        for spanned in [false, true] {
            let records: Vec<&[u8]> = if spanned {
                vec![b"ABCDEFGHIJ", b"KLM"]
            } else {
                vec![b"ABC", b"DEF", b"G"]
            };
            let expected = if spanned {
                [
                    block(&[segment(1, b"ABCDEFGH")]),
                    block(&[segment(2, b"IJ"), segment(1, b"KL")]),
                    block(&[segment(2, b"M")]),
                ]
                .concat()
            } else {
                [
                    block(&[segment(0, b"ABC")]),
                    block(&[segment(0, b"DEF"), segment(0, b"G")]),
                ]
                .concat()
            };
            let options = options(true, spanned);
            let mut output = Vec::new();
            let mut writer = RdwWriter::new(options.clone());
            for record in &records {
                writer.write_record(&mut output, record).unwrap();
            }
            writer.finish(&mut output).unwrap();
            writer.finish(&mut output).unwrap();
            assert_eq!(output, expected);
            for chunk in 1..=7 {
                assert_eq!(decode(&expected, options.clone(), chunk).unwrap(), records);
            }
        }
    }

    #[test]
    fn unblocked_spanning_handles_large_records_and_complete_records() {
        let body = vec![0x9a; 70000];
        let options = Rdw {
            spanned: true,
            max_record_length: 70004,
            ..Rdw::default()
        };
        let expected = [
            segment(1, &body[..32752]),
            segment(3, &body[32752..65504]),
            segment(2, &body[65504..]),
            segment(0, b"x"),
        ]
        .concat();
        let mut output = Vec::new();
        let mut writer = RdwWriter::new(options.clone());
        writer.write_record(&mut output, &body).unwrap();
        writer.write_record(&mut output, b"x").unwrap();
        writer.finish(&mut output).unwrap();
        assert_eq!(output, expected);
        assert_eq!(
            decode(&expected, options, 3).unwrap(),
            vec![body, b"x".to_vec()]
        );
    }

    #[test]
    fn malformed_blocks_and_segments_fail_before_crossing_block_boundaries() {
        let cases = [
            (vec![0, 7, 0, 0], "invalid BDW"),
            (vec![0, 17, 0, 0], "invalid BDW"),
            (vec![0, 8, 0, 1], "invalid BDW"),
            (vec![0x80, 0, 0, 8], "extended BDW"),
            (vec![0, 8, 0], "truncated record"),
            (vec![0, 8, 0, 0, 0, 8, 0, 0], "exceeds remaining BDW"),
            (
                vec![0, 10, 0, 0, 0, 5, 0, 0, b'A', 0],
                "ends inside an RDW/SDW",
            ),
            (vec![0, 9, 0, 0, 0, 5, 0, 0], "truncated record"),
            (vec![0, 8, 0, 0], "truncated record"),
            (block(&[segment(1, b"A")]), "invalid RDW/SDW"),
        ];
        for (bytes, message) in cases {
            for chunk in 1..=5 {
                let error = decode(&bytes, options(true, false), chunk).unwrap_err();
                assert_eq!(error.code, 3);
                assert!(error.message.contains(message), "{error}");
                assert!(error.message.contains("block 1"), "{error}");
            }
        }
    }

    #[test]
    fn spanned_record_state_machine_rejects_every_invalid_transition() {
        let sequences = [
            vec![2],
            vec![3],
            vec![1, 0],
            vec![1, 1],
            vec![0, 2],
            vec![0, 3],
            vec![1, 3, 0],
            vec![1, 3, 1],
            vec![1, 2, 2],
            vec![1, 2, 3],
        ];
        for bdw in [false, true] {
            for flags in &sequences {
                let segments: Vec<_> = flags.iter().map(|&flag| segment(flag, b"A")).collect();
                let bytes = if bdw {
                    segments
                        .iter()
                        .map(|s| block(std::slice::from_ref(s)))
                        .collect::<Vec<_>>()
                        .concat()
                } else {
                    segments.concat()
                };
                let error = decode(&bytes, options(bdw, true), 1).unwrap_err();
                assert!(
                    error.message.contains("invalid spanned record sequence"),
                    "{error}"
                );
            }
            for flags in [vec![1], vec![1, 3]] {
                let segments: Vec<_> = flags.iter().map(|&flag| segment(flag, b"A")).collect();
                let bytes = if bdw {
                    segments
                        .iter()
                        .map(|s| block(std::slice::from_ref(s)))
                        .collect::<Vec<_>>()
                        .concat()
                } else {
                    segments.concat()
                };
                assert!(
                    decode(&bytes, options(bdw, true), 1)
                        .unwrap_err()
                        .message
                        .contains("unterminated spanned record")
                );
            }
            for header in [[0, 5, 4, 0, b'A'], [0, 5, 0, 1, b'A'], [0, 4, 0, 0, 0]] {
                let bytes = if bdw {
                    block(&[header.to_vec()])
                } else {
                    header.to_vec()
                };
                assert!(
                    decode(&bytes, options(bdw, true), 1)
                        .unwrap_err()
                        .message
                        .contains("invalid RDW/SDW")
                );
            }
        }
    }

    #[test]
    fn complete_and_fragmented_records_share_the_logical_length_limit() {
        for bdw in [false, true] {
            let options = Rdw {
                max_record_length: 9,
                ..options(bdw, true)
            };
            let segments = [segment(1, b"ABC"), segment(2, b"DEF")];
            let bytes = if bdw {
                segments
                    .iter()
                    .map(|s| block(std::slice::from_ref(s)))
                    .collect::<Vec<_>>()
                    .concat()
            } else {
                segments.concat()
            };
            let error = decode(&bytes, options.clone(), 2).unwrap_err();
            assert!(error.message.contains("logical record maximum length"));
            assert!(
                RdwWriter::new(options)
                    .write_record(&mut Vec::new(), b"ABCDEF")
                    .is_err()
            );
        }
        let mut reader = RdwReader::new(Cursor::new(segment(0, b"")), Rdw::default());
        assert!(reader.read_into(&mut Vec::new()).unwrap().is_some());
        let mut writer = RdwWriter::new(options(true, true));
        assert!(writer.write_record(&mut Vec::new(), b"").is_err());
        let mut writer = RdwWriter::new(options(true, false));
        assert!(writer.write_record(&mut Vec::new(), b"123456789").is_err());
    }
}
