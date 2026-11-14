mod csv;
pub(crate) mod lua;
mod physical;
use crate::{
    config::{Dataset, Format},
    encoding::TextWriter,
    error::{Error, Result},
    framing,
    layout::RecordLayout,
};
pub(crate) use csv::CsvEncoder;
use datafusion::{
    arrow::{
        array::{Array, AsArray},
        datatypes::{DataType, Decimal128Type},
        record_batch::RecordBatch,
    },
    common::ScalarValue,
};
pub(crate) use physical::PhysicalEncoder;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

enum Writer {
    Csv {
        writer: TextWriter<crate::compression::Output>,
        encoder: Arc<CsvEncoder>,
        buffer: String,
    },
    Physical {
        writer: crate::compression::Output,
        encoder: Arc<PhysicalEncoder>,
        rdw: Option<framing::rdw::RdwWriter>,
    },
    Parquet(Box<parquet::arrow::ArrowWriter<BufWriter<File>>>),
    Lua(Box<datafusion::arrow::ipc::writer::FileWriter<BufWriter<File>>>),
}
pub struct OutputSink {
    dataset: Dataset,
    checks: crate::check::Checks,
    writer: Writer,
    pub rows: u64,
}
pub fn csv_cell(value: Option<&str>, always: bool, delimiter: char, quote: char) -> String {
    let mut out = String::new();
    if let Some(s) = value {
        append_csv_cell(&mut out, s, always, delimiter, quote);
    }
    out
}
fn append_csv_cell(out: &mut String, s: &str, always: bool, delimiter: char, quote: char) {
    let quoted = always || s.is_empty() || s.contains([delimiter, quote, '\r', '\n']);
    append_csv_value(out, s, quoted, quote);
}
fn append_csv_value(out: &mut String, s: &str, quoted: bool, quote: char) {
    if quoted {
        out.push(quote);
        let mut rest = s;
        for (position, _) in s.match_indices(quote) {
            let end = position + quote.len_utf8();
            let consumed = s.len() - rest.len();
            out.push_str(&rest[..end - consumed]);
            out.push(quote);
            rest = &s[end..];
        }
        out.push_str(rest);
        out.push(quote);
    } else {
        out.push_str(s);
    }
}
fn place_field(body: &mut Vec<u8>, offset: usize, bytes: &[u8]) {
    let end = offset + bytes.len();
    body.resize(body.len().max(end), 0);
    body[offset..end].copy_from_slice(bytes);
}
impl OutputSink {
    pub fn create(
        dataset: &Dataset,
        layout: &RecordLayout,
        path: &Path,
        result_schema: &datafusion::arrow::datatypes::Schema,
    ) -> Result<Self> {
        if dataset.format == Format::Apache {
            return Err(Error::config("apache is an input-only format"));
        }
        let file = crate::filesystem::create_staging_file(path)
            .map_err(|e| Error::output(e.to_string()).context(path.display()))?;
        let csv = dataset.csv.clone().unwrap_or_default();
        let delimiter = csv.delimiter.chars().next().unwrap();
        let quote = csv.quote.chars().next().unwrap();
        let line_ending = if csv.line_ending.as_deref() == Some("crlf") {
            "\r\n"
        } else {
            "\n"
        };
        let indices: Vec<Vec<usize>> = layout
            .fields
            .iter()
            .map(|f| {
                f.names()
                    .iter()
                    .map(|n| {
                        result_schema
                            .index_of(n)
                            .map_err(|e| Error::config(e.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<_>>()?;
        let writer = match dataset.format {
            Format::Apache => unreachable!("input-only format rejected above"),
            Format::Csv => Writer::Csv {
                writer: TextWriter::new(
                    crate::compression::Output::new(file, dataset.compression)
                        .map_err(|e| Error::output(e.to_string()).context(&dataset.id))?,
                    layout.encoding,
                ),
                encoder: Arc::new(CsvEncoder {
                    id: dataset.id.clone(),
                    layout: layout.clone(),
                    indices,
                    delimiter,
                    quote,
                    line_ending,
                    quote_always: layout
                        .fields
                        .iter()
                        .map(|f| {
                            f.spec.quote_style.as_deref().or(csv.quote_style.as_deref())
                                == Some("always")
                        })
                        .collect(),
                }),
                buffer: String::new(),
            },
            Format::Fixed | Format::Lines | Format::Rdw => Writer::Physical {
                writer: crate::compression::Output::new(file, dataset.compression)
                    .map_err(|e| Error::output(e.to_string()).context(&dataset.id))?,
                encoder: Arc::new(PhysicalEncoder::new(dataset, layout, &indices)),
                rdw: (dataset.format == Format::Rdw)
                    .then(|| framing::rdw::RdwWriter::new(dataset.rdw.clone().unwrap_or_default())),
            },
            Format::Lua => Writer::Lua(Box::new(
                datafusion::arrow::ipc::writer::FileWriter::try_new(
                    BufWriter::new(file),
                    result_schema,
                )
                .map_err(|e| Error::output(e.to_string()))?,
            )),
            Format::Parquet => Writer::Parquet(Box::new(
                parquet::arrow::ArrowWriter::try_new(
                    BufWriter::new(file),
                    Arc::new(result_schema.clone()),
                    Some(crate::parquet_io::properties(dataset)?),
                )
                .map_err(|e| Error::output(e.to_string()).context(&dataset.id))?,
            )),
        };
        let mut sink = Self {
            dataset: dataset.clone(),
            checks: layout.checks.clone(),
            writer,
            rows: 0,
        };
        if dataset.format == Format::Csv && csv.header {
            let header = layout
                .fields
                .iter()
                .map(|f| {
                    csv_cell(
                        f.spec.name.as_deref(),
                        f.spec.quote_style.as_deref().or(csv.quote_style.as_deref())
                            == Some("always"),
                        delimiter,
                        quote,
                    )
                })
                .collect::<Vec<_>>()
                .join(&csv.delimiter)
                + line_ending;
            if header.len() > framing::MAX_RECORD {
                return Err(Error::output("CSV header exceeds 16 MiB").context(&dataset.id));
            }
            if let Writer::Csv { writer, .. } = &mut sink.writer {
                writer.text(&header)?;
            }
        }
        Ok(sink)
    }
    pub fn batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.checks.is_empty()
            || matches!(self.writer, Writer::Csv { .. } | Writer::Physical { .. })
        {
            return self.write_batch(batch);
        }
        // A check and the value conversion for one record finish before the
        // next record's messages can be emitted.
        let checked = self.checks.project(&batch)?;
        for row in 0..batch.num_rows() {
            let record = batch.slice(row, 1);
            self.checks.check_row(&checked, row, self.rows + 1)?;
            self.write_batch(record)?;
        }
        Ok(())
    }
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        match &mut self.writer {
            Writer::Lua(writer) => {
                writer
                    .write(&batch)
                    .map_err(|e| Error::output(e.to_string()).context(&self.dataset.id))?;
            }
            Writer::Parquet(writer) => {
                writer.write(&batch).map_err(|e| {
                    Error::output(e.to_string()).context(format!(
                        "output {} record {}",
                        self.dataset.id,
                        self.rows + 1
                    ))
                })?;
                if writer.memory_size() >= framing::MAX_RECORD {
                    writer
                        .flush()
                        .map_err(|e| Error::output(e.to_string()).context(&self.dataset.id))?;
                }
            }
            Writer::Physical {
                writer,
                encoder,
                rdw,
            } => {
                return encoder.write_batch(&batch, writer, &mut self.rows, None, rdw.as_mut());
            }
            Writer::Csv {
                writer,
                encoder,
                buffer,
            } => {
                let start_row = self.rows;
                let mut offset = 0;
                while offset < batch.num_rows() {
                    let next =
                        encoder.encode(&batch, offset, start_row, buffer, 64 * 1024, None)?;
                    writer.text(buffer)?;
                    buffer.clear();
                    self.rows += (next - offset) as u64;
                    offset = next;
                }
                return Ok(());
            }
        }
        self.rows += batch.num_rows() as u64;
        Ok(())
    }
    pub(crate) fn csv_encoder(&self) -> Option<Arc<CsvEncoder>> {
        match &self.writer {
            Writer::Csv { encoder, .. }
                if self.checks.is_empty() && crate::parallel::worthwhile(&encoder.layout) =>
            {
                Some(encoder.clone())
            }
            _ => None,
        }
    }
    pub(crate) fn csv_writer_uses_cpu(&self) -> bool {
        self.is_compressed()
            || matches!(&self.writer,
            Writer::Csv { encoder, .. } if !encoder.layout.encoding.is_utf8())
    }
    pub(crate) fn write_csv_text(&mut self, text: &str, rows: usize) -> Result<()> {
        let Writer::Csv { writer, .. } = &mut self.writer else {
            unreachable!()
        };
        writer.text(text)?;
        self.rows += rows as u64;
        Ok(())
    }
    pub(crate) fn fixed_encoder(&self) -> Option<Arc<PhysicalEncoder>> {
        match &self.writer {
            Writer::Physical { encoder, .. }
                if self.dataset.format == Format::Fixed
                    && self.checks.is_empty()
                    && encoder.worthwhile() =>
            {
                Some(encoder.clone())
            }
            _ => None,
        }
    }
    pub(crate) fn is_compressed(&self) -> bool {
        self.dataset.compression.is_some()
    }
    pub(crate) fn write_fixed_bytes(&mut self, bytes: &[u8], rows: usize) -> Result<()> {
        let Writer::Physical { writer, .. } = &mut self.writer else {
            unreachable!()
        };
        writer
            .write_all(bytes)
            .map_err(|e| Error::output(e.to_string()))?;
        self.rows += rows as u64;
        Ok(())
    }
    pub fn finish(mut self) -> Result<u64> {
        if let Writer::Physical {
            writer,
            rdw: Some(rdw),
            ..
        } = &mut self.writer
        {
            rdw.finish(writer)
                .map_err(|e| e.context(format!("output {}", self.dataset.id)))?;
        }
        match self.writer {
            Writer::Lua(mut writer) => {
                writer.finish().map_err(|e| Error::output(e.to_string()))?;
                writer
                    .get_mut()
                    .flush()
                    .and_then(|_| writer.get_ref().get_ref().sync_all())
                    .map_err(|e| Error::output(e.to_string()).context(&self.dataset.id))?;
            }
            Writer::Parquet(mut writer) => {
                writer
                    .finish()
                    .map_err(|e| Error::output(e.to_string()).context(&self.dataset.id))?;
                writer
                    .inner_mut()
                    .flush()
                    .and_then(|_| writer.inner().get_ref().sync_all())
                    .map_err(|e| Error::output(e.to_string()).context(&self.dataset.id))?;
            }
            Writer::Csv { writer: mut w, .. } => {
                w.finish()?;
                w.inner.finish().map_err(|e| Error::output(e.to_string()))?;
            }
            Writer::Physical { writer: w, .. } => {
                w.finish().map_err(|e| Error::output(e.to_string()))?;
            }
        }
        Ok(self.rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[test]
    fn direct_decimal_output_keeps_scale_precision_and_null_checks() {
        use datafusion::arrow::{
            array::Decimal128Array,
            datatypes::{Field, Schema},
        };
        use std::sync::Arc;
        for (values, marker, expected) in [
            (
                vec![Some(1230), None, Some(-1230)],
                None,
                Ok("123c000c123d"),
            ),
            (
                vec![Some(123)],
                None,
                Err("scale would lose nonzero digits"),
            ),
            (vec![Some(10000)], None, Err("precision overflow")),
            (
                vec![Some(1230)],
                Some("123c"),
                Err("collides with NULL rule"),
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut field = serde_json::json!({"name":"n","type":"packed","length":2,"precision":3,"scale":1,"nullable":true});
            if let Some(marker) = marker {
                field["null_bytes"] = serde_json::json!(marker);
            }
            let dataset: Dataset = serde_json::from_value(serde_json::json!({"id":"result","path":"unused.dat","format":"fixed","encoding":"utf-8","sql":"SELECT n FROM data","fixed":{"record_separator":"none"},"fields":[field]})).unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "n",
                DataType::Decimal128(6, 2),
                true,
            )]));
            let layout = RecordLayout::output(&dataset, schema.clone()).unwrap();
            let path = dir.path().join("output");
            let mut sink = OutputSink::create(&dataset, &layout, &path, &schema).unwrap();
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(6, 2)
                .unwrap();
            let result = sink.batch(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap());
            match expected {
                Ok(hex) => {
                    result.unwrap();
                    sink.finish().unwrap();
                    assert_eq!(fs::read(path).unwrap(), hex::decode(hex).unwrap());
                }
                Err(message) => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code, 5);
                    assert!(error.message.contains(message), "{error}");
                }
            }
        }
    }
    #[test]
    fn borrowed_arrow_strings_keep_padding_nulls_and_collision_checks() {
        use datafusion::arrow::{
            array::{ArrayRef, LargeStringArray, StringArray, StringViewArray},
            datatypes::{Field, Schema},
        };
        use std::sync::Arc;
        let values = vec![Some("AB"), None, Some(""), Some("NULL")];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values)),
        ];
        for array in arrays {
            let d = tempfile::tempdir().unwrap();
            let schema = Arc::new(Schema::new(vec![Field::new(
                "v",
                array.data_type().clone(),
                true,
            )]));
            let dataset: Dataset = serde_json::from_value(serde_json::json!({"id":"result","path":"unused.dat","format":"fixed","encoding":"utf-8","sql":"SELECT v FROM data","fixed":{"record_separator":"none"},"fields":[{"name":"v","length":4,"nullable":true,"null_if":["NULL"]}]})).unwrap();
            let layout = RecordLayout::output(&dataset, schema.clone()).unwrap();
            let path = d.path().join("out.dat");
            let mut sink = OutputSink::create(&dataset, &layout, &path, &schema).unwrap();
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            sink.batch(batch.slice(0, 3)).unwrap();
            let error = sink.batch(batch.slice(3, 1)).unwrap_err();
            assert!(error.message.contains("collides with NULL rule"));
            sink.finish().unwrap();
            assert_eq!(fs::read(path).unwrap(), b"AB          ");
        }
    }
    #[test]
    fn quotes() {
        assert_eq!(csv_cell(None, true, ',', '"'), "");
        assert_eq!(csv_cell(Some(""), false, ',', '"'), "\"\"");
        assert_eq!(csv_cell(Some("a\"b"), false, ',', '"'), "\"a\"\"b\"");
    }
}
