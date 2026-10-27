mod csv;
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
    Csv(TextWriter<crate::compression::Output>),
    Physical(crate::compression::Output),
    Parquet(Box<parquet::arrow::ArrowWriter<BufWriter<File>>>),
}
pub struct OutputSink {
    dataset: Dataset,
    layout: RecordLayout,
    writer: Writer,
    pub rows: u64,
    physical: Option<Arc<PhysicalEncoder>>,
    csv: Option<Arc<CsvEncoder>>,
    csv_buffer: String,
}
pub fn csv_cell(value: Option<&str>, always: bool, delimiter: char, quote: char) -> String {
    let mut out = String::new();
    if let Some(s) = value {
        append_csv_cell(&mut out, s, always, delimiter, quote);
    }
    out
}
fn append_csv_cell(out: &mut String, s: &str, always: bool, delimiter: char, quote: char) {
    if always || s.is_empty() || s.contains([delimiter, quote, '\r', '\n']) {
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
        let writer = if dataset.format == Format::Csv {
            Writer::Csv(TextWriter::new(
                crate::compression::Output::new(file, dataset.compression)
                    .map_err(|e| Error::output(e.to_string()).context(&dataset.id))?,
                layout.encoding,
            ))
        } else if dataset.format == Format::Parquet {
            Writer::Parquet(Box::new(
                parquet::arrow::ArrowWriter::try_new(
                    BufWriter::new(file),
                    Arc::new(result_schema.clone()),
                    Some(crate::parquet_io::properties(dataset)?),
                )
                .map_err(|e| Error::output(e.to_string()).context(&dataset.id))?,
            ))
        } else {
            Writer::Physical(
                crate::compression::Output::new(file, dataset.compression)
                    .map_err(|e| Error::output(e.to_string()).context(&dataset.id))?,
            )
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
        let physical = matches!(dataset.format, Format::Fixed | Format::Rdw)
            .then(|| Arc::new(PhysicalEncoder::new(dataset, layout, &indices)));
        let quote_always = layout
            .fields
            .iter()
            .map(|f| f.spec.quote_style.as_deref().or(csv.quote_style.as_deref()) == Some("always"))
            .collect();
        let encoder = (dataset.format == Format::Csv).then(|| {
            Arc::new(CsvEncoder {
                id: dataset.id.clone(),
                layout: layout.clone(),
                indices,
                delimiter,
                quote,
                line_ending,
                quote_always,
            })
        });
        let mut sink = Self {
            dataset: dataset.clone(),
            layout: layout.clone(),
            writer,
            rows: 0,
            physical,
            csv: encoder,
            csv_buffer: String::new(),
        };
        if dataset.format == Format::Csv && csv.header {
            let header = sink
                .layout
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
            if let Writer::Csv(w) = &mut sink.writer {
                w.text(&header)?;
            }
        }
        Ok(sink)
    }
    pub fn batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.layout.checks.is_empty() {
            return self.write_batch(batch);
        }
        // A check and the value conversion for one record finish before the
        // next record's messages can be emitted.
        for row in 0..batch.num_rows() {
            let record = batch.slice(row, 1);
            self.layout.checks.batch(&record, self.rows)?;
            self.write_batch(record)?;
        }
        Ok(())
    }
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Writer::Parquet(writer) = &mut self.writer {
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
            self.rows += batch.num_rows() as u64;
            return Ok(());
        }
        if let (Some(encoder), Writer::Physical(writer)) = (&self.physical, &mut self.writer) {
            return encoder.write_batch(&batch, writer, &mut self.rows, None);
        }
        let encoder = self.csv.as_ref().expect("CSV encoder");
        let Writer::Csv(writer) = &mut self.writer else {
            unreachable!()
        };
        let start_row = self.rows;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let next = encoder.encode(
                &batch,
                offset,
                start_row,
                &mut self.csv_buffer,
                64 * 1024,
                None,
            )?;
            writer.text(&self.csv_buffer)?;
            self.csv_buffer.clear();
            self.rows += (next - offset) as u64;
            offset = next;
        }
        Ok(())
    }
    pub(crate) fn csv_encoder(&self) -> Option<Arc<CsvEncoder>> {
        self.csv
            .as_ref()
            .filter(|_| self.layout.checks.is_empty() && crate::parallel::worthwhile(&self.layout))
            .cloned()
    }
    pub(crate) fn csv_writer_uses_cpu(&self) -> bool {
        self.is_compressed() || self.layout.encoding.0 != encoding_rs::UTF_8
    }
    pub(crate) fn write_csv_text(&mut self, text: &str, rows: usize) -> Result<()> {
        let Writer::Csv(writer) = &mut self.writer else {
            unreachable!()
        };
        writer.text(text)?;
        self.rows += rows as u64;
        Ok(())
    }
    pub(crate) fn fixed_encoder(&self) -> Option<Arc<PhysicalEncoder>> {
        (self.dataset.format == Format::Fixed
            && self.layout.checks.is_empty()
            && crate::parallel::worthwhile(&self.layout))
        .then(|| self.physical.as_ref().unwrap().clone())
    }
    pub(crate) fn is_compressed(&self) -> bool {
        self.dataset.compression.is_some()
    }
    pub(crate) fn write_fixed_bytes(&mut self, bytes: &[u8], rows: usize) -> Result<()> {
        let Writer::Physical(writer) = &mut self.writer else {
            unreachable!()
        };
        writer
            .write_all(bytes)
            .map_err(|e| Error::output(e.to_string()))?;
        self.rows += rows as u64;
        Ok(())
    }
    pub fn finish(self) -> Result<u64> {
        match self.writer {
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
            Writer::Csv(mut w) => {
                w.finish()?;
                w.inner.finish().map_err(|e| Error::output(e.to_string()))?;
            }
            Writer::Physical(w) => {
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
