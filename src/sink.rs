use crate::field::FieldType;
use crate::{
    config::{Dataset, Format, Job},
    encoding::TextWriter,
    error::{Error, Result},
    framing,
    layout::RecordLayout,
};
use datafusion::{
    arrow::{
        array::{Array, AsArray},
        datatypes::{DataType, Decimal128Type},
        record_batch::RecordBatch,
    },
    common::ScalarValue,
};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

enum Writer {
    Csv(TextWriter<BufWriter<File>>),
    Physical(BufWriter<File>),
}
pub struct OutputSink {
    dataset: Dataset,
    layout: RecordLayout,
    writer: Writer,
    pub rows: u64,
    indices: Vec<Vec<usize>>,
    delimiter: char,
    quote: char,
    line_ending: &'static str,
}
pub fn csv_cell(value: Option<&str>, always: bool, delimiter: char, quote: char) -> String {
    let Some(s) = value else {
        return String::new();
    };
    if always || s.is_empty() || s.contains([delimiter, quote, '\r', '\n']) {
        format!(
            "{quote}{}{quote}",
            s.replace(quote, &format!("{quote}{quote}"))
        )
    } else {
        s.into()
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
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(path)
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
            Writer::Csv(TextWriter::new(BufWriter::new(file), layout.encoding))
        } else {
            Writer::Physical(BufWriter::new(file))
        };
        let indices = layout
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
        let mut sink = Self {
            dataset: dataset.clone(),
            layout: layout.clone(),
            writer,
            rows: 0,
            indices,
            delimiter,
            quote,
            line_ending,
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
        let csv = self.dataset.csv.clone().unwrap_or_default();
        let mut body = Vec::with_capacity(self.layout.length);
        let mut bytes = Vec::new();
        let mut values = Vec::new();
        for row in 0..batch.num_rows() {
            body.clear();
            body.resize(self.layout.length, 0);
            let mut cells = vec![];
            for (f, indices) in self.layout.fields.iter().zip(&self.indices) {
                let result = (|| -> Result<()> {
                    // Borrow strings from Arrow rather than allocating an owned ScalarValue.
                    if self.dataset.format != Format::Csv && f.kind == FieldType::String {
                        let column = batch.column(indices[0]);
                        if !column.is_null(row) {
                            let text = match column.data_type() {
                                DataType::Utf8 => Some(column.as_string::<i32>().value(row)),
                                DataType::LargeUtf8 => Some(column.as_string::<i64>().value(row)),
                                DataType::Utf8View => Some(column.as_string_view().value(row)),
                                _ => None,
                            };
                            if let Some(text) = text {
                                f.encode_string_into(text, &mut bytes)?;
                                place_field(&mut body, f.offset, &bytes);
                                return Ok(());
                            }
                        }
                    }
                    if self.dataset.format != Format::Csv
                        && f.kind.decimal()
                        && f.kind != FieldType::Decimal
                    {
                        let column = batch.column(indices[0]);
                        if let DataType::Decimal128(_, scale) = column.data_type() {
                            let array = column.as_primitive::<Decimal128Type>();
                            if !array.is_null(row) {
                                f.encode_decimal_into(array.value(row), *scale, &mut bytes)?;
                                place_field(&mut body, f.offset, &bytes);
                                return Ok(());
                            }
                        }
                    }
                    values.clear();
                    for i in indices {
                        let column = batch.column(*i);
                        let value = if let DataType::Decimal128(p, s) = column.data_type() {
                            let array = column.as_primitive::<Decimal128Type>();
                            ScalarValue::Decimal128(
                                (!array.is_null(row)).then(|| array.value(row)),
                                *p,
                                *s,
                            )
                        } else {
                            ScalarValue::try_from_array(column, row).map_err(Error::from)?
                        };
                        values.push(value);
                    }
                    if self.dataset.format == Format::Csv {
                        let v = &values[0];
                        if v.is_null() {
                            if f.spec.nullable != Some(true) {
                                return Err(Error::output("NULL in non-nullable CSV output"));
                            }
                            cells.push(String::new());
                        } else {
                            let text = f.format_text(v)?;
                            cells.push(csv_cell(
                                Some(&text),
                                f.spec.quote_style.as_deref().or(csv.quote_style.as_deref())
                                    == Some("always"),
                                self.delimiter,
                                self.quote,
                            ));
                        }
                    } else {
                        f.encode_into(&values, self.dataset.format, &mut bytes)?;
                        place_field(&mut body, f.offset, &bytes);
                    }
                    Ok(())
                })();
                result.map_err(|e| {
                    e.context(format!(
                        "output {} record {} column {} expected {:?} width {:?}",
                        self.dataset.id,
                        self.rows + 1,
                        f.spec.name.as_deref().unwrap_or("FILLER"),
                        f.kind,
                        f.length
                    ))
                })?;
            }
            match &mut self.writer {
                Writer::Csv(w) => {
                    w.text(&(cells.join(&csv.delimiter) + self.line_ending))?;
                }
                Writer::Physical(w) => {
                    if self.dataset.format == Format::Rdw {
                        framing::write_rdw(w, &body, self.layout.max_record_length)?;
                    } else {
                        w.write_all(&body)
                            .and_then(|_| w.write_all(&self.layout.separator))
                            .map_err(|e| Error::output(e.to_string()))?;
                    }
                }
            }
            self.rows += 1;
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<u64> {
        match &mut self.writer {
            Writer::Csv(w) => {
                w.finish()?;
                w.inner
                    .get_ref()
                    .sync_all()
                    .map_err(|e| Error::output(e.to_string()))?;
            }
            Writer::Physical(w) => {
                w.flush()
                    .and_then(|_| w.get_ref().sync_all())
                    .map_err(|e| Error::output(e.to_string()))?;
            }
        }
        Ok(self.rows)
    }
}

fn canonical_destination(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .map_err(|e| Error::config(e.to_string()).context(parent.display()))?;
    if !parent.is_dir() {
        return Err(Error::config("output parent must be an existing directory"));
    }
    Ok(parent.join(
        path.file_name()
            .ok_or_else(|| Error::config("invalid output filename"))?,
    ))
}
fn ancestor(path: &Path) -> Result<PathBuf> {
    let mut p = path;
    loop {
        if p.exists() {
            return fs::canonicalize(p).map_err(|e| Error::config(e.to_string()));
        }
        p = p
            .parent()
            .ok_or_else(|| Error::config("temporary directory has no existing ancestor"))?;
    }
}
#[cfg(target_os = "linux")]
fn mount(path: &Path) -> Result<(u64, u64)> {
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
    let meta = fs::metadata(path).map_err(|e| Error::config(e.to_string()))?;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| Error::config("NUL in path"))?;
    let mut stat = std::mem::MaybeUninit::<libc::statx>::zeroed();
    // statx follows the same resolved path that rename will use; mount IDs detect bind mounts too.
    let rc = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            c.as_ptr(),
            0,
            libc::STATX_MNT_ID,
            stat.as_mut_ptr(),
        )
    };
    if rc != 0 {
        return Err(Error::config(std::io::Error::last_os_error().to_string()));
    }
    let stat = unsafe { stat.assume_init() };
    Ok((meta.dev(), stat.stx_mnt_id))
}
#[cfg(not(target_os = "linux"))]
fn mount(_: &Path) -> Result<(u64, u64)> {
    Err(Error::config(
        "atomic publication is currently supported on Linux only",
    ))
}
#[cfg(unix)]
fn identity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).ok().map(|m| (m.dev(), m.ino()))
}
#[cfg(not(unix))]
fn identity(_: &Path) -> Option<(u64, u64)> {
    None
}
pub fn preflight(job: &Job, temp: &Path) -> Result<()> {
    let temporary = ancestor(temp)?;
    if !temporary.is_dir() {
        return Err(Error::config("temporary directory must be a directory"));
    }
    let fs = mount(&temporary)?;
    let mut paths = BTreeSet::new();
    let mut inodes = BTreeSet::new();
    for input in &job.input {
        let path = fs::canonicalize(&input.path)
            .map_err(|e| Error::config(e.to_string()).context(input.path.display()))?;
        if !path.is_file() {
            return Err(Error::config("input must be a regular file"));
        }
        paths.insert(path);
        if let Some(id) = identity(&input.path) {
            inodes.insert(id);
        }
    }
    for output in &job.output {
        let dest = canonical_destination(&output.path)?;
        let resolved = if dest.exists() {
            fs::canonicalize(&dest).map_err(|e| Error::config(e.to_string()))?
        } else {
            dest.clone()
        };
        if !paths.insert(resolved) || identity(&dest).is_some_and(|id| !inodes.insert(id)) {
            return Err(Error::config(format!(
                "input/output path collision: {}",
                dest.display()
            )));
        }
        if fs::symlink_metadata(&dest).is_ok() {
            if !dest.is_file() {
                return Err(Error::config("output must be a regular file"));
            }
            if output.mode.as_deref() != Some("overwrite") {
                return Err(Error::config(format!(
                    "output already exists: {}",
                    dest.display()
                )));
            }
        }
        if mount(dest.parent().unwrap())? != fs {
            return Err(Error::config(
                "temporary directory and all output parents must be on the same filesystem and mount",
            ));
        }
    }
    Ok(())
}
fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::output(e.to_string()).context(path.display()))
}
fn publish_one(source: &Path, dest: &Path, overwrite: bool) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let source_c = std::ffi::CString::new(source.as_os_str().as_bytes())
            .map_err(|_| Error::output("invalid path"))?;
        let dest_c = std::ffi::CString::new(dest.as_os_str().as_bytes())
            .map_err(|_| Error::output("invalid path"))?;
        let rc = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source_c.as_ptr(),
                libc::AT_FDCWD,
                dest_c.as_ptr(),
                if overwrite { 0 } else { libc::RENAME_NOREPLACE },
            )
        };
        if rc != 0 {
            return Err(Error::output(std::io::Error::last_os_error().to_string()));
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (source, dest, overwrite);
        Err(Error::output("atomic publication unsupported on this OS"))
    }
}
pub fn publish(job: &Job, staged: &[PathBuf]) -> Result<()> {
    let mut published = vec![];
    for (i, (output, source)) in job.output.iter().zip(staged).enumerate() {
        let result = publish_one(
            source,
            &output.path,
            output.mode.as_deref() == Some("overwrite"),
        );
        if let Err(e) = result {
            return Err(e.context(format!(
                "publication failed; published={published:?}; unpublished={:?}",
                job.output[i..]
                    .iter()
                    .map(|d| (&d.id, &d.path))
                    .collect::<Vec<_>>()
            )));
        }
        published.push((&output.id, &output.path));
        if let Err(e) =
            sync_dir(source.parent().unwrap()).and_then(|_| sync_dir(output.path.parent().unwrap()))
        {
            return Err(e.context(format!(
                "publication sync failed; published={published:?}; unpublished={:?}",
                job.output[i + 1..]
                    .iter()
                    .map(|d| (&d.id, &d.path))
                    .collect::<Vec<_>>()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let layout = RecordLayout::compile(&dataset, true, Some(schema.clone())).unwrap();
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
            let layout = RecordLayout::compile(&dataset, true, Some(schema.clone())).unwrap();
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
    #[test]
    fn no_replace_race() {
        let d = tempfile::tempdir().unwrap();
        let a = d.path().join("a");
        let b = d.path().join("b");
        fs::write(&a, b"new").unwrap();
        fs::write(&b, b"old").unwrap();
        assert!(publish_one(&a, &b, false).is_err());
        assert_eq!(fs::read(&b).unwrap(), b"old");
        publish_one(&a, &b, true).unwrap();
        assert_eq!(fs::read(&b).unwrap(), b"new");
    }
    #[test]
    fn partial_publication_reports_exact_state() {
        let d = tempfile::tempdir().unwrap();
        let config = serde_json::json!({"input":[{"id":"data","format":"csv","encoding":"utf-8","path":"in.csv","fields":[{"name":"value"}]}],"process":[],"output":[{"id":"first","format":"csv","encoding":"utf-8","sql":"SELECT value FROM data","path":"first.csv"},{"id":"second","format":"csv","encoding":"utf-8","sql":"SELECT value FROM data","path":"second.csv"}]});
        let jobfile = d.path().join("job.yml");
        fs::write(&jobfile, config.to_string()).unwrap();
        let job = crate::config::load(&jobfile).unwrap();
        let a = d.path().join("a.tmp");
        let b = d.path().join("b.tmp");
        fs::write(&a, b"first").unwrap();
        fs::write(&b, b"second").unwrap();
        fs::create_dir(d.path().join("second.csv")).unwrap();
        let error = publish(&job, &[a, b.clone()]).unwrap_err();
        assert_eq!(error.code, 5);
        assert_eq!(fs::read(d.path().join("first.csv")).unwrap(), b"first");
        assert!(b.exists());
        assert!(
            error.message.contains("published=[(\"first\""),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("unpublished=[(\"second\""),
            "{}",
            error.message
        );
    }
}
