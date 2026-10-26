use datafusion::arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
        Int64Array, Int64Builder, ListBuilder, StringArray, StructArray, Time64MicrosecondArray,
        TimestampNanosecondArray, UInt32Array, UInt64Array,
    },
    compute::{concat_batches, take},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::{
        ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder,
        arrow_writer::ArrowWriterOptions,
    },
    basic::Compression,
    file::properties::WriterProperties,
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    path::Path,
    process::{Command, Output},
    sync::Arc,
};

fn invoke(dir: &Path, config: &Value, command: &str, scan: bool, threads: usize) -> Output {
    fs::create_dir_all(dir.join("tmp")).unwrap();
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    process
        .arg(command)
        .arg(dir.join("job.json"))
        .args([
            "--threads",
            &threads.to_string(),
            "--memory-limit",
            "64MiB",
            "--temp-dir",
        ])
        .arg(dir.join("tmp"));
    if scan {
        process.arg("--scan");
    }
    process.output().unwrap()
}
fn ok(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn read(path: &Path) -> RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    concat_batches(&schema, &batches).unwrap()
}
fn write(path: &Path, batch: &RecordBatch, arrow_metadata: bool) {
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(997))
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let options = ArrowWriterOptions::new()
        .with_properties(properties)
        .with_skip_arrow_metadata(!arrow_metadata);
    let mut writer =
        ArrowWriter::try_new_with_options(File::create(path).unwrap(), batch.schema(), options)
            .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}
fn native_batch(rows: usize) -> RecordBatch {
    let arrays: Vec<(&str, ArrayRef)> = vec![
        ("n", Arc::new(Int64Array::from_iter_values(0..rows as i64))),
        (
            "text",
            Arc::new(StringArray::from_iter((0..rows).map(|n| match n % 3 {
                0 => None,
                1 => Some(""),
                _ => Some("日本,\"語\"\r\n"),
            }))),
        ),
        (
            "amount",
            Arc::new(
                Decimal128Array::from_iter(
                    (0..rows).map(|n| (n % 3 != 0).then_some(n as i128 * 1000 - 999999)),
                )
                .with_precision_and_scale(30, 9)
                .unwrap(),
            ),
        ),
        (
            "flag",
            Arc::new(BooleanArray::from_iter(
                (0..rows).map(|n| (n % 3 != 0).then_some(n % 2 == 0)),
            )),
        ),
        (
            "payload",
            Arc::new(BinaryArray::from_iter((0..rows).map(|n| match n % 3 {
                0 => None,
                1 => Some(&b""[..]),
                _ => Some(&b"\x00\xff"[..]),
            }))),
        ),
        (
            "date",
            Arc::new(Date32Array::from_iter_values(
                (0..rows).map(|n| n as i32 - 1000),
            )),
        ),
        (
            "time",
            Arc::new(Time64MicrosecondArray::from_iter_values(
                (0..rows).map(|n| n as i64 * 123456),
            )),
        ),
        (
            "timestamp",
            Arc::new(
                TimestampNanosecondArray::from_iter_values(
                    (0..rows).map(|n| n as i64 * 123456789 - 100000000000),
                )
                .with_timezone("Asia/Tokyo"),
            ),
        ),
        (
            "unsigned",
            Arc::new(UInt64Array::from_iter_values(
                (0..rows).map(|n| u64::MAX - n as u64),
            )),
        ),
        (
            "float",
            Arc::new(Float64Array::from_iter_values((0..rows).map(|n| {
                match n % 3 {
                    0 => f64::INFINITY,
                    1 => f64::NEG_INFINITY,
                    _ => -0.0,
                }
            }))),
        ),
    ];
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .map(|(name, a)| Field::new(*name, a.data_type().clone(), a.null_count() > 0))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, arrays.into_iter().map(|(_, a)| a).collect()).unwrap()
}
fn copy_job(sql: &str) -> Value {
    json!({"input":[{"id":"data","path":"in.parquet","format":"parquet"}],
        "output":[{"id":"result","path":"out.parquet","format":"parquet","mode":"overwrite","sql":sql}]})
}

#[test]
fn parquet_preserves_types_nulls_and_order_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let batch = native_batch(24001);
    write(&dir.path().join("in.parquet"), &batch, true);
    let job = copy_job("SELECT * FROM data");
    ok(invoke(dir.path(), &job, "check", false, 4));
    assert!(!dir.path().join("out.parquet").exists());
    let scan = ok(invoke(dir.path(), &job, "check", true, 4));
    assert!(String::from_utf8_lossy(&scan.stdout).contains("24001 records scanned"));
    let mut first_bytes = None;
    for threads in [1, 4] {
        ok(invoke(dir.path(), &job, "run", false, threads));
        assert_eq!(read(&dir.path().join("out.parquet")), batch);
        let bytes = fs::read(dir.path().join("out.parquet")).unwrap();
        if let Some(first) = &first_bytes {
            assert_eq!(
                &bytes, first,
                "Parquet bytes must not depend on the worker count"
            );
        }
        first_bytes = Some(bytes);
    }
    let job = copy_job("SELECT * FROM data ORDER BY n % 7 LIMIT 9000 OFFSET 8000");
    let mut order: Vec<u32> = (0..24001).collect();
    order.sort_by_key(|n| n % 7);
    let indices = UInt32Array::from(order[8000..17000].to_vec());
    let expected = RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|a| take(a.as_ref(), &indices, None).unwrap())
            .collect(),
    )
    .unwrap();
    for threads in [1, 4] {
        ok(invoke(dir.path(), &job, "run", false, threads));
        assert_eq!(read(&dir.path().join("out.parquet")), expected);
    }
}

#[test]
fn parquet_empty_files_compression_and_non_arrow_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "n",
        datafusion::arrow::datatypes::DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..23))]).unwrap();
    write(&dir.path().join("in.parquet"), &batch, false);
    for (name, codec) in [
        ("uncompressed", Compression::UNCOMPRESSED),
        ("snappy", Compression::SNAPPY),
        ("zstd", Compression::ZSTD(Default::default())),
        ("gzip", Compression::GZIP(Default::default())),
        ("brotli", Compression::BROTLI(Default::default())),
        ("lz4_raw", Compression::LZ4_RAW),
    ] {
        let mut job = copy_job("SELECT * FROM data");
        job["output"][0]["parquet"] = json!({"compression":name,"row_group_size":7});
        ok(invoke(dir.path(), &job, "run", false, 4));
        let b = ParquetRecordBatchReaderBuilder::try_new(
            File::open(dir.path().join("out.parquet")).unwrap(),
        )
        .unwrap();
        assert_eq!(b.metadata().num_row_groups(), 4);
        assert!(
            b.metadata()
                .row_groups()
                .iter()
                .all(|g| g.num_rows() <= 7 && g.column(0).compression() == codec)
        );
        assert_eq!(read(&dir.path().join("out.parquet")), batch);
        fs::copy(
            dir.path().join("out.parquet"),
            dir.path().join("in.parquet"),
        )
        .unwrap();
    }
    let job = copy_job("SELECT * FROM data WHERE false");
    ok(invoke(dir.path(), &job, "run", false, 4));
    assert_eq!(read(&dir.path().join("out.parquet")).num_rows(), 0);
    assert!(fs::metadata(dir.path().join("out.parquet")).unwrap().len() > 0);
    fs::copy(
        dir.path().join("out.parquet"),
        dir.path().join("in.parquet"),
    )
    .unwrap();
    let job = copy_job("SELECT * FROM data");
    let scan = ok(invoke(dir.path(), &job, "check", true, 4));
    assert!(String::from_utf8_lossy(&scan.stdout).contains("0 records scanned"));
    ok(invoke(dir.path(), &job, "run", false, 4));
    assert_eq!(read(&dir.path().join("out.parquet")).num_rows(), 0);
}

#[test]
fn nested_parquet_remains_native_and_can_be_unnested() {
    let dir = tempfile::tempdir().unwrap();
    let mut lists = ListBuilder::new(Int64Builder::new());
    lists.values().append_value(2);
    lists.values().append_null();
    lists.values().append_value(1);
    lists.append(true);
    lists.append(false);
    lists.append(true);
    let list: ArrayRef = Arc::new(lists.finish());
    let children = vec![(
        Arc::new(Field::new(
            "name",
            datafusion::arrow::datatypes::DataType::Utf8,
            true,
        )),
        Arc::new(StringArray::from(vec![Some("x"), None, Some("")])) as ArrayRef,
    )];
    let object: ArrayRef = Arc::new(StructArray::from(children));
    let schema = Arc::new(Schema::new(vec![
        Field::new("items", list.data_type().clone(), true),
        Field::new("object", object.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![list, object]).unwrap();
    write(&dir.path().join("in.parquet"), &batch, true);
    ok(invoke(
        dir.path(),
        &copy_job("SELECT * FROM data"),
        "run",
        false,
        4,
    ));
    assert_eq!(read(&dir.path().join("out.parquet")), batch);
    let job = copy_job("SELECT unnest(items) AS item FROM data");
    ok(invoke(dir.path(), &job, "run", false, 4));
    let actual = read(&dir.path().join("out.parquet"));
    assert_eq!(
        actual.column(0).as_ref(),
        &Int64Array::from(vec![Some(2), None, Some(1)])
    );
}

#[test]
fn csv_and_fixed_roundtrip_through_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let input = "name,amount\n日本,123.45\n\"\",0.00\n,-1.25\n";
    fs::write(dir.path().join("in.csv"), input).unwrap();
    let job = json!({"input":[{"id":"data","format":"csv","path":"in.csv","encoding":"utf-8",
        "fields":[{"name":"name","nullable":true},{"name":"amount","type":"decimal","precision":8,"scale":2}]}],
        "output":[{"id":"result","format":"parquet","path":"out.parquet","sql":"SELECT * FROM data"}]});
    ok(invoke(dir.path(), &job, "run", false, 4));
    let job = json!({"input":[{"id":"data","format":"parquet","path":"out.parquet"}],
        "output":[{"id":"result","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT * FROM data"}]});
    ok(invoke(dir.path(), &job, "run", false, 4));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        input
    );
    for format in ["fixed", "rdw"] {
        let mut bytes = Vec::new();
        for record in b"AA  0012BB  0034CC  0056".as_chunks::<8>().0 {
            if format == "rdw" {
                bytes.extend_from_slice(&[0, 12, 0, 0]);
            }
            bytes.extend_from_slice(record);
        }
        fs::write(dir.path().join("in.dat"), &bytes).unwrap();
        let mut common = json!({"format":format,"encoding":"utf-8",
            "fields":[{"name":"name","length":4},{"name":"n","type":"uzoned","zone":"ascii","precision":4,"length":4}]});
        if format == "fixed" {
            common["fixed"] = json!({"record_separator":"none","record_length":8});
        }
        let mut source = common.clone();
        source["id"] = json!("data");
        source["path"] = json!("in.dat");
        let job = json!({"input":[source],"output":[{"id":"result","format":"parquet","path":"physical.parquet","mode":"overwrite","sql":"SELECT * FROM data"}]});
        ok(invoke(dir.path(), &job, "run", false, 4));
        let mut output = common;
        output["id"] = json!("result");
        output["path"] = json!("out.dat");
        output["mode"] = json!("overwrite");
        output["sql"] = json!("SELECT * FROM data");
        let job = json!({"input":[{"id":"data","path":"physical.parquet","format":"parquet"}],"output":[output]});
        ok(invoke(dir.path(), &job, "run", false, 4));
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    }
}

#[test]
fn parquet_errors_do_not_replace_outputs() {
    let dir = tempfile::tempdir().unwrap();
    write(&dir.path().join("in.parquet"), &native_batch(23), true);
    fs::write(dir.path().join("out.parquet"), b"previous").unwrap();
    let base = copy_job("SELECT * FROM data");
    for (key, value) in [
        ("encoding", json!("utf-8")),
        ("fields", json!([])),
        ("csv", json!({})),
        ("fixed", json!({"record_separator":"none"})),
        ("rdw", json!({})),
        ("parquet", json!({"compression":"invalid"})),
        ("parquet", json!({"row_group_size":0})),
        ("parquet", json!({"unknown":true})),
    ] {
        let mut job = base.clone();
        job["output"][0][key] = value;
        let result = invoke(dir.path(), &job, "run", false, 4);
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            fs::read(dir.path().join("out.parquet")).unwrap(),
            b"previous"
        );
    }
    // Damage a data page while preserving the footer: planning is lazy, scan/run fail.
    let mut bytes = fs::read(dir.path().join("in.parquet")).unwrap();
    bytes[4..24].fill(0xff);
    fs::write(dir.path().join("in.parquet"), bytes).unwrap();
    ok(invoke(dir.path(), &base, "check", false, 4));
    for (command, scan) in [("check", true), ("run", false)] {
        let result = invoke(dir.path(), &base, command, scan, 4);
        assert_eq!(
            result.status.code(),
            Some(3),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            fs::read(dir.path().join("out.parquet")).unwrap(),
            b"previous"
        );
    }
    for bytes in [b"".as_slice(), b"not parquet", b"PAR1"] {
        fs::write(dir.path().join("in.parquet"), bytes).unwrap();
        let result = invoke(dir.path(), &base, "run", false, 4);
        assert_eq!(
            result.status.code(),
            Some(3),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
}
