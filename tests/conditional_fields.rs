use datafusion::{arrow::datatypes::Schema, common::ScalarValue};
use dxtran::{config::Dataset, layout::RecordLayout, source::InputReader};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
    sync::Arc,
};

fn fields() -> Value {
    json!([
        {"name":"record_type","length":1},
        {"name":"label","offset":1,"length":4,"when":"record_type IN ('H', 'T')"},
        {"name":"amount","offset":1,"length":4,"type":"decimal","precision":4,
         "when":"record_type = 'D' AND length(record_type) = 1"}
    ])
}
fn physical(fields: Value, format: &str) -> Dataset {
    let mut value =
        json!({"id":"data","path":"unused","format":format,"encoding":"utf-8","fields":fields});
    if format == "fixed" {
        value["fixed"] = json!({"record_separator":"none"});
    }
    serde_json::from_value(value).unwrap()
}
fn invoke(dir: &Path, command: &str, scan: bool, threads: usize) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_dxt"));
    child
        .arg(command)
        .arg(dir.join("job.json"))
        .args([
            "--threads",
            &threads.to_string(),
            "--memory-limit",
            "128MiB",
            "--temp-dir",
        ])
        .arg(dir.join("tmp"));
    if scan {
        child.arg("--scan");
    }
    child.output().unwrap()
}
fn ok(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn job(dir: &Path, input: Value, output: Value) {
    fs::create_dir_all(dir.join("tmp")).unwrap();
    fs::write(
        dir.join("job.json"),
        json!({"input":[input],"output":output}).to_string(),
    )
    .unwrap();
}
fn input(fields: Value, format: &str) -> Value {
    let mut value =
        json!({"id":"data","path":"in.dat","format":format,"encoding":"utf-8","fields":fields});
    if format == "fixed" {
        value["fixed"] = json!({"record_separator":"none"});
    }
    value
}
fn output(fields: Value, format: &str) -> Value {
    let mut value = input(fields, format);
    value["id"] = json!("result");
    value["path"] = json!("out.dat");
    value["mode"] = json!("overwrite");
    value["sql"] = json!("SELECT * FROM data");
    value
}

#[test]
fn sql_predicates_decode_only_active_fields_and_publish_nullable_union_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("in.dat");
    // Three 5-byte records; the header payload is not a valid decimal.
    fs::write(&path, b"HABCDD0012TEND ").unwrap();
    let mut dataset = physical(fields(), "fixed");
    dataset.path = path;
    let layout = RecordLayout::input(&dataset).unwrap();
    assert!(!layout.schema.field(0).is_nullable());
    assert!(layout.schema.field(1).is_nullable());
    assert!(layout.schema.field(2).is_nullable());
    let mut reader = InputReader::open(&dataset, &layout).unwrap();
    let header = reader.next_row().unwrap().unwrap();
    assert_eq!(header[1], ScalarValue::Utf8(Some("ABCD".into())));
    assert!(header[2].is_null());
    let detail = reader.next_row().unwrap().unwrap();
    assert!(detail[1].is_null());
    assert_eq!(detail[2], ScalarValue::Decimal128(Some(12), 4, 0));
    assert_eq!(
        reader.next_row().unwrap().unwrap()[1],
        ScalarValue::Utf8(Some("END".into()))
    );
    assert!(reader.next_row().unwrap().is_none());
}

#[test]
fn conditional_fixed_io_preserves_bytes_across_batches_and_threads() {
    let dir = tempfile::tempdir().unwrap();
    let bytes: Vec<_> = (0..9001)
        .flat_map(|i| if i % 2 == 0 { *b"HABCD" } else { *b"D0012" })
        .collect();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    job(
        dir.path(),
        input(fields(), "fixed"),
        json!([output(fields(), "fixed")]),
    );
    for threads in [1, 4] {
        ok(invoke(dir.path(), "check", true, threads));
        ok(invoke(dir.path(), "run", false, threads));
        // Decimal output uses its normal space padding.
        let expected: Vec<_> = bytes
            .chunks(5)
            .flat_map(|r| if r[0] == b'D' { &b"D  12"[..] } else { r })
            .copied()
            .collect();
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), expected);
    }
}

#[test]
fn rdw_conditions_support_different_lengths_and_rest_fields() {
    let dir = tempfile::tempdir().unwrap();
    let definitions = json!([
        {"name":"record_type","length":1},
        {"name":"label","offset":1,"length":"rest","max_length":12,"when":"record_type = 'H'"},
        {"name":"amount","offset":1,"length":4,"type":"decimal","precision":4,"when":"record_type <> 'H'"}
    ]);
    let mut bytes = Vec::new();
    for record in [&b"Hhello"[..], &b"D  12"[..], &b"H"[..]] {
        dxtran::framing::write_rdw(&mut bytes, record, 32760).unwrap();
    }
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    job(
        dir.path(),
        input(definitions.clone(), "rdw"),
        json!([output(definitions, "rdw")]),
    );
    ok(invoke(dir.path(), "check", true, 1));
    ok(invoke(dir.path(), "run", false, 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn output_conditions_use_sql_result_columns_and_ignore_inactive_values() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("in.dat"),
        "kind,label,n\nH,ABCD,-1\nD,too-long,12\n",
    )
    .unwrap();
    let source = json!({"id":"data","path":"in.dat","format":"csv","encoding":"utf-8",
        "fields":[{"name":"kind"},{"name":"label"},{"name":"n","type":"decimal","precision":4}]});
    let mut destination = output(
        json!([
            {"name":"record_type","length":1},
            {"name":"label","offset":1,"length":4,"when":"record_type = 'H' AND n < 0"},
            {"name":"n","offset":1,"length":4,"type":"uzoned","zone":"ascii","precision":4,
             "when":"record_type = 'D' AND n BETWEEN 0 AND 9999"}
        ]),
        "fixed",
    );
    destination["sql"] = json!("SELECT kind AS record_type, n, label FROM data");
    job(dir.path(), source, json!([destination]));
    ok(invoke(dir.path(), "run", false, 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"HABCDD0012");
}

#[test]
fn alternative_offsets_for_one_column_and_bits_keep_one_public_column_each() {
    let dir = tempfile::tempdir().unwrap();
    let definitions = json!([
        {"name":"record_type","length":1},
        {"name":"value","offset":1,"length":1,"when":"record_type = 'H'"},
        {"offset":2,"length":1,"fill_byte":32,"when":"record_type = 'H'"},
        {"offset":1,"length":1,"fill_byte":32,"when":"record_type = 'D'"},
        {"name":"value","offset":2,"length":1,"when":"record_type = 'D'"},
        {"name":"flags","type":"bits","offset":3,"length":1,"bits":[{"name":"enabled","bit":0}],"when":"record_type IN ('H', 'D')"}
    ]);
    let bytes = b"HA \x80D B\x00";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    job(
        dir.path(),
        input(definitions.clone(), "fixed"),
        json!([output(definitions, "fixed")]),
    );
    ok(invoke(dir.path(), "run", false, 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn invalid_predicates_are_configuration_errors() {
    for expression in [
        "record_type =",
        "unknown = 1",
        "amount > 0",
        "42",
        "random() > 0.5",
        "sum(1) > 0",
        "record_type = 'H'; SELECT 1",
    ] {
        let mut definitions = fields();
        definitions[1]["when"] = json!(expression);
        let error = RecordLayout::input(&physical(definitions, "fixed")).unwrap_err();
        assert_eq!(error.code, 2, "{expression}: {error}");
    }
    let dataset = physical(fields(), "csv");
    assert_eq!(RecordLayout::input(&dataset).unwrap_err().code, 2);
    // Output predicates are checked against the SQL schema as well.
    assert_eq!(
        RecordLayout::output(&physical(fields(), "fixed"), Arc::new(Schema::empty()))
            .unwrap_err()
            .code,
        2
    );
}

#[test]
fn overlaps_gaps_null_conditions_and_active_codec_errors_fail_without_publication() {
    let dir = tempfile::tempdir().unwrap();
    for (definitions, bytes, message) in [
        (fields(), &b"XABCD"[..], "cover"),
        (
            {
                let mut f = fields();
                f[2]["when"] = json!("record_type = 'H'");
                f
            },
            &b"HABCD"[..],
            "overlaps",
        ),
        (
            {
                let mut f = fields();
                f[1]["when"] = json!("record_type = NULL");
                f
            },
            &b"HABCD"[..],
            "cover",
        ),
        (fields(), &b"DABCD"[..], "decimal"),
    ] {
        fs::write(dir.path().join("in.dat"), bytes).unwrap();
        let destination = json!({"id":"result","path":"out.dat","format":"csv","encoding":"utf-8","sql":"SELECT record_type FROM data"});
        job(
            dir.path(),
            input(definitions, "fixed"),
            json!([destination]),
        );
        for threads in [1, 4] {
            let result = invoke(dir.path(), "run", false, threads);
            assert_eq!(
                result.status.code(),
                Some(3),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let diagnostic = String::from_utf8_lossy(&result.stderr);
            assert!(diagnostic.contains(message), "{diagnostic}");
            assert!(diagnostic.contains("record 1"), "{diagnostic}");
            assert!(!dir.path().join("out.dat").exists());
        }
    }
}

#[test]
fn output_condition_conflicts_fail_before_replacing_existing_files() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("in.dat"),
        "record_type,label,amount\nH,ABCD,12\n",
    )
    .unwrap();
    let source = json!({"id":"data","path":"in.dat","format":"csv","encoding":"utf-8",
        "fields":[{"name":"record_type"},{"name":"label"},{"name":"amount","type":"decimal","precision":4}]});
    for expression in [
        "record_type = 'H'",
        "CAST(NULL AS BOOLEAN)",
        "CAST(label AS INT) > 0",
    ] {
        let mut definitions = fields();
        definitions[2]["when"] = json!(expression);
        if expression != "record_type = 'H'" {
            definitions[1]["when"] = json!("FALSE");
        }
        job(
            dir.path(),
            source.clone(),
            json!([output(definitions, "fixed")]),
        );
        for threads in [1, 4] {
            fs::write(dir.path().join("out.dat"), b"existing").unwrap();
            let result = invoke(dir.path(), "run", false, threads);
            assert_eq!(
                result.status.code(),
                Some(5),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert!(String::from_utf8_lossy(&result.stderr).contains("record 1"));
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"existing");
        }
    }
}

#[test]
fn null_and_bit_common_fields_can_select_a_record_definition() {
    let dir = tempfile::tempdir().unwrap();
    let definitions = json!([
        {"name":"value","offset":2,"length":1,"when":"record_type IS NULL AND enabled"},
        {"offset":2,"length":1,"fill_byte":32,"when":"record_type IS NOT NULL OR NOT enabled"},
        {"name":"record_type","offset":0,"length":1,"nullable":true,"null_if":[""]},
        {"name":"flags","type":"bits","offset":1,"length":1,"bits":[{"name":"enabled","bit":0}]}
    ]);
    fs::write(dir.path().join("in.dat"), b" \x80AX\x00 ").unwrap();
    let dataset = {
        let mut d = physical(definitions, "fixed");
        d.path = dir.path().join("in.dat");
        d
    };
    let layout = RecordLayout::input(&dataset).unwrap();
    let mut reader = InputReader::open(&dataset, &layout).unwrap();
    assert_eq!(
        reader.next_row().unwrap().unwrap(),
        vec![
            ScalarValue::Utf8(Some("A".into())),
            ScalarValue::Utf8(None),
            ScalarValue::Boolean(Some(true))
        ]
    );
    let second = reader.next_row().unwrap().unwrap();
    assert!(second[0].is_null());
    assert_eq!(second[1], ScalarValue::Utf8(Some("X".into())));
}
