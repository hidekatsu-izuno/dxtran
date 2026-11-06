use datafusion::common::ScalarValue;
use dxtran::{
    config::{Field, Format},
    encoding::TextEncoding,
    field::{CompiledField, Direction, FieldType},
};
use serde_json::{Value, json};
use std::{fs, process::Command};

fn compile(spec: Value, format: Format, direction: Direction, encoding: &str) -> CompiledField {
    CompiledField::compile(
        serde_json::from_value(spec).unwrap(),
        format,
        direction,
        0,
        TextEncoding::resolve(encoding).unwrap(),
    )
    .unwrap()
}

#[test]
fn bstring_preserves_opaque_bytes_and_uses_encoded_spaces() {
    let raw = [0xff, 0x00, 0x81, 0x40];
    let f = compile(
        json!({"name":"code", "type":"bstring", "length":4, "trim":false}),
        Format::Fixed,
        Direction::Output,
        "utf-8",
    );
    assert_eq!(f.kind(), FieldType::Bstring);
    assert_eq!(
        f.decode(&raw).unwrap(),
        [ScalarValue::Binary(Some(raw.to_vec()))]
    );
    assert_eq!(
        f.encode(&[ScalarValue::Binary(Some(raw.to_vec()))], Format::Fixed)
            .unwrap(),
        raw
    );

    for (trim, expected) in [
        (json!(false), vec![0x40, 0xc1, 0x40, 0x40]),
        (json!("left"), vec![0xc1, 0x40, 0x40]),
        (json!("right"), vec![0x40, 0xc1]),
        (json!("both"), vec![0xc1]),
    ] {
        let f = compile(
            json!({"name":"code", "type":"bstring", "length":4, "trim":trim}),
            Format::Fixed,
            Direction::Output,
            "cp939",
        );
        assert_eq!(
            f.decode(&[0x40, 0xc1, 0x40, 0x40]).unwrap(),
            [ScalarValue::Binary(Some(expected.clone()))]
        );
        let mut output = expected;
        output.resize(4, 0x40);
        assert_eq!(
            f.encode(
                &[ScalarValue::Binary(Some(vec![0x40, 0xc1, 0x40, 0x40]))],
                Format::Fixed
            )
            .unwrap(),
            output
        );
        assert_eq!(
            f.encode(&[ScalarValue::Binary(None)], Format::Fixed)
                .unwrap(),
            [0x40; 4]
        );
    }
    let f = compile(
        json!({"name":"code", "type":"bstring", "length":4}),
        Format::Fixed,
        Direction::Output,
        "cp939",
    );
    assert_eq!(
        f.decode(&[0x40, 0xc1, 0x40, 0x40]).unwrap(),
        [ScalarValue::Binary(Some(vec![0x40, 0xc1]))]
    );
    assert!(
        f.encode(&[ScalarValue::Binary(Some(vec![0xc1; 5]))], Format::Fixed)
            .is_err()
    );
    assert!(
        f.encode(&[ScalarValue::Utf8(Some("A".into()))], Format::Fixed)
            .is_err()
    );
}

#[test]
fn multibyte_space_sequences_are_trimmed_and_padded_as_bytes() {
    let f = compile(
        json!({"name":"code", "type":"bstring", "length":6, "space_char":"　", "trim":"both"}),
        Format::Fixed,
        Direction::Output,
        "utf-16le",
    );
    let bytes = [0, 0x30, 0xff, 0x99, 0, 0x30];
    assert_eq!(
        f.decode(&bytes).unwrap(),
        [ScalarValue::Binary(Some(vec![0xff, 0x99]))]
    );
    assert_eq!(
        f.encode(&[ScalarValue::Binary(Some(bytes.to_vec()))], Format::Fixed)
            .unwrap(),
        [0xff, 0x99, 0, 0x30, 0, 0x30]
    );
    assert_eq!(
        f.encode(&[ScalarValue::Binary(None)], Format::Fixed)
            .unwrap(),
        [0, 0x30, 0, 0x30, 0, 0x30]
    );
    assert!(
        f.encode(&[ScalarValue::Binary(Some(vec![0x99]))], Format::Fixed)
            .is_err()
    );
    // A partial space sequence is payload, never half of a trim match.
    assert_eq!(
        f.decode(&[0x30, 0x99, 0, 0x30]).unwrap(),
        [ScalarValue::Binary(Some(vec![0x30, 0x99]))]
    );
}

#[test]
fn rdw_variable_bstrings_and_null_rules_do_not_decode_payload() {
    let f = compile(
        json!({"name":"code", "type":"bstring", "length":"rest", "max_length":5, "nullable":true, "null_if":["N"], "null_output":"N", "trim":"right"}),
        Format::Rdw,
        Direction::Output,
        "cp939",
    );
    assert_eq!(
        f.decode(&[0xd5, 0x40]).unwrap(),
        [ScalarValue::Binary(None)]
    );
    assert_eq!(
        f.encode(&[ScalarValue::Binary(None)], Format::Rdw).unwrap(),
        [0xd5]
    );
    assert_eq!(
        f.decode(&[0xff, 0x00, 0x40]).unwrap(),
        [ScalarValue::Binary(Some(vec![0xff, 0x00]))]
    );
    assert_eq!(
        f.encode(
            &[ScalarValue::Binary(Some(vec![0xff, 0x00, 0x40]))],
            Format::Rdw
        )
        .unwrap(),
        [0xff, 0]
    );
    assert!(
        f.encode(&[ScalarValue::Binary(Some(vec![0xd5]))], Format::Rdw)
            .is_err()
    );
    assert!(f.decode(&[0xc1; 6]).is_err());
    let raw = compile(
        json!({"name":"code", "type":"bstring", "length":"rest", "nullable":true, "null_bytes":"ffff", "max_length":3}),
        Format::Rdw,
        Direction::Output,
        "utf-8",
    );
    assert_eq!(
        raw.decode(&[0xff, 0xff]).unwrap(),
        [ScalarValue::Binary(None)]
    );
    assert_eq!(
        raw.encode(&[ScalarValue::Binary(None)], Format::Rdw)
            .unwrap(),
        [0xff, 0xff]
    );
    assert_eq!(
        raw.decode(b"A ").unwrap(),
        [ScalarValue::Binary(Some(b"A ".to_vec()))]
    );
}

#[test]
fn renamed_space_char_is_validated_and_works_in_csv() {
    assert!(
        serde_json::from_value::<Field>(json!({"name":"code", "length":4, "pad_char":" "}))
            .is_err()
    );
    for options in [
        json!({"space_char":""}),
        json!({"space_char":"ab"}),
        json!({"null_if_pattern":".*", "nullable":true}),
        json!({"binary_encoding":"hex"}),
    ] {
        let mut spec = json!({"name":"code", "type":"bstring", "length":4});
        spec.as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        assert!(
            CompiledField::compile(
                serde_json::from_value(spec).unwrap(),
                Format::Fixed,
                Direction::Input,
                0,
                TextEncoding::UTF_8
            )
            .is_err()
        );
    }
    let f = compile(
        json!({"name":"label", "space_char":"　", "trim":"both"}),
        Format::Csv,
        Direction::Input,
        "utf-8",
    );
    assert_eq!(f.trim_text("　A　"), "A");
    let f = compile(
        json!({"name":"code", "type":"bstring", "length":4, "space_char":"　"}),
        Format::Fixed,
        Direction::Input,
        "jef",
    );
    assert!(f.decode(&[0xff, 0xff, 0x00, 0x00]).is_ok());
}

fn run(work: &std::path::Path, job: &Value, threads: usize) -> std::process::Output {
    let path = work.join("job.json");
    fs::write(&path, job.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(path)
        .args(["--threads", &threads.to_string()])
        .output()
        .unwrap()
}

#[test]
fn binary_sort_joins_and_raw_fixed_rdw_roundtrips_work_in_parallel() {
    let dir = tempfile::tempdir().unwrap();
    // Source order: digit, uppercase, lowercase. EBCDIC byte order is reversed.
    let records = [0xf0, 0x40, 0xc1, 0x40, 0x81, 0x40].repeat(3001);
    fs::write(dir.path().join("in.dat"), &records).unwrap();
    fs::write(dir.path().join("lookup.csv"), "code\nA\na\n0\n").unwrap();
    let input_fields = json!([{"name":"code", "type":"bstring", "length":2}]);
    let job = json!({
        "input":[
            {"id":"data", "format":"fixed", "path":"in.dat", "encoding":"cp939", "fixed":{"record_separator":"none"}, "fields":input_fields,
             "check":{"when":"icu_decode(code, 'cp939') NOT IN ('0', 'A', 'a')", "level":"error", "message":"bad code"}},
            {"id":"lookup", "format":"csv", "path":"lookup.csv", "encoding":"utf-8", "fields":[{"name":"code"}]}
        ],
        "output":[
            {"id":"copy", "format":"fixed", "path":"copy.dat", "encoding":"cp939", "mode":"overwrite", "fixed":{"record_separator":"none"}, "fields":input_fields, "sql":"SELECT * FROM data"},
            {"id":"rdw", "format":"rdw", "path":"copy.rdw", "encoding":"cp939", "mode":"overwrite", "rdw":{}, "fields":input_fields, "sql":"SELECT * FROM data"},
            {"id":"sorted", "format":"csv", "path":"sorted.csv", "encoding":"utf-8", "mode":"overwrite", "sql":"SELECT icu_decode(code, 'cp939') AS label FROM data ORDER BY code"},
            {"id":"joined", "format":"csv", "path":"joined.csv", "encoding":"utf-8", "mode":"overwrite", "sql":"SELECT lookup.code FROM data JOIN lookup ON data.code = icu_encode(lookup.code, 'cp939')"},
            {"id":"decoded_join", "format":"csv", "path":"decoded_join.csv", "encoding":"utf-8", "mode":"overwrite", "sql":"SELECT lookup.code FROM data JOIN lookup ON icu_decode(data.code, 'cp939') = lookup.code"},
            {"id":"hex", "format":"csv", "path":"hex.csv", "encoding":"utf-8", "mode":"overwrite", "sql":"SELECT * FROM data LIMIT 3"}
        ]
    });
    for threads in [1, 4] {
        let output = run(dir.path(), &job, threads);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read(dir.path().join("copy.dat")).unwrap(), records);
        let expected_rdw = [
            vec![0, 6, 0, 0, 0xf0, 0x40],
            vec![0, 6, 0, 0, 0xc1, 0x40],
            vec![0, 6, 0, 0, 0x81, 0x40],
        ]
        .concat()
        .repeat(3001);
        assert_eq!(fs::read(dir.path().join("copy.rdw")).unwrap(), expected_rdw);
        assert_eq!(
            fs::read_to_string(dir.path().join("sorted.csv")).unwrap(),
            format!(
                "label\n{}{}{}",
                "a\n".repeat(3001),
                "A\n".repeat(3001),
                "0\n".repeat(3001)
            )
        );
        for path in ["joined.csv", "decoded_join.csv"] {
            assert_eq!(
                fs::read_to_string(dir.path().join(path)).unwrap(),
                format!("code\n{}", "0\nA\na\n".repeat(3001))
            );
        }
        assert_eq!(
            fs::read_to_string(dir.path().join("hex.csv")).unwrap(),
            "code\nf0\nc1\n81\n"
        );
    }
}

#[test]
fn raw_preservation_and_failed_bstring_output_keep_existing_files() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), [0xff, 0x00, 0xc0, 0xaf]).unwrap();
    let mut job = json!({
        "input":[{"id":"data", "format":"fixed", "path":"in.dat", "encoding":"utf-8", "fixed":{"record_separator":"none"}, "fields":[{"name":"code", "type":"bstring", "length":2, "trim":false}]}],
        "output":[{"id":"result", "format":"fixed", "path":"out.dat", "encoding":"cp939", "fixed":{"record_separator":"none"}, "fields":[{"name":"code", "type":"bstring", "length":2}], "mode":"overwrite", "sql":"SELECT * FROM data"}]
    });
    let output = run(dir.path(), &job, 4);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        [0xff, 0, 0xc0, 0xaf]
    );
    fs::write(dir.path().join("out.dat"), b"original").unwrap();
    job["output"][0]["fields"][0]["length"] = json!(1);
    let output = run(dir.path(), &job, 4);
    assert_eq!(output.status.code(), Some(5));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"original");
}
