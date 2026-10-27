use datafusion::arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    path::Path,
    process::{Command, Output},
    sync::Arc,
};

fn source() -> Value {
    json!({"id":"data","path":"in.csv","format":"csv","encoding":"utf-8",
        "fields":[{"name":"id"},{"name":"amount","type":"decimal","precision":6,"nullable":true}]})
}
fn destination() -> Value {
    json!({"id":"result","path":"out.csv","format":"csv","encoding":"utf-8","mode":"overwrite",
        "sql":"SELECT id, amount FROM data"})
}
fn rule(when: &str, level: &str, message: &str) -> Value {
    json!({"when":when,"level":level,"message":message})
}
fn invoke(
    dir: &Path,
    input: Value,
    output: Value,
    command: &str,
    scan: bool,
    threads: usize,
) -> Output {
    fs::create_dir_all(dir.join("tmp")).unwrap();
    fs::write(
        dir.join("job.json"),
        json!({"input":[input],"output":output}).to_string(),
    )
    .unwrap();
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
fn ok(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn events(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|v| v["event"] == "check")
        .collect()
}

#[test]
fn messages_use_the_requested_streams_and_checks_do_not_filter_records() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,amount\na,12\nb,-1\nc,\n").unwrap();
    let mut input = source();
    input["check"] = json!([
        rule("amount < 0", "warn", "負の金額です"),
        rule("amount IS NULL", "info", "金額未指定\nそのまま継続"),
        rule("amount = NULL", "error", "must not be emitted")
    ]);
    let mut output = destination();
    output["check"] = rule(
        "id IN ('a', 'b') AND amount IS NOT NULL",
        "info",
        "出力対象です",
    );
    for threads in [1, 4] {
        let result = ok(invoke(
            dir.path(),
            input.clone(),
            json!([output.clone()]),
            "run",
            false,
            threads,
        ));
        let warnings = events(&result.stderr);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0]["message"], "負の金額です");
        assert_eq!(warnings[0]["level"], "warn");
        assert_eq!(warnings[0]["record"], 2);
        let info = events(&result.stdout);
        assert_eq!(info.iter().filter(|v| v["side"] == "input").count(), 1);
        assert_eq!(info.iter().filter(|v| v["side"] == "output").count(), 2);
        assert!(
            info.iter()
                .any(|v| v["message"] == "金額未指定\nそのまま継続")
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "id,amount\na,12\nb,-1\nc,\n"
        );
    }
}

#[test]
fn input_checks_run_before_projection_filters_and_later_codec_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut input = source();
    input["check"] = json!([
        rule("amount < 0", "warn", "before-abend"),
        rule("amount < 0", "error", "入力金額エラー"),
        rule("TRUE", "info", "after-abend")
    ]);
    let mut output = destination();
    output["sql"] = json!("SELECT id FROM data WHERE id = 'other'");
    fs::write(dir.path().join("in.csv"), "id,amount\na,-1\nb,broken\n").unwrap();
    for threads in [1, 4] {
        let result = invoke(
            dir.path(),
            input.clone(),
            json!([output.clone()]),
            "run",
            false,
            threads,
        );
        assert_eq!(
            result.status.code(),
            Some(3),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let diagnostic = String::from_utf8_lossy(&result.stderr);
        assert_eq!(diagnostic.matches("入力金額エラー").count(), 1);
        assert!(diagnostic.contains("record 1 check 2"));
        assert_eq!(events(&result.stderr)[0]["message"], "before-abend");
        assert!(events(&result.stdout).is_empty());
        assert!(!dir.path().join("out.csv").exists());
        assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }
}

#[test]
fn output_abend_uses_sql_types_and_preserves_every_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,amount\na,12\nb,-1\nc,-2\n").unwrap();
    let mut first = destination();
    first["id"] = json!("first");
    first["path"] = json!("first.csv");
    let mut second = destination();
    second["sql"] = json!("SELECT id, CAST(amount AS BIGINT) AS amount FROM data ORDER BY id");
    second["check"] = json!([
        rule("amount < 0", "error", "出力金額エラー"),
        rule("TRUE", "info", "output-ok")
    ]);
    for threads in [1, 4] {
        fs::write(dir.path().join("first.csv"), "old-first").unwrap();
        fs::write(dir.path().join("out.csv"), "old-second").unwrap();
        let result = invoke(
            dir.path(),
            source(),
            json!([first.clone(), second.clone()]),
            "run",
            false,
            threads,
        );
        assert_eq!(
            result.status.code(),
            Some(5),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let diagnostic = String::from_utf8_lossy(&result.stderr);
        assert_eq!(diagnostic.matches("出力金額エラー").count(), 1);
        assert!(diagnostic.contains("output result record 2 check 1"));
        assert_eq!(events(&result.stdout).len(), 1);
        assert_eq!(
            fs::read_to_string(dir.path().join("first.csv")).unwrap(),
            "old-first"
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "old-second"
        );
    }
}

#[test]
fn check_and_explain_validate_without_logging_and_scan_runs_only_input_checks() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,amount\na,1\n").unwrap();
    let mut input = source();
    input["check"] = rule("TRUE", "info", "input-check");
    let mut output = destination();
    output["check"] = rule("TRUE", "error", "output-check");
    for command in ["check", "explain"] {
        let result = ok(invoke(
            dir.path(),
            input.clone(),
            json!([output.clone()]),
            command,
            false,
            4,
        ));
        assert!(events(&result.stdout).is_empty());
        assert!(events(&result.stderr).is_empty());
    }
    let result = ok(invoke(
        dir.path(),
        input.clone(),
        json!([output.clone()]),
        "check",
        true,
        4,
    ));
    assert_eq!(events(&result.stdout).len(), 1);
    input["check"] = rule("TRUE", "error", "scan-abend");
    assert_eq!(
        invoke(dir.path(), input, json!([output]), "check", true, 4)
            .status
            .code(),
        Some(3)
    );
    assert!(!dir.path().join("out.csv").exists());
}

#[test]
fn invalid_configuration_and_sql_fail_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,amount\na,1\n").unwrap();
    for check in [
        rule("unknown > 0", "error", "invalid"),
        rule("amount >", "error", "invalid"),
        rule("42", "error", "invalid"),
        rule("random() > 0", "error", "invalid"),
        rule("sum(amount) > 0", "error", "invalid"),
        rule("TRUE", "debug", "invalid"),
        json!({"when":"TRUE","level":"info"}),
        json!({"when":"TRUE","level":"info","message":"invalid","extra":true}),
        json!({"when":{"field":"id","equals":"a"},"level":"info","message":"invalid"}),
    ] {
        for side in ["input", "output"] {
            let mut input = source();
            let mut output = destination();
            if side == "input" {
                input["check"] = check.clone();
            } else {
                output["check"] = check.clone();
            }
            let result = invoke(dir.path(), input, json!([output]), "check", false, 1);
            assert_eq!(
                result.status.code(),
                Some(2),
                "{check}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }
}

#[test]
fn checks_read_the_conditional_union_on_fixed_and_rdw_inputs_and_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"label","offset":1,"length":4,"when":"kind = 'H'"},
        {"name":"amount","offset":1,"length":4,"type":"decimal","precision":4,"when":"kind = 'D'"}
    ]);
    for format in ["fixed", "rdw"] {
        let mut bytes = Vec::new();
        for row in [b"HABCD", b"D  12"] {
            if format == "rdw" {
                dxtran::framing::write_rdw(&mut bytes, row, 32760).unwrap();
            } else {
                bytes.extend_from_slice(row);
            }
        }
        fs::write(dir.path().join("in.dat"), &bytes).unwrap();
        let mut input = json!({"id":"data","path":"in.dat","format":format,"encoding":"utf-8","fields":fields,
            "check":[rule("kind = 'H' AND amount IS NULL", "info", "header"), rule("kind = 'D' AND amount = 12", "warn", "detail")]});
        if format == "fixed" {
            input["fixed"] = json!({"record_separator":"none"});
        }
        let mut output = input.clone();
        output["id"] = json!("result");
        output["path"] = json!("out.dat");
        output["mode"] = json!("overwrite");
        output["sql"] = json!("SELECT * FROM data");
        output["check"] = rule("kind = 'D' AND label IS NULL", "info", "detail-output");
        for threads in [1, 4] {
            let result = ok(invoke(
                dir.path(),
                input.clone(),
                json!([output.clone()]),
                "run",
                false,
                threads,
            ));
            assert_eq!(events(&result.stdout).len(), 2);
            assert_eq!(events(&result.stderr).len(), 1);
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
        }
    }
}

#[test]
fn parquet_checks_work_for_native_input_and_output_schemas() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(-1)]))],
    )
    .unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(dir.path().join("in.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let input = json!({"id":"data","path":"in.parquet","format":"parquet",
        "check":rule("n IS NULL", "info", "native-null")});
    let output = json!({"id":"result","path":"out.parquet","format":"parquet","sql":"SELECT n FROM data","mode":"overwrite",
        "check":rule("n < 0", "warn", "native-negative")});
    for threads in [1, 4] {
        let result = ok(invoke(
            dir.path(),
            input.clone(),
            json!([output.clone()]),
            "run",
            false,
            threads,
        ));
        assert_eq!(events(&result.stdout)[0]["record"], 2);
        assert_eq!(events(&result.stderr)[0]["record"], 3);
        let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(dir.path().join("out.parquet")).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), batch);
    }
}

#[test]
fn condition_evaluation_failures_use_the_correct_phase_and_record() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,amount\na,1\nb,2\n").unwrap();
    for side in ["input", "output"] {
        let mut input = source();
        let mut output = destination();
        let check = rule("CAST(id AS INTEGER) > 0", "warn", "never");
        if side == "input" {
            input["check"] = check;
        } else {
            output["check"] = check;
        }
        let result = invoke(dir.path(), input, json!([output]), "run", false, 4);
        assert_eq!(
            result.status.code(),
            Some(if side == "input" { 3 } else { 5 })
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains("record 1 check 1"));
        assert!(events(&result.stderr).is_empty());
    }
}

#[test]
fn checks_are_not_repeated_for_a_record_retained_at_a_batch_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let width = dxtran::framing::MAX_RECORD / 2;
    fs::write(dir.path().join("in.dat"), vec![b'x'; width * 3]).unwrap();
    let input = json!({"id":"data","path":"in.dat","format":"fixed","encoding":"utf-8",
        "fixed":{"record_separator":"none"},"fields":[{"name":"value","length":width}],
        "check":rule("length(value) > 0", "info", "one-per-record")});
    let mut output = destination();
    output["sql"] = json!("SELECT length(value) AS length FROM data");
    let result = ok(invoke(dir.path(), input, json!([output]), "run", false, 4));
    let info = events(&result.stdout);
    assert_eq!(info.len(), 3);
    assert_eq!(
        info.iter()
            .map(|v| v["record"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

#[test]
fn a_codec_failure_precedes_later_output_check_messages() {
    let dir = tempfile::tempdir().unwrap();
    let mut output = destination();
    output["fields"] = json!([{"name":"amount","precision":1}]);
    output["check"] = rule("amount < 0", "info", "too-late");
    fs::write(dir.path().join("in.csv"), "id,amount\na,12\nb,-1\n").unwrap();
    let result = invoke(dir.path(), source(), json!([output]), "run", false, 4);
    assert_eq!(result.status.code(), Some(5));
    assert!(events(&result.stdout).is_empty());
    assert!(String::from_utf8_lossy(&result.stderr).contains("precision overflow"));
}
