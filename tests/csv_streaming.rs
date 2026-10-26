use dxtran::encoding::TextEncoding;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn run(dir: &Path, config: &Value) -> Output {
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(dir.join("job.json"))
        .args(["--threads", "4", "--temp-dir"])
        .arg(dir.join("tmp"))
        .output()
        .unwrap()
}

#[test]
fn csv_buffers_preserve_multiline_null_empty_and_encoding_state() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("tmp")).unwrap();
    let mut expected = String::from("memo;optional;amount\r\n");
    for n in 0..10001 {
        let optional = if n % 2 == 0 { "" } else { "''" };
        expected.push_str(&format!(
            "'{n};日本''語\r\n続き';{optional};{}.{:02}\r\n",
            n % 1000,
            n % 100
        ));
    }
    // Each encoding passes multiple Arrow batches and output flush boundaries.
    // Input permits an omitted final separator; output emits the configured one.
    for encoding in ["utf-8", "utf-16le", "utf-16be", "shift_jis", "iso-2022-jp"] {
        let codec = TextEncoding::resolve(encoding).unwrap();
        fs::write(
            dir.path().join("in.csv"),
            codec.encode(expected.trim_end_matches("\r\n")).unwrap(),
        )
        .unwrap();
        let config = json!({"input":[{"id":"data","path":"in.csv","format":"csv","encoding":encoding,
            "csv":{"delimiter":";","quote":"'"},"fields":[{"name":"memo"},{"name":"optional","nullable":true},
                {"name":"amount","type":"decimal","precision":6,"scale":2}]}],
            "output":[{"id":"result","path":"out.csv","mode":"overwrite","format":"csv","encoding":encoding,
                "csv":{"delimiter":";","quote":"'","line_ending":"crlf"},"sql":"SELECT * FROM data"}]});
        let result = run(dir.path(), &config);
        assert!(
            result.status.success(),
            "{encoding}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            fs::read(dir.path().join("out.csv")).unwrap(),
            codec.encode(&expected).unwrap(),
            "{encoding}"
        );
    }
}

#[test]
fn direct_csv_output_keeps_precision_and_null_checks() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("tmp")).unwrap();
    let mut input = String::from("n\n");
    for n in 1..=9000 {
        input.push_str(&format!("{n}\n"));
    }
    fs::write(dir.path().join("in.csv"), input).unwrap();
    for (sql, field, message) in [
        (
            "SELECT CAST(CASE WHEN n = 8193 THEN 10.00 ELSE 0.01 END AS DECIMAL(5,2)) AS value FROM data",
            json!({"name":"value","precision":3,"scale":2}),
            "precision",
        ),
        (
            "SELECT CASE WHEN n = 8193 THEN NULL ELSE 'ok' END AS value FROM data",
            json!({"name":"value","nullable":false}),
            "NULL",
        ),
    ] {
        fs::write(dir.path().join("out.csv"), b"previous").unwrap();
        let config = json!({"input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8",
            "fields":[{"name":"n","type":"decimal","precision":5}]}],
            "output":[{"id":"result","path":"out.csv","mode":"overwrite","format":"csv","encoding":"utf-8",
                "fields":[field],"sql":sql}]});
        let result = run(dir.path(), &config);
        let error = String::from_utf8_lossy(&result.stderr);
        assert_eq!(result.status.code(), Some(5), "{error}");
        assert!(
            error.contains("record 8193 column value") && error.contains(message),
            "{error}"
        );
        assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), b"previous");
        assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }
}
