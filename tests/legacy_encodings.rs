use dxtran::encoding::TextEncoding;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn invoke(dir: &Path, config: &Value, command: &str, args: &[&str]) -> Output {
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(dir.join("job.json"))
        .args(args)
        .args(["--temp-dir", dir.join("tmp").to_str().unwrap()])
        .output()
        .unwrap()
}

fn ok(result: &Output) {
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn physical_legacy_fields_preserve_padding_shift_boundaries_and_binary_data() {
    let dir = tempfile::tempdir().unwrap();
    for (encoding, width, pad, record, text) in [
        (
            "jef",
            4,
            "　",
            "c6fc4040c5ecb5feff00123d",
            "日,東京,ff00,-123\n",
        ),
        (
            "x-fujitsu-ebcdic-ascii",
            2,
            " ",
            "c140c2c3ff00123d",
            "A,BC,ff00,-123\n",
        ),
        (
            "x-fujitsu-ebcdic-kana",
            2,
            " ",
            "c140c2c3ff00123d",
            "A,BC,ff00,-123\n",
        ),
        (
            "x-fujitsu-ebcdic-lower",
            2,
            " ",
            "c140c2c3ff00123d",
            "A,BC,ff00,-123\n",
        ),
        (
            "cp930",
            6,
            " ",
            "0e45620f40400e455745750fff00123d",
            "日,東京,ff00,-123\n",
        ),
        (
            "cp939",
            6,
            " ",
            "0e45620f40400e455745750fff00123d",
            "日,東京,ff00,-123\n",
        ),
        (
            "cp942",
            4,
            " ",
            "93fa2020938c8b9eff00123d",
            "日,東京,ff00,-123\n",
        ),
        (
            "cp942c",
            4,
            " ",
            "93fa2020938c8b9eff00123d",
            "日,東京,ff00,-123\n",
        ),
        (
            "cp943c",
            4,
            " ",
            "93fa2020938c8b9eff00123d",
            "日,東京,ff00,-123\n",
        ),
    ] {
        let body = hex::decode(record).unwrap();
        for format in ["fixed", "rdw"] {
            // Exercise multiple batches and parallel fixed conversion as well as RDW.
            let rows = if encoding == "cp939" { 8193 } else { 2 };
            let mut record = Vec::new();
            if format == "rdw" {
                record.extend_from_slice(&((body.len() + 4) as u16).to_be_bytes());
                record.extend_from_slice(&[0, 0]);
            }
            record.extend_from_slice(&body);
            let expected = record.repeat(rows);
            fs::write(dir.path().join("in.dat"), &expected).unwrap();
            let mut input = json!({"id":"data", "format":format, "path":"in.dat", "encoding":encoding,
                "fields":[{"name":"left_text", "length":width, "pad_char":pad},
                    {"name":"right_text", "length":width, "pad_char":pad},
                    {"name":"payload", "type":"binary", "length":2},
                    {"name":"amount", "type":"packed", "length":2, "precision":3}]});
            if format == "fixed" {
                input["fixed"] = json!({"record_separator":"none"});
            }
            let mut output = input.clone();
            output["id"] = json!("copy");
            output["path"] = json!("out.dat");
            output["sql"] = json!("SELECT * FROM data");
            output["mode"] = json!("overwrite");
            let config = json!({"input":[input], "output":[output,
                {"id":"readable", "path":"out.csv", "format":"csv", "encoding":"utf-8",
                    "mode":"overwrite", "sql":"SELECT * FROM data"}]});
            ok(&invoke(dir.path(), &config, "check", &["--scan"]));
            for threads in ["1", "4"] {
                let result = invoke(dir.path(), &config, "run", &["--threads", threads]);
                ok(&result);
                assert_eq!(
                    fs::read(dir.path().join("out.dat")).unwrap(),
                    expected,
                    "{encoding}/{format}"
                );
                assert_eq!(
                    fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                    format!("left_text,right_text,payload,amount\n{}", text.repeat(rows)),
                    "{encoding}/{format}"
                );
                let logs: Vec<Value> = String::from_utf8(result.stderr)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                let log = logs
                    .iter()
                    .find(|log| log["event"] == "encoding" && log["definition"] == "copy")
                    .unwrap();
                let canonical = TextEncoding::resolve(encoding).unwrap().name();
                assert_eq!(log["label"], encoding);
                assert_eq!(log["canonical"], canonical);
                assert_eq!(log["output_encoding"], canonical);
            }
        }
    }
}

#[test]
fn fujitsu_ebcdic_csv_transcodes_syntax_and_newlines() {
    let dir = tempfile::tempdir().unwrap();
    let text = "A,\"B,C\"\r\nD,\"E\"\"F\"\r\n";
    for encoding in [
        "x-fujitsu-ebcdic-ascii",
        "x-fujitsu-ebcdic-kana",
        "x-fujitsu-ebcdic-lower",
    ] {
        let codec = TextEncoding::resolve(encoding).unwrap();
        let bytes = codec.encode(text).unwrap();
        fs::write(dir.path().join("in.csv"), &bytes).unwrap();
        let config = json!({"input":[{"id":"data", "format":"csv", "path":"in.csv",
            "encoding":encoding, "csv":{"header":false}, "fields":[{"name":"a"}, {"name":"b"}]}],
            "output":[{"id":"copy", "format":"csv", "path":"out.csv", "encoding":encoding,
                "csv":{"header":false, "line_ending":"crlf"}, "mode":"overwrite", "sql":"SELECT * FROM data"},
                {"id":"readable", "format":"csv", "path":"utf8.csv", "encoding":"utf-8",
                    "csv":{"header":false, "line_ending":"crlf"}, "mode":"overwrite", "sql":"SELECT * FROM data"}]});
        ok(&invoke(dir.path(), &config, "run", &["--threads", "4"]));
        assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), bytes);
        assert_eq!(
            fs::read_to_string(dir.path().join("utf8.csv")).unwrap(),
            text
        );
    }
}

#[test]
fn legacy_conversion_failures_keep_previous_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = json!({"input":[{"id":"data", "format":"fixed", "path":"in.dat", "encoding":"jef",
        "fixed":{"record_separator":"none"}, "fields":[{"name":"text", "length":2, "pad_char":"　"}]}],
        "output":[{"id":"result", "format":"csv", "path":"out.csv", "encoding":"utf-8",
            "mode":"overwrite", "sql":"SELECT * FROM data"}]});
    fs::write(dir.path().join("in.dat"), [0, 0]).unwrap();
    fs::write(dir.path().join("out.csv"), b"previous").unwrap();
    let result = invoke(dir.path(), &config, "check", &["--scan"]);
    assert_eq!(result.status.code(), Some(3));
    for threads in ["1", "4"] {
        let result = invoke(dir.path(), &config, "run", &["--threads", threads]);
        assert_eq!(
            result.status.code(),
            Some(3),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            error.contains("record 1 byte 0") && error.contains("column text"),
            "{error}"
        );
        assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), b"previous");
    }
    fs::write(dir.path().join("in.csv"), "text\n😀\n").unwrap();
    config["input"][0] = json!({"id":"data", "format":"csv", "path":"in.csv", "encoding":"utf-8", "fields":[{"name":"text"}]});
    config["output"][0]["encoding"] = json!("cp939");
    let result = invoke(dir.path(), &config, "run", &[]);
    assert_eq!(
        result.status.code(),
        Some(5),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("cannot be represented"));
    assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), b"previous");
    assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
}
