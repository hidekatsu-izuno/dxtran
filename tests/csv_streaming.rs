use dxtran::encoding::TextEncoding;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn run(dir: &Path, config: &Value) -> Output {
    run_with(dir, config, 4, "256MiB")
}
fn run_with(dir: &Path, config: &Value, threads: usize, memory: &str) -> Output {
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(dir.join("job.json"))
        .args([
            "--threads",
            &threads.to_string(),
            "--memory-limit",
            memory,
            "--temp-dir",
        ])
        .arg(dir.join("tmp"))
        .output()
        .unwrap()
}

#[test]
fn parallel_csv_keeps_order_wide_chunks_and_compressed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("tmp")).unwrap();
    let records: Vec<_> = (0..24001)
        .map(|n| format!("{n},\"日本,{}\"\"\n続き\"\n", "x".repeat(512)))
        .collect();
    let expected = records.concat();
    fs::write(dir.path().join("in.csv"), expected.trim_end_matches('\n')).unwrap();
    let mut config = json!({"input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8",
        "csv":{"header":false},"fields":[{"name":"n","type":"decimal","precision":5},{"name":"memo"}]}],
        "output":[{"id":"result","path":"out.csv","format":"csv","encoding":"utf-8",
            "mode":"overwrite","csv":{"header":false},"sql":"SELECT * FROM data"}]});
    for (threads, memory) in [
        (1, "256MiB"),
        (2, "256MiB"),
        (4, "256MiB"),
        (8, "256MiB"),
        (8, "8MiB"),
    ] {
        let result = run_with(dir.path(), &config, threads, memory);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(fs::read(dir.path().join("out.csv")).unwrap() == expected.as_bytes());
    }
    for codec in ["gzip", "zstd"] {
        config["output"][0]["compression"] = json!(codec);
        let result = run_with(dir.path(), &config, 1, "256MiB");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let serial = fs::read(dir.path().join("out.csv")).unwrap();
        let result = run_with(dir.path(), &config, 4, "256MiB");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(
            fs::read(dir.path().join("out.csv")).unwrap() == serial,
            "{codec}"
        );
        fs::write(dir.path().join("compressed.csv"), &serial).unwrap();
        let mut readback = config.clone();
        readback["input"][0]["path"] = json!("compressed.csv");
        readback["input"][0]["compression"] = json!(codec);
        readback["output"][0]
            .as_object_mut()
            .unwrap()
            .remove("compression");
        let result = run_with(dir.path(), &readback, 4, "256MiB");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(fs::read(dir.path().join("out.csv")).unwrap() == expected.as_bytes());
    }
    config["output"][0]
        .as_object_mut()
        .unwrap()
        .remove("compression");
    config["output"][0]["sql"] = json!("SELECT * FROM data ORDER BY n % 7 LIMIT 9000 OFFSET 8000");
    let mut order: Vec<usize> = (0..records.len()).collect();
    order.sort_by_key(|n| n % 7);
    let expected: String = order[8000..17000]
        .iter()
        .map(|&n| records[n].as_str())
        .collect();
    for threads in [1, 4, 8] {
        let result = run_with(dir.path(), &config, threads, "256MiB");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(fs::read(dir.path().join("out.csv")).unwrap() == expected.as_bytes());
    }
}

#[test]
fn parallel_csv_reports_first_unused_field_error_before_later_syntax_errors() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("tmp")).unwrap();
    let mut records = "ok,1\n".repeat(24001);
    records.replace_range(8192 * 5..8193 * 5, "ok,x\n");
    records.replace_range(8193 * 5..8194 * 5, "o\"k,1\n");
    fs::write(dir.path().join("in.csv"), records).unwrap();
    let config = json!({"input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8",
        "csv":{"header":false},"fields":[{"name":"memo"},{"name":"n","type":"decimal","precision":5}]}],
        "output":[{"id":"result","path":"out.csv","format":"csv","encoding":"utf-8",
            "mode":"overwrite","sql":"SELECT memo FROM data"}]});
    for threads in [1, 4, 8] {
        fs::write(dir.path().join("out.csv"), b"previous").unwrap();
        let result = run_with(dir.path(), &config, threads, "256MiB");
        let error = String::from_utf8_lossy(&result.stderr);
        assert_eq!(result.status.code(), Some(3), "{error}");
        assert!(
            error.contains("record 8193") && error.contains("column n"),
            "{error}"
        );
        assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), b"previous");
        assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }
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
