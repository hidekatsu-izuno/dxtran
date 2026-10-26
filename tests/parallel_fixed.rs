use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn job(dir: &Path, width: usize, separator: &str, sql: &str) -> Value {
    let common = json!({"format":"fixed","encoding":"utf-8",
        "fixed":{"record_length":width,"record_separator":separator},
        "fields":[{"name":"text","length":width-8,"trim":"none"},
                  {"name":"n","type":"uzoned","zone":"ascii","precision":8,"length":8}]});
    let mut input = common.clone();
    input["id"] = json!("data");
    input["path"] = json!("in.dat");
    let mut output = common;
    output["id"] = json!("result");
    output["path"] = json!("out.dat");
    output["mode"] = json!("overwrite");
    output["sql"] = json!(sql);
    fs::create_dir_all(dir.join("tmp")).unwrap();
    json!({"input":[input],"process":[],"output":[output]})
}
fn run(dir: &Path, job: &Value, threads: usize, memory: &str) -> Output {
    fs::write(dir.join("job.json"), job.to_string()).unwrap();
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
fn ok(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn records(rows: usize, width: usize, separator: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    for n in 1..=rows {
        result.extend_from_slice(format!("{n:0text$}{n:08}", text = width - 8).as_bytes());
        result.extend_from_slice(separator);
    }
    result
}

#[test]
fn parallel_fixed_preserves_bytes_and_stable_sql_order() {
    let d = tempfile::tempdir().unwrap();
    for (width, sep, bytes) in [
        (32, "none", &b""[..]),
        (32, "crlf", &b"\r\n"[..]),
        (2048, "lf", &b"\n"[..]),
    ] {
        let expected = records(24001, width, bytes);
        // Optional final separator, including a partial final batch.
        fs::write(
            d.path().join("in.dat"),
            &expected[..expected.len() - bytes.len()],
        )
        .unwrap();
        let config = job(d.path(), width, sep, "SELECT * FROM data");
        for threads in [1, 2, 4, 8] {
            ok(run(d.path(), &config, threads, "256MiB"));
            assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), expected);
        }
    }
    let input = records(24001, 32, b"");
    fs::write(d.path().join("in.dat"), &input).unwrap();
    let sql = "SELECT text,n FROM data ORDER BY n % 7 LIMIT 5000 OFFSET 8000";
    let config = job(d.path(), 32, "none", sql);
    ok(run(d.path(), &config, 1, "256MiB"));
    let expected = fs::read(d.path().join("out.dat")).unwrap();
    for threads in [2, 4, 8] {
        ok(run(d.path(), &config, threads, "256MiB"));
        assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), expected);
    }
    // A small memory pool selects the serial fallback even with many workers.
    let config = job(d.path(), 32, "none", "SELECT * FROM data");
    ok(run(d.path(), &config, 8, "8MiB"));
    assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), input);
}

#[test]
fn parallel_errors_report_first_record_and_preserve_outputs() {
    let d = tempfile::tempdir().unwrap();
    let mut input = records(24001, 32, b"\r\n");
    for record in [8193, 16385] {
        input[(record - 1) * 34 + 24] = b'x';
    }
    fs::write(d.path().join("in.dat"), input).unwrap();
    let mut config = job(d.path(), 32, "crlf", "SELECT text,n FROM data");
    // Unselected physical fields are still validated.
    config["output"][0] = json!({"id":"result","path":"out.dat","format":"csv","encoding":"utf-8","mode":"overwrite","sql":"SELECT text FROM data"});
    for threads in [1, 4, 8] {
        fs::write(d.path().join("out.dat"), b"previous").unwrap();
        let output = run(d.path(), &config, threads, "256MiB");
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(3), "{error}");
        assert!(
            error.contains("record 8193 byte 278528") && error.contains("column n"),
            "{error}"
        );
        assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), b"previous");
        assert_eq!(fs::read_dir(d.path().join("tmp")).unwrap().count(), 0);
    }
    fs::write(d.path().join("in.dat"), records(24001, 32, b"")).unwrap();
    let config = job(
        d.path(),
        32,
        "none",
        "SELECT text, CAST(CASE WHEN n IN (8193,16385) THEN 100000000 ELSE n END AS DECIMAL(10,0)) AS n FROM data",
    );
    for threads in [1, 4, 8] {
        let output = run(d.path(), &config, threads, "256MiB");
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(5), "{error}");
        assert!(error.contains("record 8193 column n"), "{error}");
        assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), b"previous");
        assert_eq!(fs::read_dir(d.path().join("tmp")).unwrap().count(), 0);
    }
}

#[test]
fn parallel_fixed_rejects_truncated_bodies_and_separators() {
    let d = tempfile::tempdir().unwrap();
    let config = job(d.path(), 32, "crlf", "SELECT * FROM data");
    for cut in [1, 3] {
        let input = records(8193, 32, b"\r\n");
        fs::write(d.path().join("in.dat"), &input[..input.len() - cut]).unwrap();
        for threads in [1, 4] {
            let output = run(d.path(), &config, threads, "256MiB");
            let error = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(3), "{error}");
            assert!(
                error.contains("record 8193 byte 278528") && error.contains("truncated record"),
                "{error}"
            );
        }
    }
    let mut input = records(8193, 32, b"\r\n");
    input[8192 * 34 + 32] = b'x';
    fs::write(d.path().join("in.dat"), input).unwrap();
    let output = run(d.path(), &config, 4, "256MiB");
    assert_eq!(output.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&output.stderr).contains("separator mismatch"));
}

#[test]
fn wide_fixed_output_is_chunked_before_encoding() {
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join("in.dat"), records(1000, 32, b"")).unwrap();
    let mut config = job(d.path(), 32, "none", "SELECT text,n FROM data");
    config["output"][0]["fixed"]["record_length"] = json!(32776);
    config["output"][0]["fields"][0]["length"] = json!(32768);
    ok(run(d.path(), &config, 1, "256MiB"));
    let expected = fs::read(d.path().join("out.dat")).unwrap();
    ok(run(d.path(), &config, 4, "256MiB"));
    assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), expected);
}

#[cfg(unix)]
#[test]
fn parallel_conversion_cancels_and_cleans_temporary_output() {
    use std::{
        process::Stdio,
        time::{Duration, Instant},
    };
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join("in.dat"), records(10000, 32, b"")).unwrap();
    let mut config = job(d.path(), 32, "none", "SELECT text,n FROM data");
    // Keep the encoder/writer busy without waiting for a blocking SQL operator.
    config["output"][0]["fixed"]["record_length"] = json!(65544);
    config["output"][0]["fields"][0]["length"] = json!(65536);
    fs::write(d.path().join("job.json"), config.to_string()).unwrap();
    fs::write(d.path().join("out.dat"), b"previous").unwrap();
    fs::write(d.path().join("tmp/keep"), b"keep").unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(d.path().join("job.json"))
        .args(["--threads", "4", "--temp-dir"])
        .arg(d.path().join("tmp"))
        .stdout(Stdio::null())
        .stderr(fs::File::create(d.path().join("log")).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while fs::read_dir(d.path().join("tmp")).unwrap().count() == 1 {
        assert!(child.try_wait().unwrap().is_none());
        if Instant::now() > deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("job did not start");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(100));
    rustix::process::kill_process(
        rustix::process::Pid::from_child(&child),
        rustix::process::Signal::TERM,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("cancellation timeout");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(4));
    assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), b"previous");
    assert_eq!(fs::read_dir(d.path().join("tmp")).unwrap().count(), 1);
}
