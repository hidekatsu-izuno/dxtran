use serde_json::json;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn setup() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\nfirst\nsecond\nfirst\n").unwrap();
    fs::write(dir.path().join("first.csv"), "keep first").unwrap();
    fs::write(dir.path().join("out.csv"), "keep second").unwrap();
    dir
}

fn write_job(dir: &Path, sql: &str, process: bool) {
    let output = |id: &str, path: &str, sql: &str| json!({"id":id,"path":path,"sql":sql,"format":"csv","encoding":"utf-8","mode":"overwrite"});
    let processes = if process {
        json!([{"id":"intermediate","sql":sql}])
    } else {
        json!([])
    };
    let sql = if process {
        "SELECT * FROM intermediate"
    } else {
        sql
    };
    let job = json!({
        "input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8","fields":[{"name":"value"}]}],
        "process":processes,
        "output":[output("first", "first.csv", "SELECT * FROM data"), output("result", "out.csv", sql)],
    });
    fs::write(dir.join("job.yml"), job.to_string()).unwrap();
}

fn invoke(dir: &Path, command: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(dir.join("job.yml"))
        .args(["--threads", "1", "--temp-dir"])
        .arg(dir.join("work"))
        .output()
        .unwrap()
}

fn assert_untouched(dir: &Path) {
    assert_eq!(
        fs::read_to_string(dir.join("first.csv")).unwrap(),
        "keep first"
    );
    assert_eq!(
        fs::read_to_string(dir.join("out.csv")).unwrap(),
        "keep second"
    );
    assert!(!dir.join("work").exists());
}

#[test]
fn reserved_identifiers_fail_before_any_output_is_created() {
    let dir = setup();
    for sql in [
        "SELECT value, value AS dxt$secret FROM data",
        "SELECT value AS dxt$ FROM data",
        "SELECT value AS DXT$secret FROM data",
        "SELECT value AS \"DxT$secret\" FROM data",
        "SELECT value AS `dxt$secret` FROM data",
        "SELECT value AS 'dxt$secret' FROM data",
        "SELECT value 'DxT$secret' FROM data",
        "SELECT t.\"dxt$secret\" AS value FROM (SELECT value AS \"dxt$secret\" FROM data) AS t",
        "WITH t(\"dxt$secret\") AS (SELECT value FROM data) SELECT * FROM t",
        "WITH \"dxt$shadow\" AS (SELECT value FROM data) SELECT value FROM data",
        "WITH 'dxt$unused' AS (SELECT value FROM data) SELECT value FROM data",
        "SELECT * FROM data AS t('dxt$hidden')",
        "WITH unused AS (SELECT value AS dxt$secret FROM data) SELECT value FROM data",
        "SELECT value FROM data UNION ALL SELECT value AS dxt$secret FROM data",
        "SELECT (SELECT value AS dxt$secret FROM data LIMIT 1) AS value FROM data",
        "SELECT value FROM data AS 'dxt$set_all_1'",
        "SELECT value FROM (SELECT value FROM data) AS 'DXT$set_all_1'",
        "SELECT \"dxt$order_1\" FROM data",
    ] {
        for process in [false, true] {
            write_job(dir.path(), sql, process);
            for command in ["check", "explain", "run"] {
                let output = invoke(dir.path(), command);
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert_eq!(output.status.code(), Some(4), "{command}: {sql}\n{stderr}");
                assert!(
                    stderr.contains("reserved internal prefix 'dxt$'"),
                    "{sql}\n{stderr}"
                );
                assert!(
                    stderr.contains(if process {
                        "process intermediate"
                    } else {
                        "output result"
                    }),
                    "{stderr}"
                );
                assert_untouched(dir.path());
            }
        }
    }
}

#[test]
fn literals_comments_and_nonreserved_aliases_keep_their_values() {
    let dir = setup();
    for (sql, expected) in [
        (
            "SELECT 'dxt$literal' AS value FROM data LIMIT 1 -- dxt$order_1",
            "value\ndxt$literal\n",
        ),
        (
            "SELECT 'DXT$literal' AS value /* dxt$set_all_1 */ FROM data LIMIT 1",
            "value\nDXT$literal\n",
        ),
        (
            "SELECT $$dxt$literal$$ AS value FROM data LIMIT 1",
            "value\ndxt$literal\n",
        ),
        (
            "SELECT t.a$b AS value FROM (SELECT value AS a$b FROM data) AS t",
            "value\nfirst\nsecond\nfirst\n",
        ),
        (
            "SELECT t.\"$x\" AS value FROM (SELECT value AS \"$x\" FROM data) AS t",
            "value\nfirst\nsecond\nfirst\n",
        ),
        (
            "SELECT t.adxt$value AS value FROM (SELECT value AS adxt$value FROM data) AS t",
            "value\nfirst\nsecond\nfirst\n",
        ),
        (
            "SELECT t.__dxt_user AS value FROM (SELECT value AS __dxt_user FROM data) AS t",
            "value\nfirst\nsecond\nfirst\n",
        ),
    ] {
        for process in [false, true] {
            write_job(dir.path(), sql, process);
            let output = invoke(dir.path(), "run");
            assert!(
                output.status.success(),
                "{sql}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                expected
            );
        }
    }
}

#[test]
fn invalid_public_alias_is_reported_instead_of_silently_dropped() {
    let dir = setup();
    write_job(
        dir.path(),
        "SELECT value, value AS __dxt_user FROM data",
        false,
    );
    let output = invoke(dir.path(), "run");
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_untouched(dir.path());
}

#[test]
fn internal_columns_and_set_markers_share_the_reserved_prefix() {
    let dir = setup();
    write_job(
        dir.path(),
        "SELECT value FROM data EXCEPT ALL SELECT value FROM data WHERE value = 'second'",
        false,
    );
    let explained = invoke(dir.path(), "explain");
    assert!(
        explained.status.success(),
        "{}",
        String::from_utf8_lossy(&explained.stderr)
    );
    let plans = String::from_utf8_lossy(&explained.stdout);
    assert!(plans.contains("dxt$order_"), "{plans}");
    assert!(plans.contains("dxt$set_all_"), "{plans}");
    assert!(!plans.contains("__dxt_"), "{plans}");
    let output = invoke(dir.path(), "run");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "value\nfirst\nfirst\n"
    );
}
