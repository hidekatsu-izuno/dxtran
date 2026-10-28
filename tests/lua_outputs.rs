use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

const WRITER: &str = r#"
local function log(text)
  local file=assert(io.open(dxt.resolve_path('writer.log'),'ab'))
  file:write(text .. '\n'); file:close()
end
return {open_writer=function(options, columns, mode)
  log('open:' .. mode)
  local count=0
  return {
    write=function(row)
      count=count+1
      log('write:' .. count)
      if options.fail_write and count == 2 then error('write failed') end
    end,
    commit=function() log('commit'); if options.fail_commit then error('commit failed') end end,
    rollback=function() log('rollback') end,
    close=function() log('close') end,
  }
end}
"#;

fn input() -> Value {
    json!({"id":"data", "format":"csv", "path":"in.csv", "encoding":"utf-8", "fields":[{"name":"id","nullable":true},{"name":"value","nullable":true}]})
}
fn destination() -> Value {
    json!({"id":"result", "format":"lua", "path":"writer.lua", "sql":"SELECT * FROM data"})
}
fn setup() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("writer.lua"), WRITER).unwrap();
    fs::write(dir.path().join("in.csv"), "id,value\n1,one\n2,two\n").unwrap();
    dir
}
fn job(dir: &Path, outputs: Value) {
    fs::write(
        dir.join("job.json"),
        json!({"input":[input()], "output":outputs}).to_string(),
    )
    .unwrap();
}
fn invoke(dir: &Path, command: &str, scan: bool, threads: usize) -> Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    process
        .arg(command)
        .arg("--allow-lua")
        .arg(dir.join("job.json"))
        .args([
            "--threads",
            &threads.to_string(),
            "--memory-limit",
            "64MiB",
            "--temp-dir",
        ])
        .arg(dir.join("spill"))
        .current_dir(std::env::temp_dir());
    if scan {
        process.arg("--scan");
    }
    process.output().unwrap()
}
fn status(output: Output, code: i32) -> Output {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn log(dir: &Path) -> String {
    fs::read_to_string(dir.join("writer.log")).unwrap()
}

#[test]
fn metadata_and_scan_never_open_output_writers_and_success_commits_once() {
    for threads in [1, 4] {
        let dir = setup();
        job(dir.path(), json!([destination()]));
        for (command, scan) in [("check", false), ("explain", false), ("check", true)] {
            status(invoke(dir.path(), command, scan, threads), 0);
            assert!(!dir.path().join("writer.log").exists());
        }
        let script = fs::read(dir.path().join("writer.lua")).unwrap();
        status(invoke(dir.path(), "run", false, threads), 0);
        assert_eq!(
            log(dir.path()),
            "open:append\nwrite:1\nwrite:2\ncommit\nclose\n"
        );
        assert_eq!(fs::read(dir.path().join("writer.lua")).unwrap(), script);
        assert_eq!(fs::read_dir(dir.path().join("spill")).unwrap().count(), 0);
    }
}

#[test]
fn writer_failure_rolls_back_and_closes_once() {
    for (key, expected) in [
        (
            "fail_write",
            "open:merge\nwrite:1\nwrite:2\nrollback\nclose\n",
        ),
        (
            "fail_commit",
            "open:merge\nwrite:1\nwrite:2\ncommit\nrollback\nclose\n",
        ),
    ] {
        let dir = setup();
        let mut output = destination();
        output["mode"] = json!("merge");
        output["lua"] = json!({"options":{key:true}});
        job(dir.path(), json!([output]));
        let result = status(invoke(dir.path(), "run", false, 4), 5);
        assert!(String::from_utf8_lossy(&result.stderr).contains("failed_external=result"));
        assert_eq!(log(dir.path()), expected);
        assert_eq!(fs::read_dir(dir.path().join("spill")).unwrap().count(), 0);
    }
}

#[test]
fn all_checks_and_file_conversions_finish_before_any_external_write() {
    for invalid in [
        json!({"id":"bad","format":"fixed","path":"bad.dat","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"value","length":1}],"sql":"SELECT value FROM data"}),
        json!({"id":"bad","format":"csv","path":"bad.csv","encoding":"utf-8","sql":"SELECT * FROM data","check":{"when":"id = '2'","level":"error","message":"no publication"}}),
    ] {
        let dir = setup();
        job(dir.path(), json!([destination(), invalid]));
        status(invoke(dir.path(), "run", false, 4), 5);
        assert!(!dir.path().join("writer.log").exists());
        assert!(!dir.path().join("bad.csv").exists());
        assert!(!dir.path().join("bad.dat").exists());
    }
}

#[test]
fn cleanup_errors_report_commit_state_and_preserve_the_original_write_error() {
    for (script, options, expected) in [
        (
            WRITER.replace("log('close')", "log('close'); error('close failed')"),
            json!({}),
            "commit succeeded",
        ),
        (
            WRITER.replace(
                "log('rollback')",
                "log('rollback'); error('rollback failed')",
            ),
            json!({"fail_write":true}),
            "write failed",
        ),
        (
            WRITER.replace("write=function(row)", "write=false, unused=function(row)"),
            json!({}),
            "lua record 1",
        ),
    ] {
        let dir = setup();
        fs::write(dir.path().join("writer.lua"), script).unwrap();
        let mut output = destination();
        output["lua"] = json!({"options":options});
        job(dir.path(), json!([output]));
        let result = status(invoke(dir.path(), "run", false, 4), 5);
        let message = String::from_utf8_lossy(&result.stderr);
        assert!(message.contains(expected), "{message}");
        let events = log(dir.path());
        assert_eq!(events.matches("close\n").count(), 1);
        if expected == "commit succeeded" {
            assert!(events.contains("commit\n"));
            assert!(!events.contains("rollback\n"));
        } else {
            assert_eq!(events.matches("rollback\n").count(), 1);
        }
    }
}

#[test]
fn publication_error_reports_prior_commits_and_stops_later_outputs() {
    let dir = setup();
    let first = json!({"id":"first","format":"csv","path":"first.csv","encoding":"utf-8","sql":"SELECT * FROM data"});
    let mut failed = destination();
    failed["lua"] = json!({"options":{"fail_write":true}});
    let last = json!({"id":"last","format":"csv","path":"last.csv","encoding":"utf-8","sql":"SELECT * FROM data"});
    job(dir.path(), json!([first, failed, last]));
    let result = status(invoke(dir.path(), "run", false, 4), 5);
    let events: Vec<Value> = String::from_utf8_lossy(&result.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    let message = events
        .iter()
        .find(|event| event["event"] == "error")
        .unwrap()["message"]
        .as_str()
        .unwrap();
    assert!(message.contains("published=[(\"first\""));
    assert!(message.contains("not_started=[\"last\"]"));
    assert!(dir.path().join("first.csv").exists());
    assert!(!dir.path().join("last.csv").exists());
    assert_eq!(
        log(dir.path()),
        "open:append\nwrite:1\nwrite:2\nrollback\nclose\n"
    );
}

#[test]
fn output_values_and_schema_preserve_exact_types() {
    let dir = setup();
    fs::write(
        dir.path().join("writer.lua"),
        r#"
return {open_writer=function(options, columns, mode)
  assert(columns[1].type == 'int64')
  assert(columns[2].type == 'uint64')
  assert(columns[3].type == 'decimal' and columns[3].scale == 2)
  assert(columns[4].type == 'binary')
  assert(columns[5].type == 'boolean')
  assert(columns[6].type == 'float64')
  return {write=function(row)
    assert(row.i == '-9223372036854775808')
    assert(row.u == '18446744073709551615')
    assert(row.d == '123.40')
    assert(row.b == '\0\255')
    assert(row.flag == false)
    assert(row.f == 1.5)
    assert(row.empty == '' and row.n == nil)
    assert(row.day == '2026-10-28')
    assert(row.clock == '12:34:56.123456')
    assert(row.stamp == '2026-10-28T12:34:56.123456')
  end,commit=function()end,rollback=function()end}
end}
"#,
    )
    .unwrap();
    let mut output = destination();
    output["sql"] = json!(
        "SELECT CAST('-9223372036854775808' AS BIGINT) AS i, CAST('18446744073709551615' AS BIGINT UNSIGNED) AS u, CAST('123.40' AS DECIMAL(9,2)) AS d, decode('00ff','hex') AS b, false AS flag, CAST(1.5 AS DOUBLE) AS f, '' AS empty, CAST(NULL AS VARCHAR) AS n, DATE '2026-10-28' AS day, CAST('12:34:56.123456' AS TIME) AS clock, TIMESTAMP '2026-10-28T12:34:56.123456' AS stamp FROM data LIMIT 1"
    );
    job(dir.path(), json!([output]));
    status(invoke(dir.path(), "run", false, 4), 0);
}

#[test]
fn invalid_output_modes_fields_apis_and_script_collisions_fail_before_writing() {
    let dir = setup();
    for invalid in [
        json!({"mode":"error_if_exists"}),
        json!({"fields":[{"name":"id"}]}),
        json!({"encoding":"utf-8"}),
        json!({"mode":"bogus"}),
    ] {
        let mut output = destination();
        output
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        job(dir.path(), json!([output]));
        status(invoke(dir.path(), "check", false, 1), 2);
        assert!(!dir.path().join("writer.log").exists());
    }
    job(
        dir.path(),
        json!([destination(), {"id":"bad","format":"csv","path":"writer.lua","mode":"overwrite","encoding":"utf-8","sql":"SELECT * FROM data"}]),
    );
    status(invoke(dir.path(), "run", false, 1), 2);
    assert_eq!(
        fs::read_to_string(dir.path().join("writer.lua")).unwrap(),
        WRITER
    );
    for script in ["return {}", "return {open_writer=false}"] {
        fs::write(dir.path().join("writer.lua"), script).unwrap();
        job(dir.path(), json!([destination()]));
        status(invoke(dir.path(), "check", false, 1), 2);
    }
}

#[cfg(target_os = "linux")]
mod sqlite {
    use super::*;
    fn python(dir: &Path, code: &str) -> Output {
        let output = Command::new("python3")
            .arg("-c")
            .arg(code)
            .current_dir(dir)
            .output()
            .unwrap();
        status(output, 0)
    }
    fn setup_sqlite() -> tempfile::TempDir {
        let dir = setup();
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sqlite/sqlite.lua"),
            dir.path().join("writer.lua"),
        )
        .unwrap();
        python(
            dir.path(),
            "import sqlite3; db=sqlite3.connect('db.sqlite'); db.executescript(\"CREATE TABLE target (id TEXT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO target VALUES ('1','old'),('9','keep')\"); db.close()",
        );
        dir
    }
    fn output(mode: &str) -> Value {
        let mut output = destination();
        output["mode"] = json!(mode);
        output["lua"] = json!({"libraries":{"sqlite3":"libsqlite3.so.0"},"options":{"database":"db.sqlite","table":"target","keys":["id"]}});
        output
    }
    fn rows(dir: &Path) -> Value {
        let output = python(
            dir,
            "import sqlite3,json; db=sqlite3.connect('db.sqlite'); print(json.dumps(db.execute('SELECT * FROM target ORDER BY id').fetchall())); db.close()",
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
    #[test]
    fn append_overwrite_and_merge_use_transactions_and_keep_the_table_schema() {
        for threads in [1, 4] {
            for mode in ["append", "overwrite", "merge"] {
                let dir = setup_sqlite();
                if mode == "append" {
                    fs::write(dir.path().join("in.csv"), "id,value\n2,two\n").unwrap();
                }
                job(dir.path(), json!([output(mode)]));
                let original = rows(dir.path());
                for (command, scan) in [("check", false), ("explain", false), ("check", true)] {
                    status(invoke(dir.path(), command, scan, threads), 0);
                    assert_eq!(rows(dir.path()), original);
                }
                status(invoke(dir.path(), "run", false, threads), 0);
                assert_eq!(
                    rows(dir.path()),
                    match mode {
                        "append" => json!([["1", "old"], ["2", "two"], ["9", "keep"]]),
                        "overwrite" => json!([["1", "one"], ["2", "two"]]),
                        _ => json!([["1", "one"], ["2", "two"], ["9", "keep"]]),
                    }
                );
                if mode != "append" {
                    status(invoke(dir.path(), "run", false, threads), 0);
                }
            }
        }
    }
    #[test]
    fn constraint_failure_rolls_back_all_rows_including_overwrite_delete() {
        for mode in ["append", "overwrite", "merge"] {
            let dir = setup_sqlite();
            fs::write(dir.path().join("in.csv"), "id,value\n2,two\n3,\n").unwrap();
            job(dir.path(), json!([output(mode)]));
            let original = rows(dir.path());
            status(invoke(dir.path(), "run", false, 4), 5);
            assert_eq!(rows(dir.path()), original, "{mode}");
        }
    }
    #[test]
    fn multiple_outputs_and_same_database_input_are_staged_before_mutation() {
        let dir = setup_sqlite();
        let mut second = output("append");
        second["id"] = json!("second");
        second["sql"] = json!("SELECT '3' AS id, 'three' AS value FROM data LIMIT 1");
        job(dir.path(), json!([output("overwrite"), second]));
        status(invoke(dir.path(), "run", false, 4), 0);
        assert_eq!(
            rows(dir.path()),
            json!([["1", "one"], ["2", "two"], ["3", "three"]])
        );
        let source = json!({"id":"data","format":"lua","path":"writer.lua","lua":{"libraries":{"sqlite3":"libsqlite3.so.0"},"options":{"database":"db.sqlite","query":"SELECT id,value FROM target ORDER BY id"}},"fields":[{"name":"id"},{"name":"value"}]});
        let mut destination = output("overwrite");
        destination["sql"] = json!("SELECT id, value || '!' AS value FROM data");
        fs::write(
            dir.path().join("job.json"),
            json!({"input":[source],"output":[destination]}).to_string(),
        )
        .unwrap();
        status(invoke(dir.path(), "run", false, 4), 0);
        assert_eq!(
            rows(dir.path()),
            json!([["1", "one!"], ["2", "two!"], ["3", "three!"]])
        );
    }
    #[test]
    fn merge_keys_are_validated_and_null_keys_fail_without_changes() {
        let dir = setup_sqlite();
        for keys in [json!([]), json!(["missing"]), json!(["id", "id"])] {
            let mut output = output("merge");
            output["lua"]["options"]["keys"] = keys;
            job(dir.path(), json!([output]));
            status(invoke(dir.path(), "check", false, 1), 2);
        }
        fs::write(dir.path().join("in.csv"), "id,value\n1,new\n,two\n").unwrap();
        job(dir.path(), json!([output("merge")]));
        let original = rows(dir.path());
        status(invoke(dir.path(), "run", false, 4), 5);
        assert_eq!(rows(dir.path()), original);
    }

    #[test]
    fn sqlite_bindings_keep_integer_limits_null_empty_blob_and_decimal_text() {
        let dir = setup_sqlite();
        python(
            dir.path(),
            "import sqlite3; db=sqlite3.connect('db.sqlite'); db.execute('CREATE TABLE typed (lo INTEGER, hi INTEGER, score REAL, flag INTEGER, bytes BLOB, empty TEXT, optional TEXT, amount TEXT)'); db.close()",
        );
        let mut output = output("overwrite");
        output["lua"]["options"]["table"] = json!("typed");
        output["sql"] = json!(
            "SELECT CAST('-9223372036854775808' AS BIGINT) AS lo, CAST('9223372036854775807' AS BIGINT) AS hi, CAST(1.5 AS DOUBLE) AS score, false AS flag, decode('00ff','hex') AS bytes, '' AS empty, CAST(NULL AS VARCHAR) AS optional, CAST('123.40' AS DECIMAL(9,2)) AS amount FROM data LIMIT 1"
        );
        job(dir.path(), json!([output]));
        status(invoke(dir.path(), "run", false, 4), 0);
        let result = python(
            dir.path(),
            "import sqlite3,json; db=sqlite3.connect('db.sqlite'); print(json.dumps(db.execute('SELECT lo,hi,score,flag,hex(bytes),empty,optional,amount FROM typed').fetchall())); db.close()",
        );
        let actual: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(
            actual,
            json!([[
                -9223372036854775808i64,
                9223372036854775807i64,
                1.5,
                0,
                "00FF",
                "",
                null,
                "123.40"
            ]])
        );
    }

    #[test]
    fn composite_keys_preserve_columns_not_in_the_query_and_empty_overwrite_clears_rows() {
        let dir = setup_sqlite();
        python(
            dir.path(),
            "import sqlite3; db=sqlite3.connect('db.sqlite'); db.executescript(\"CREATE TABLE composite (a TEXT,b TEXT,value TEXT,preserved TEXT DEFAULT 'default',PRIMARY KEY(a,b)); INSERT INTO composite VALUES ('1','x','old','original'),('9','z','keep','kept')\"); db.close()",
        );
        let mut output = output("merge");
        output["lua"]["options"]["table"] = json!("composite");
        output["lua"]["options"]["keys"] = json!(["a", "b"]);
        output["sql"] = json!("SELECT id AS a, 'x' AS b, value FROM data");
        job(dir.path(), json!([output.clone()]));
        status(invoke(dir.path(), "run", false, 4), 0);
        let result = python(
            dir.path(),
            "import sqlite3,json; db=sqlite3.connect('db.sqlite'); print(json.dumps(db.execute('SELECT * FROM composite ORDER BY a,b').fetchall())); db.close()",
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&result.stdout).unwrap(),
            json!([
                ["1", "x", "one", "original"],
                ["2", "x", "two", "default"],
                ["9", "z", "keep", "kept"]
            ])
        );
        output["mode"] = json!("overwrite");
        output["sql"] = json!("SELECT id AS a, 'x' AS b, value FROM data WHERE false");
        job(dir.path(), json!([output]));
        status(invoke(dir.path(), "run", false, 4), 0);
        python(
            dir.path(),
            "import sqlite3; db=sqlite3.connect('db.sqlite'); assert db.execute('SELECT COUNT(*) FROM composite').fetchone()[0] == 0; db.close()",
        );
    }

    #[test]
    fn documented_write_and_readback_examples_match_expected_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sqlite");
        fs::create_dir(dir.path().join("data")).unwrap();
        fs::create_dir(dir.path().join("out")).unwrap();
        for name in [
            "sqlite.lua",
            "create_db.py",
            "write.yml",
            "write_output.yml",
            "readback_writes.yml",
        ] {
            fs::copy(example.join(name), dir.path().join(name)).unwrap();
        }
        fs::copy(
            example.join("data/customer_updates.csv"),
            dir.path().join("data/customer_updates.csv"),
        )
        .unwrap();
        for threads in [1, 4] {
            status(
                Command::new("python3")
                    .arg(dir.path().join("create_db.py"))
                    .output()
                    .unwrap(),
                0,
            );
            for file in ["write.yml", "readback_writes.yml"] {
                for (command, scan) in [
                    ("check", false),
                    ("explain", false),
                    ("check", true),
                    ("run", false),
                ] {
                    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
                    process
                        .arg(command)
                        .arg("--allow-lua")
                        .arg(dir.path().join(file))
                        .args(["--threads", &threads.to_string()]);
                    if scan {
                        process.arg("--scan");
                    }
                    status(process.output().unwrap(), 0);
                }
            }
            for name in ["appended.csv", "overwritten.csv", "merged.csv"] {
                assert_eq!(
                    fs::read(dir.path().join("out").join(name)).unwrap(),
                    fs::read(example.join("expected").join(name)).unwrap()
                );
            }
        }
    }
}
