use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn rule(when: &str, level: &str, message: &str) -> Value {
    json!({"when":when,"level":level,"message":message})
}
fn job() -> Value {
    json!({"input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8",
        "fields":[{"name":"id"},{"name":"n","type":"decimal","precision":8,"nullable":true}]}],
        "output":[{"id":"result","path":"out.csv","format":"csv","encoding":"utf-8","mode":"overwrite","sql":"SELECT * FROM data"}]})
}
fn invoke(root: &Path, job: &Value, command: &str, scan: bool, threads: usize) -> Output {
    fs::create_dir_all(root.join("tmp")).unwrap();
    fs::write(root.join("job.json"), job.to_string()).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_dxt"));
    if job["input"][0]["format"] == "lua" {
        cmd.arg("--allow-lua");
    }
    cmd.arg(command)
        .arg(root.join("job.json"))
        .args(["--threads", &threads.to_string(), "--temp-dir"])
        .arg(root.join("tmp"));
    if scan {
        cmd.arg("--scan");
    }
    cmd.output().unwrap()
}
fn success(output: Output) -> Output {
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
        .filter_map(|s| serde_json::from_str::<Value>(s).ok())
        .filter(|v| v["event"] == "check")
        .collect()
}

#[test]
fn all_four_boundaries_run_once_and_share_checked_data_across_outputs() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,n\na,1\nb,2\nc,\n").unwrap();
    let mut config = job();
    config["input"][0]["check"] = rule("TRUE", "info", "row");
    config["input"][0]["after"] = rule("COUNT(*) = 3 AND SUM(n) = 3", "info", "input-after");
    config["process"] = json!([{"id":"p","sql":"SELECT * FROM data WHERE n IS NOT NULL",
        "before":rule("SUM(n) = 3", "info", "process-before"),
        "after":rule("COUNT(*) = 2 AND AVG(n) = 1.5", "info", "process-after")}]);
    config["output"][0]["sql"] = json!("SELECT * FROM p");
    config["output"][0]["before"] = rule(
        "SUM(n) = 3 AND MIN(n) = 1 AND MAX(n) = 2",
        "info",
        "output-before",
    );
    let mut second = config["output"][0].clone();
    second["id"] = json!("second");
    second["path"] = json!("second.csv");
    second.as_object_mut().unwrap().remove("before");
    config["output"].as_array_mut().unwrap().push(second);
    for threads in [1, 4] {
        let output = success(invoke(dir.path(), &config, "run", false, threads));
        let checks = events(&output.stdout);
        assert_eq!(checks.iter().filter(|v| v["message"] == "row").count(), 3);
        let aggregates: Vec<_> = checks
            .iter()
            .filter(|v| v["aggregate"] == true)
            .map(|v| v["message"].as_str().unwrap())
            .collect();
        assert_eq!(
            aggregates,
            [
                "input-after",
                "process-before",
                "process-after",
                "output-before"
            ]
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "id,n\na,1\nb,2\n"
        );
        assert_eq!(
            fs::read(dir.path().join("out.csv")).unwrap(),
            fs::read(dir.path().join("second.csv")).unwrap()
        );
        assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }
}

#[test]
fn empty_data_is_checked_and_scan_runs_only_input_after() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,n\n").unwrap();
    let mut config = job();
    config["input"][0]["after"] = rule("COUNT(*) = 0", "error", "empty-input");
    for command in ["check", "explain"] {
        let output = success(invoke(dir.path(), &config, command, false, 4));
        assert!(events(&output.stdout).is_empty());
    }
    for (command, scan) in [("check", true), ("run", false)] {
        let output = invoke(dir.path(), &config, command, scan, 4);
        assert_eq!(
            output.status.code(),
            Some(3),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("input data after check 1"));
        assert!(!dir.path().join("out.csv").exists());
        assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
    }
    config["input"][0]["after"] = json!([
        rule("SUM(n) < 0", "error", "null-is-not-true"),
        rule("COUNT(*) = 0", "info", "zero")
    ]);
    config["output"][0]["before"] = rule("COUNT(*) = 0", "error", "empty-output");
    let output = success(invoke(dir.path(), &config, "check", true, 1));
    assert_eq!(events(&output.stdout)[0]["rows"], 0);
    let output = invoke(dir.path(), &config, "run", false, 1);
    assert_eq!(output.status.code(), Some(5));
    assert!(!dir.path().join("out.csv").exists());
}

#[test]
fn process_before_precedes_its_sql_and_failures_keep_all_destinations() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,n\na,12\n").unwrap();
    let mut config = job();
    config["process"] = json!([{"id":"p","sql":"SELECT CAST(id AS BIGINT) AS id, n FROM data",
        "before":rule("COUNT(*) > 0", "error", "before-sql")}]);
    config["output"][0]["sql"] = json!("SELECT * FROM p");
    let output = invoke(dir.path(), &config, "run", false, 4);
    assert_eq!(output.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&output.stderr).contains("before-sql"));
    config["process"][0]["sql"] = json!("SELECT * FROM data");
    config["process"][0]
        .as_object_mut()
        .unwrap()
        .remove("before");
    config["process"][0]["after"] = rule("COUNT(*) > 0", "error", "after-sql");
    let output = invoke(dir.path(), &config, "run", false, 1);
    assert_eq!(output.status.code(), Some(4));
    assert!(String::from_utf8_lossy(&output.stderr).contains("after-sql"));
    config.as_object_mut().unwrap().remove("process");
    config["output"][0]["sql"] = json!("SELECT * FROM data");
    let mut second = config["output"][0].clone();
    second["id"] = json!("second");
    second["path"] = json!("second.csv");
    second["fields"] = json!([{"name":"n","precision":1}]);
    second["before"] = rule("COUNT(*) > 0", "error", "before-format");
    config["output"].as_array_mut().unwrap().push(second);
    for name in ["out.csv", "second.csv"] {
        fs::write(dir.path().join(name), "previous").unwrap();
    }
    let output = invoke(dir.path(), &config, "run", false, 4);
    assert_eq!(output.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&output.stderr).contains("before-format"));
    for name in ["out.csv", "second.csv"] {
        assert_eq!(
            fs::read_to_string(dir.path().join(name)).unwrap(),
            "previous"
        );
    }
    assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
}

#[test]
fn before_sources_are_explicit_for_joins_and_all_rules_are_validated() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,n\na,1\nb,2\n").unwrap();
    let mut config = job();
    let mut other = config["input"][0].clone();
    other["id"] = json!("other");
    config["input"].as_array_mut().unwrap().push(other);
    let mut check = rule("COUNT(*) = 2", "warn", "source");
    check["from"] = json!("other");
    config["process"] = json!([{"id":"p","sql":"SELECT d.id, d.n FROM data d JOIN other o ON d.id=o.id","before":[check]}]);
    config["output"][0]["sql"] = json!("SELECT * FROM p");
    let output = success(invoke(dir.path(), &config, "run", false, 4));
    assert_eq!(events(&output.stderr)[0]["from"], "other");
    config["process"][0]["before"][0]
        .as_object_mut()
        .unwrap()
        .remove("from");
    assert_eq!(
        invoke(dir.path(), &config, "check", false, 4).status.code(),
        Some(2)
    );
    for expression in [
        "n > 0",
        "SUM(n)",
        "SUM(n) OVER () > 0",
        "random() > 0",
        "(SELECT TRUE)",
        "COUNT(*) > 0; SELECT TRUE",
    ] {
        let mut invalid = job();
        invalid["input"][0]["after"] = rule(expression, "error", "invalid");
        assert_eq!(
            invoke(dir.path(), &invalid, "check", false, 4)
                .status
                .code(),
            Some(2),
            "{expression}"
        );
    }
}

#[test]
fn checked_boundaries_preserve_relational_order_and_empty_aggregate_keys() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "id,n\nb,2\na,1\nb,3\nc,\n").unwrap();
    for sql in [
        "SELECT id, SUM(n) AS total FROM data GROUP BY id",
        "SELECT l.id, l.n FROM data l JOIN data r ON l.id = r.id",
        "SELECT DISTINCT id FROM data",
        "SELECT id FROM data UNION ALL SELECT id FROM data",
        "SELECT id, n FROM data ORDER BY n DESC NULLS LAST LIMIT 3",
        "SELECT id, n FROM data LIMIT 0",
        "SELECT COUNT(*) AS n FROM data WHERE FALSE",
    ] {
        let mut config = job();
        config["process"] = json!([{"id":"p","sql":sql}]);
        config["output"][0]["sql"] = json!("SELECT * FROM p");
        success(invoke(dir.path(), &config, "run", false, 1));
        let expected = fs::read(dir.path().join("out.csv")).unwrap();
        config["input"][0]["after"] = rule("COUNT(*) < 0", "error", "input");
        config["process"][0]["before"] = rule("COUNT(*) < 0", "error", "before");
        config["process"][0]["after"] = rule("COUNT(*) < 0", "error", "after");
        config["output"][0]["before"] = rule("COUNT(*) < 0", "error", "output");
        for threads in [1, 4] {
            success(invoke(dir.path(), &config, "run", false, threads));
            assert_eq!(
                fs::read(dir.path().join("out.csv")).unwrap(),
                expected,
                "{sql}"
            );
        }
    }
}

#[test]
fn lua_cursors_are_consumed_once_before_all_checked_consumers() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("input.lua"),
        r#"
return { open = function(options)
  local f = assert(io.open(dxt.resolve_path('reads.log'), 'ab'))
  f:write('open\n'); f:close()
  local i = 0
  return {
    next = function()
      i = i + 1
      if i <= 2 then return {id = tostring(i)} end
    end,
    close = function()
      local f = assert(io.open(dxt.resolve_path('reads.log'), 'ab'))
      f:write('close\n'); f:close()
    end
  }
end }
"#,
    )
    .unwrap();
    let mut config = job();
    config["input"] =
        json!([{"id":"data","format":"lua","path":"input.lua","fields":[{"name":"id"}]}]);
    config["process"] = json!([{"id":"p","sql":"SELECT * FROM data",
        "before":rule("COUNT(*) <> 2","error","before")}]);
    config["output"][0]["sql"] = json!("SELECT l.id FROM p l JOIN p r ON l.id = r.id");
    config["output"][0]["before"] = rule("COUNT(*) <> 2", "error", "output");
    let mut second = config["output"][0].clone();
    second["id"] = json!("second");
    second["path"] = json!("second.csv");
    config["output"].as_array_mut().unwrap().push(second);
    for with_after in [false, true] {
        if with_after {
            config["input"][0]["after"] = rule("COUNT(*) <> 2", "error", "input");
            config["process"][0]["after"] = rule("COUNT(*) <> 2", "error", "process");
        }
        for threads in [1, 4] {
            fs::write(dir.path().join("reads.log"), "").unwrap();
            success(invoke(dir.path(), &config, "run", false, threads));
            assert_eq!(
                fs::read_to_string(dir.path().join("reads.log")).unwrap(),
                "open\nclose\n"
            );
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "id\n1\n2\n"
            );
            assert_eq!(
                fs::read(dir.path().join("out.csv")).unwrap(),
                fs::read(dir.path().join("second.csv")).unwrap()
            );
            assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
        }
    }
}
