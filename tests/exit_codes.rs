use dxtran::config;
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn job() -> Value {
    json!({"input":[{"id":"data","format":"csv","path":"in.csv","encoding":"utf-8","fields":[{"name":"value"}]}],
        "output":[{"id":"result","format":"csv","path":"out.csv","encoding":"utf-8","mode":"overwrite","sql":"SELECT * FROM data"}]})
}
fn invoke(dir: &Path, config: &Value, command: &str, extra: &[&str]) -> Output {
    fs::write(dir.join("job.yml"), config.to_string()).unwrap();
    invoke_file(dir, command, extra)
}
fn invoke_file(dir: &Path, command: &str, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(dir.join("job.yml"))
        .args(["--threads", "1", "--temp-dir"])
        .arg(dir.join("tmp"))
        .args(extra)
        .output()
        .unwrap()
}
fn events(output: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
fn assert_finish(output: &Output, original: u8, mapped: u8) {
    assert_eq!(
        output.status.code(),
        Some(i32::from(mapped)),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events = events(output);
    let finish = events
        .iter()
        .find(|event| event["event"] == "finish")
        .unwrap();
    assert_eq!(finish["exit_code"], json!(mapped));
    assert_eq!(finish["original_exit_code"], json!(original));
}

#[test]
fn every_standard_exit_code_can_be_replaced_without_changing_error_phases() {
    let dir = tempfile::tempdir().unwrap();
    for original in [0, 2, 3, 4, 5] {
        fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
        fs::write(dir.path().join("out.csv"), "previous").unwrap();
        let mut config = job();
        config["exit_codes"] = json!({"0":10,"2":12,"3":13,"4":14,"5":15});
        match original {
            2 => {
                config["input"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("encoding");
            }
            3 => {
                fs::write(dir.path().join("in.csv"), "value\n\"unclosed\n").unwrap();
            }
            4 => {
                config["output"][0]["sql"] = json!("SELECT missing FROM data");
            }
            5 => {
                config["output"][0]["format"] = json!("fixed");
                config["output"][0]["fixed"] = json!({"record_separator":"none"});
                config["output"][0]["fields"] = json!([{"name":"value","length":1}]);
            }
            _ => (),
        }
        let output = invoke(dir.path(), &config, "run", &[]);
        assert_finish(&output, original, original + 10);
        if original == 0 {
            assert_eq!(
                fs::read(dir.path().join("out.csv")).unwrap(),
                b"value\nAB\n"
            );
        } else {
            let phase = match original {
                2 => "configuration",
                3 => "input",
                4 => "sql",
                _ => "output",
            };
            assert!(
                events(&output)
                    .iter()
                    .any(|event| event["event"] == "error" && event["phase"] == phase)
            );
            assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), b"previous");
        }
    }
}

#[test]
fn replacements_apply_once_and_unspecified_codes_keep_their_value() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
    let mut config = job();
    config["exit_codes"] = json!({"0":3,"3":5,"5":255});
    assert_finish(&invoke(dir.path(), &config, "run", &[]), 0, 3);
    fs::write(dir.path().join("in.csv"), "value\n\"unclosed\n").unwrap();
    assert_finish(&invoke(dir.path(), &config, "run", &[]), 3, 5);
    fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
    config["output"][0]["check"] = json!({"when":"true","level":"error","message":"stop"});
    assert_finish(&invoke(dir.path(), &config, "run", &[]), 5, 255);
    config["exit_codes"] = json!({"3":8});
    assert_finish(&invoke(dir.path(), &config, "run", &[]), 5, 5);
    config.as_object_mut().unwrap().remove("exit_codes");
    assert_finish(&invoke(dir.path(), &config, "run", &[]), 5, 5);
}

#[test]
fn mapping_an_error_to_zero_still_aborts_and_preserves_all_outputs() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\n\"unclosed\n").unwrap();
    for name in ["first.csv", "out.csv"] {
        fs::write(dir.path().join(name), b"previous").unwrap();
    }
    let mut config = job();
    let mut first = config["output"][0].clone();
    first["id"] = json!("first");
    first["path"] = json!("first.csv");
    first["sql"] = json!("SELECT 'first' AS value");
    config["output"].as_array_mut().unwrap().insert(0, first);
    config["exit_codes"] = json!({"0":9,"3":0});
    let output = invoke(dir.path(), &config, "run", &[]);
    assert_finish(&output, 3, 0);
    assert!(
        events(&output)
            .iter()
            .any(|event| event["event"] == "output_staged" && event["output"] == "first")
    );
    assert!(
        events(&output)
            .iter()
            .any(|event| event["event"] == "error" && event["phase"] == "input")
    );
    for name in ["first.csv", "out.csv"] {
        assert_eq!(fs::read(dir.path().join(name)).unwrap(), b"previous");
    }
    assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
}

#[test]
fn check_explain_and_run_use_the_same_mapping() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
    let mut config = job();
    config["exit_codes"] = json!({"0":17,"3":23});
    for (command, extra) in [
        ("check", vec![]),
        ("check", vec!["--scan"]),
        ("explain", vec![]),
        ("run", vec![]),
    ] {
        assert_finish(&invoke(dir.path(), &config, command, &extra), 0, 17);
    }
    fs::write(dir.path().join("in.csv"), "value\n\"unclosed\n").unwrap();
    assert_finish(&invoke(dir.path(), &config, "check", &[]), 0, 17);
    assert_finish(&invoke(dir.path(), &config, "explain", &[]), 0, 17);
    assert_finish(&invoke(dir.path(), &config, "check", &["--scan"]), 3, 23);
    assert_finish(&invoke(dir.path(), &config, "run", &[]), 3, 23);
}

#[test]
fn imported_maps_merge_and_change_the_expanded_hash() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\n\"unclosed\n").unwrap();
    let mut base = job();
    base["exit_codes"] = json!({"2":22,"3":33});
    fs::write(dir.path().join("base.yml"), base.to_string()).unwrap();
    fs::write(
        dir.path().join("job.yml"),
        "import: base.yml\nexit_codes: {\"0\": 7, \"3\": 44}\n",
    )
    .unwrap();
    let loaded = config::load(&dir.path().join("job.yml")).unwrap();
    assert_eq!(loaded.exit_codes.resolve(0), 7);
    assert_eq!(loaded.exit_codes.resolve(2), 22);
    assert_eq!(loaded.exit_codes.resolve(3), 44);
    assert_eq!(loaded.exit_codes.resolve(4), 4);
    assert_finish(&invoke_file(dir.path(), "run", &[]), 3, 44);
    fs::write(
        dir.path().join("job.yml"),
        "import: base.yml\nexit_codes: {\"0\": 7, \"3\": 45}\n",
    )
    .unwrap();
    let changed = config::load(&dir.path().join("job.yml")).unwrap();
    assert_ne!(loaded.expanded_hash, changed.expanded_hash);
    base["input"][0].as_object_mut().unwrap().remove("encoding");
    fs::write(dir.path().join("base.yml"), base.to_string()).unwrap();
    assert_eq!(
        config::load(&dir.path().join("job.yml")).unwrap_err().code,
        2
    );
    assert_finish(&invoke_file(dir.path(), "run", &[]), 2, 22);
}

#[test]
fn invalid_maps_never_apply_partially_valid_replacements() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
    for mapping in [
        Value::Null,
        json!([]),
        json!("3: 8"),
        json!(false),
        json!({"1":0}),
        json!({"02":8}),
        json!({"6":8}),
        json!({"2":8,"3":256}),
        json!({"2":8,"3":-1}),
        json!({"2":8,"3":8.5}),
        json!({"2":8,"3":"8"}),
        json!({"2":8,"3":false}),
        json!({"2":8,"3":null}),
    ] {
        let mut config = job();
        config["exit_codes"] = mapping;
        let output = invoke(dir.path(), &config, "run", &[]);
        assert_finish(&output, 2, 2);
        assert!(String::from_utf8_lossy(&output.stderr).contains("exit_codes"));
        assert!(!dir.path().join("out.csv").exists());
    }
    fs::write(
        dir.path().join("job.yml"),
        "exit_codes: {3: 8}\ninput: []\noutput: []\n",
    )
    .unwrap();
    assert_finish(&invoke_file(dir.path(), "run", &[]), 2, 2);
    fs::write(
        dir.path().join("job.yml"),
        "exit_codes: {\"2\": 8, \"2\": 9}\ninput: []\noutput: []\n",
    )
    .unwrap();
    assert_finish(&invoke_file(dir.path(), "run", &[]), 2, 2);
}

#[test]
fn a_valid_map_applies_to_later_configuration_validation() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
    for case in 0..4 {
        let mut config = job();
        config["exit_codes"] = json!({"2":61});
        match case {
            0 => {
                config.as_object_mut().unwrap().remove("input");
            }
            1 => {
                config["unexpected"] = json!(true);
            }
            2 => {
                config["input"][0]["unexpected"] = json!(true);
            }
            _ => {
                config["input"][0]["fields"][0]["name"] = json!("bad name");
            }
        }
        assert_finish(&invoke(dir.path(), &config, "check", &[]), 2, 61);
    }
    let mut config = job();
    config["exit_codes"] = json!({"2":61});
    assert_finish(
        &invoke(dir.path(), &config, "run", &["--input.missing.path=unused"]),
        2,
        61,
    );
}

#[test]
fn errors_before_a_valid_map_is_loaded_keep_standard_codes() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\nAB\n").unwrap();
    let mut config = job();
    config["exit_codes"] = json!({"0":7,"2":61});
    let output = invoke(dir.path(), &config, "run", &["--threads", "0"]);
    assert_eq!(output.status.code(), Some(2));
    let output = invoke(dir.path(), &config, "run", &["--help"]);
    assert_eq!(output.status.code(), Some(0));
    for text in [
        "exit_codes: {\"2\": 61}\ninput: [\n",
        "import: missing.yml\nexit_codes: {\"2\": 61}\n",
    ] {
        fs::write(dir.path().join("job.yml"), text).unwrap();
        assert_finish(&invoke_file(dir.path(), "run", &[]), 2, 2);
    }
    fs::remove_file(dir.path().join("job.yml")).unwrap();
    assert_finish(&invoke_file(dir.path(), "run", &[]), 2, 2);
}

#[cfg(unix)]
#[test]
fn interruption_uses_the_loaded_map_after_cleanup() {
    use std::{
        io::Write,
        process::Stdio,
        time::{Duration, Instant},
    };
    let dir = tempfile::tempdir().unwrap();
    let mut input = fs::File::create(dir.path().join("in.csv")).unwrap();
    writeln!(input, "value").unwrap();
    for n in 0..10000 {
        writeln!(input, "{n}").unwrap();
    }
    drop(input);
    fs::write(dir.path().join("out.csv"), b"previous").unwrap();
    let mut config = job();
    config["exit_codes"] = json!({"4":33});
    config["output"][0]["sql"] =
        json!("SELECT a.value, b.value AS other FROM data a CROSS JOIN data b");
    fs::write(dir.path().join("job.yml"), config.to_string()).unwrap();
    let temp = dir.path().join("tmp");
    fs::create_dir(&temp).unwrap();
    let log = dir.path().join("execution.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(dir.path().join("job.yml"))
        .args(["--threads", "1", "--memory-limit", "16MiB", "--temp-dir"])
        .arg(&temp)
        .stdout(Stdio::null())
        .stderr(fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if fs::read_dir(&temp).unwrap().next().is_some() {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited before work started: {status}");
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("child did not start");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
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
            let _ = child.wait();
            panic!("interruption did not finish");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        status.code(),
        Some(33),
        "{}",
        fs::read_to_string(&log).unwrap()
    );
    let finish: Value = fs::read_to_string(&log)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|event| event["event"] == "finish")
        .unwrap();
    assert_eq!(finish["original_exit_code"], json!(4));
    assert_eq!(finish["exit_code"], json!(33));
    assert_eq!(fs::read(dir.path().join("out.csv")).unwrap(), b"previous");
    assert_eq!(fs::read_dir(&temp).unwrap().count(), 0);
}
