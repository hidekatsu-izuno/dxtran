use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn job() -> Value {
    json!({"input":[{"id":"data","format":"csv","path":"in.csv","encoding":"utf-8","fields":[{"name":"value"}]}],
        "output":[{"id":"result","format":"csv","path":"out.csv","mode":"overwrite","encoding":"utf-8","sql":"SELECT * FROM data"}]})
}
fn invoke(config: &Path, cwd: &Path, command: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(config)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap()
}
fn success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn save(root: &Path, config: &Value) -> std::path::PathBuf {
    let path = root.join("job.json");
    fs::write(&path, config.to_string()).unwrap();
    path
}

#[test]
fn yaml_paths_expand_typed_parameters_and_absolute_directories() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::create_dir(root.join("config")).unwrap();
    fs::write(root.join("in_2026-11-07_1.2300_true.csv"), "value\na\n").unwrap();
    let mut config = job();
    config["params"] = json!({"Root":{"type":"string"},"day":{"type":"date","default":"DATE '2026-11-07'"},
        "amount":{"type":"decimal","default":"1.2300"},"enabled":{"type":"boolean","default":true},"branch":{"type":"string"}});
    config["input"][0]["path"] = json!("${Root}/in_${day}_${amount}_${enabled}.csv");
    config["output"][0]["path"] = json!("${Root}/out_${day}_${amount}_${enabled}_${branch}.csv");
    config["output"][0]["sql"] = json!("SELECT value, ${branch} AS branch FROM data");
    let path = save(&root.join("config"), &config);
    let root_arg = format!("Root={}", root.display());
    let args = ["-p", &root_arg, "-p", "branch=東京 = O'Brien"];
    for (command, scan) in [
        ("check", false),
        ("check", true),
        ("explain", false),
        ("run", false),
    ] {
        let mut flags = args.to_vec();
        if scan {
            flags.push("--scan");
        }
        success(invoke(&path, root, command, &flags));
    }
    assert_eq!(
        fs::read_to_string(root.join("out_2026-11-07_1.2300_true_東京 = O'Brien.csv")).unwrap(),
        "value,branch\na,東京 = O'Brien\n"
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let bound = dxtran::config::load(&path)
            .unwrap()
            .with_params(&[root_arg, "branch=東京 = O'Brien".into()])
            .await
            .unwrap();
        assert_eq!(
            bound.input[0].path,
            root.join("in_2026-11-07_1.2300_true.csv")
        );
    });
}

#[test]
fn imported_paths_use_their_own_base_and_cli_overrides_expand_from_cwd() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for folder in ["config", "defs", "other", "cwd"] {
        fs::create_dir(root.join(folder)).unwrap();
    }
    fs::write(root.join("defs/path.yml"), "in_${day}.csv\n").unwrap();
    fs::write(root.join("other/path.yml"), "out_${day}.csv\n").unwrap();
    fs::write(root.join("defs/in_2026-11-07.csv"), "value\nimport\n").unwrap();
    let mut config = job();
    config["params"] = json!({"day":{"type":"date","default":"DATE '2026-11-07'"}});
    config["input"][0]["path"] = json!({"import":"../defs/path.yml"});
    config["output"][0]["path"] = json!({"import":"../other/path.yml"});
    let path = save(&root.join("config"), &config);
    success(invoke(&path, &root.join("cwd"), "run", &[]));
    assert_eq!(
        fs::read_to_string(root.join("other/out_2026-11-07.csv")).unwrap(),
        "value\nimport\n"
    );

    fs::write(root.join("cwd/in_2026-11-07.csv"), "value\ncli\n").unwrap();
    config["input"][0]["path"] = json!("${ignored}");
    config["output"][0]["path"] = json!("${unclosed");
    let path = save(&root.join("config"), &config);
    let flags = [
        "--input.data.path=in_${day}.csv",
        "--output.result.path=out_${day}.csv",
    ];
    for command in ["check", "explain", "run"] {
        success(invoke(&path, &root.join("cwd"), command, &flags));
    }
    assert_eq!(
        fs::read_to_string(root.join("cwd/out_2026-11-07.csv")).unwrap(),
        "value\ncli\n"
    );
    assert!(!root.join("config/out_2026-11-07.csv").exists());

    // An absolute parameter at the beginning of a CLI path replaces the CWD base.
    config["params"]["Root"] = json!({"type":"string"});
    let path = save(&root.join("config"), &config);
    let root_arg = format!("Root={}", root.join("defs").display());
    success(invoke(
        &path,
        &root.join("cwd"),
        "run",
        &[
            "-p",
            &root_arg,
            "--input.data.path=${Root}/in_${day}.csv",
            "--output.result.path=${Root}/cli_${day}.csv",
        ],
    ));
    assert_eq!(
        fs::read_to_string(root.join("defs/cli_2026-11-07.csv")).unwrap(),
        "value\nimport\n"
    );
}

#[test]
fn escaping_and_parameter_values_are_not_expanded_recursively() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::write(root.join("${not_declared}.csv"), "value\na\n").unwrap();
    let mut config = job();
    config["params"] = json!({"leaf":{"default":"'${not_declared}'"}});
    config["input"][0]["path"] = json!("${leaf}.csv");
    config["output"][0]["path"] = json!(r"result_\${literal}_${leaf}.csv");
    config["output"][0]["sql"] = json!("SELECT '$leaf' AS marker FROM data");
    success(invoke(&save(root, &config), root, "run", &[]));
    assert_eq!(
        fs::read_to_string(root.join("result_${literal}_${not_declared}.csv")).unwrap(),
        "marker\n$leaf\n"
    );
    config.as_object_mut().unwrap().remove("params");
    config["input"][0]["path"] = json!(r"\${not_declared}.csv");
    config["output"][0]["path"] = json!(r"$ordinary_\${literal}.csv");
    success(invoke(&save(root, &config), root, "run", &[]));
    assert!(root.join("$ordinary_${literal}.csv").is_file());
    for yaml in [r"'\${not_declared}.csv'", r#""\\${not_declared}.csv""#] {
        fs::write(root.join("path.yml"), format!("{yaml}\n")).unwrap();
        config["input"][0]["path"] = json!({"import":"path.yml"});
        success(invoke(&save(root, &config), root, "run", &[]));
    }
}

#[test]
fn invalid_path_references_fail_before_io_and_preserve_existing_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::write(root.join("in.csv"), "value\na\n").unwrap();
    fs::write(root.join("first.csv"), "old").unwrap();
    let mut config = job();
    let mut first = config["output"][0].clone();
    first["id"] = json!("first");
    first["path"] = json!("first.csv");
    config["output"].as_array_mut().unwrap().insert(0, first);
    config["params"] = json!({"nil":{"default":"NULL"},"blank":{"default":"''"}});
    for (template, diagnostic) in [
        ("${unknown}.csv", "undeclared path parameter 'unknown'"),
        ("${}.csv", "invalid parameter name"),
        ("${bad-name}.csv", "invalid parameter name"),
        ("${day", "unclosed ${...}"),
        ("${nil}.csv", "must not be NULL"),
        ("${blank}", "must not be empty"),
    ] {
        config["output"][1]["path"] = json!(template);
        let path = save(root, &config);
        for command in ["check", "explain", "run"] {
            let output = invoke(&path, root, command, &[]);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let message = String::from_utf8_lossy(&output.stderr);
            assert!(message.contains(diagnostic), "{message}");
            assert!(
                message.contains("output.result.path") && message.contains("job.json:"),
                "{message}"
            );
            assert_eq!(fs::read_to_string(root.join("first.csv")).unwrap(), "old");
        }
    }
    config["output"][1]["path"] = json!("out.csv");
    let output = invoke(
        &save(root, &config),
        root,
        "check",
        &["--output.result.path=${unknown}.csv"],
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("<command line>"));
    config["input"][0]["path"] = json!("${unknown}.csv");
    let output = invoke(&save(root, &config), root, "check", &[]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("input.data.path"));
}

#[test]
fn collision_checks_and_expanded_hashes_use_the_resolved_paths() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::write(root.join("in.csv"), "value\na\n").unwrap();
    let mut config = job();
    config["params"] = json!({"destination":{"default":"'in.csv'"}});
    config["output"][0]["path"] = json!("${destination}");
    let output = invoke(&save(root, &config), root, "run", &[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("path collision"));
    assert_eq!(
        fs::read_to_string(root.join("in.csv")).unwrap(),
        "value\na\n"
    );

    config.as_object_mut().unwrap().remove("params");
    let path = save(root, &config);
    let mut hashes = Vec::new();
    for cwd in [root.join("a"), root.join("b")] {
        fs::create_dir(&cwd).unwrap();
        fs::write(cwd.join("${source}.csv"), "value\na\n").unwrap();
        let output = success(invoke(
            &path,
            &cwd,
            "check",
            &[
                r"--input.data.path=\${source}.csv",
                r"--output.result.path=\${target}.csv",
            ],
        ));
        let event = String::from_utf8_lossy(&output.stderr)
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|event| event["event"] == "start")
            .unwrap();
        hashes.push(event["expanded_hash"].clone());
    }
    assert_ne!(hashes[0], hashes[1]);
}

#[test]
fn relative_path_bases_are_not_interpreted_as_templates() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let cwd = root.join("${literal}");
    fs::create_dir(&cwd).unwrap();
    fs::write(cwd.join("in.csv"), "value\na\n").unwrap();
    let config = job();
    success(invoke(
        &save(root, &config),
        &cwd,
        "run",
        &["--input.data.path=in.csv", "--output.result.path=out.csv"],
    ));
    assert_eq!(
        fs::read_to_string(cwd.join("out.csv")).unwrap(),
        "value\na\n"
    );
    success(invoke(&save(&cwd, &config), root, "run", &[]));
}
