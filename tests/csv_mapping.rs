use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn job() -> Value {
    json!({"input":[{"id":"data","format":"csv","path":"in.csv","encoding":"utf-8",
        "fields":[{"name":"customer_id","source_name":"顧客コード"},
            {"name":"amount","source_name":"売上金額","type":"decimal","precision":8}, {"name":"status"}],
        "check":{"when":"amount < 0","level":"error","message":"negative"}}],
        "output":[{"id":"result","format":"csv","path":"out.csv","encoding":"utf-8",
            "mode":"overwrite","sql":"SELECT customer_id, amount FROM data WHERE status = 'A'"}]})
}
fn invoke(root: &Path, config: &Value, command: &str) -> Output {
    fs::write(root.join("job.json"), config.to_string()).unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    if config["input"][0]["format"] == "lua" {
        process.arg("--allow-lua");
    }
    process
        .arg(command)
        .arg(root.join("job.json"))
        .output()
        .unwrap()
}

#[test]
fn external_headers_map_to_sql_names_after_decoding() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = job();
    for encoding in ["utf-8", "cp932", "utf-16le"] {
        let bytes = dxtran::encoding::TextEncoding::resolve(encoding)
            .unwrap()
            .encode("顧客コード,売上金額,status\nC01,123,A\nC02,456,B\n")
            .unwrap();
        fs::write(dir.path().join("in.csv"), bytes).unwrap();
        config["input"][0]["encoding"] = json!(encoding);
        let output = invoke(dir.path(), &config, "run");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "customer_id,amount\nC01,123\n"
        );
    }
}

#[test]
fn mapped_headers_still_require_exact_name_count_and_order() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("out.csv"), "old").unwrap();
    for header in [
        "売上金額,顧客コード,status",
        "顧客コード,売上金額,状態",
        "顧客コード,売上金額",
        "顧客コード,売上金額,status,extra",
        "customer_id,amount,status",
    ] {
        fs::write(dir.path().join("in.csv"), format!("{header}\n")).unwrap();
        let output = invoke(dir.path(), &job(), "run");
        assert_eq!(output.status.code(), Some(3));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("CSV header names, count and order")
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "old"
        );
    }
}

#[test]
fn invalid_mapping_options_are_configuration_errors() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("in.csv"),
        "顧客コード,売上金額,status\nC01,123,A\n",
    )
    .unwrap();
    let mut configs = Vec::new();
    let mut config = job();
    config["input"][0]["csv"] = json!({"header":false});
    configs.push(config);
    let mut config = job();
    config["input"][0]["fields"][1]["source_name"] = json!("顧客コード");
    configs.push(config);
    let mut config = job();
    config["output"][0]["fields"] = json!([{"name":"customer_id","source_name":"顧客コード"}]);
    configs.push(config);
    let mut config = job();
    config["input"][0]["format"] = json!("fixed");
    config["input"][0]["fixed"] = json!({"record_separator":"none"});
    configs.push(config);
    let mut config = job();
    config["input"][0]
        .as_object_mut()
        .unwrap()
        .remove("encoding");
    config["input"][0]["format"] = json!("lua");
    configs.push(config);
    for config in configs {
        let output = invoke(dir.path(), &config, "check");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("source name")
                || String::from_utf8_lossy(&output.stderr).contains("source_name")
        );
    }
}
