use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn job() -> Value {
    json!({"description":"売上変換ジョブ", "input":[{"id":"data","format":"csv","path":"missing.csv","encoding":"utf-8","fields":[{"name":"value","description":"入力値"}]}],
        "output":[{"id":"result","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT * FROM data"}]})
}
fn invoke(root: &Path, config: &Value, command: &str, args: &[&str]) -> Output {
    fs::write(root.join("job.json"), config.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(root.join("job.json"))
        .args(args)
        .current_dir(root)
        .output()
        .unwrap()
}
fn success(output: Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn doc_expands_imports_and_descriptions_without_data_parameters_or_lua_execution() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::write(
        root.join("fields.yml"),
        "- name: value\n  description: 取込値\n",
    )
    .unwrap();
    fs::write(
        root.join("unsafe.lua"),
        "local f=assert(io.open('doc-must-not-execute','w')); f:close(); error('must not run')",
    )
    .unwrap();
    let mut config = job();
    config["description"] = json!("売上変換\n\n業務仕様の説明。");
    config["params"] = json!({"day":{"type":"date","description":"対象営業日"},"bad":{"default":"CAST('bad' AS INT)","description":"SQLは評価しない"}});
    config["input"][0].as_object_mut().unwrap().remove("path");
    config["input"][0]["description"] = json!("入力の売上");
    config["input"][0]["fields"] = json!({"import":"fields.yml"});
    config["input"][0]["csv"] = json!({"description":"ヘッダー付きCSV","header":true});
    config["input"][0]["after"] = json!({"description":"件数検査","when":"COUNT(*) = 0","level":"error","message":"入力なし"});
    config["input"].as_array_mut().unwrap().push(json!({"id":"native","format":"parquet","path":"missing.parquet","description":"ファイルの型を使う"}));
    config["input"].as_array_mut().unwrap().push(json!({"id":"external","format":"lua","path":"unsafe.lua","fields":[{"name":"id","description":"外部ID"}]}));
    config["lua"] = json!({"description":"開始終了処理","path":"unsafe.lua","options":{"description":"自由なオプション値"}});
    config["process"] = json!([{"id":"selected","description":"対象データを抽出","sql":"SELECT value FROM data WHERE ${day} IS NOT NULL"}]);
    config["output"][0].as_object_mut().unwrap().remove("path");
    config["output"][0]["description"] = json!("変換後の出力");
    config["output"][0]["sql"] = json!("SELECT * FROM selected");
    config["output"][0]["fields"] = json!([{"name":"value","description":"出力値"}]);
    config["exit_codes"] = json!({"2":8});
    let markdown = success(invoke(root, &config, "doc", &[]));
    assert!(markdown.starts_with("# Job Definition\n"));
    for expected in [
        "売上変換\n\n業務仕様の説明。",
        "対象営業日",
        "Required",
        "CAST('bad' AS INT)",
        "入力の売上",
        "取込値",
        "ヘッダー付きCSV",
        "件数検査",
        "対象データを抽出",
        "${day}",
        "変換後の出力",
        "出力値",
        "path: supplied via the CLI.",
        "Uses the input file schema.",
        "外部ID",
        "開始終了処理",
        "自由なオプション値",
        "Exit Codes",
    ] {
        assert!(
            markdown.contains(expected),
            "missing {expected}: {markdown}"
        );
    }
    assert!(!markdown.contains("\"event\":"));
    assert_eq!(
        markdown,
        success(invoke(root, &config, "doc", &["--allow-lua"]))
    );
    assert!(!root.join("doc-must-not-execute").exists());
    assert!(!root.join("out.csv").exists());
    // Ordinary commands still require their runtime paths.
    assert_eq!(invoke(root, &config, "check", &[]).status.code(), Some(8));
    let output = invoke(root, &config, "doc", &["-p", "day=2026-11-07"]);
    assert_eq!(output.status.code(), Some(8));
    assert!(output.stdout.is_empty());
}

#[test]
fn descriptions_do_not_change_fixed_filler_bits_or_lua_conversion() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::write(root.join("in.dat"), b"a|\x80").unwrap();
    let fields = json!([{"name":"id","length":1,"description":"識別子"},
        {"length":1,"fill_byte":124,"description":"区切り"},
        {"name":"flags","length":1,"type":"bits","description":"状態","bits":[{"name":"enabled","bit":0,"description":"有効フラグ"}]}]);
    let mut config = json!({"description":"固定長変換", "input":[{"id":"data","format":"fixed","path":"in.dat","encoding":"utf-8","description":"入力","fixed":{"record_separator":"none","description":"改行なし"},"fields":fields}],
        "output":[{"id":"result","format":"fixed","path":"out.dat","encoding":"utf-8","description":"出力","fixed":{"record_separator":"none","description":"固定長"},"fields":fields,"sql":"SELECT id, enabled FROM data"}]});
    let markdown = success(invoke(root, &config, "doc", &[]));
    assert!(
        markdown.contains("FILLER")
            && markdown.contains("区切り")
            && markdown.contains("有効フラグ")
    );
    success(invoke(root, &config, "run", &[]));
    assert_eq!(fs::read(root.join("out.dat")).unwrap(), b"a|\x80");
    fs::write(root.join("input.lua"), "return {open=function() local done=false; return {next=function() if done then return nil end; done=true; return {id='a'} end} end}").unwrap();
    config["input"] = json!([{"id":"data","format":"lua","path":"input.lua","description":"外部データ","lua":{"description":"プラグイン設定"},"fields":[{"name":"id","description":"外部ID"}]}]);
    config["output"] = json!([{"id":"result","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT * FROM data"}]);
    success(invoke(root, &config, "run", &["--allow-lua"]));
    assert_eq!(fs::read_to_string(root.join("out.csv")).unwrap(), "id\na\n");
}

#[test]
fn markdown_tables_and_code_fences_preserve_user_content() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut config = job();
    config["input"][0]["fields"][0]["description"] = json!("A|B\n<tag> & more");
    config["input"][0]["fields"][0]["type"] = json!("decimal");
    config["input"][0]["fields"][0]["precision"] = json!(8);
    config["input"][0]["fields"][0]["scale"] = json!(2);
    config["output"][0]["sql"] = json!("SELECT '```' AS value FROM data");
    let markdown = success(invoke(root, &config, "doc", &[]));
    assert!(markdown.contains("A\\|B<br>&lt;tag&gt; &amp; more"));
    assert!(markdown.contains("| Additional | Description |"));
    let field_row = markdown
        .lines()
        .find(|line| line.starts_with("| 1 |"))
        .unwrap();
    assert!(
        field_row.contains("precision: 8") && field_row.contains("scale: 2"),
        "{field_row}"
    );
    assert!(field_row.contains("<br>"));
    assert!(!markdown.contains("Additional settings"));
    assert!(markdown.contains("````sql\nSELECT '```' AS value FROM data\n````"));
    assert!(!root.join("out.csv").exists());
    let path_flag = format!(
        "--input.data.path={}",
        root.join("does-not-exist.csv").display()
    );
    let markdown = success(invoke(root, &config, "doc", &[&path_flag]));
    assert!(markdown.contains("does-not-exist.csv"));
}

#[test]
fn descriptions_must_be_strings_and_doc_errors_do_not_write_markdown() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for section in ["root", "input", "field", "param", "check", "csv", "bit"] {
        let mut config = job();
        match section {
            "root" => config["description"] = json!(42),
            "input" => config["input"][0]["description"] = json!(false),
            "field" => config["input"][0]["fields"][0]["description"] = json!([]),
            "param" => config["params"] = json!({"x":{"description":42}}),
            "check" => {
                config["input"][0]["after"] =
                    json!({"when":"TRUE","level":"info","message":"x","description":42})
            }
            "csv" => config["input"][0]["csv"] = json!({"description":42}),
            "bit" => {
                config["input"][0]["fields"][0]["bits"] =
                    json!([{"name":"enabled","bit":0,"description":42}])
            }
            _ => unreachable!(),
        }
        let output = invoke(root, &config, "doc", &[]);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{section}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
    }
}
