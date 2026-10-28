use datafusion::common::ScalarValue;
use dxtran::{config::Dataset, layout::RecordLayout, source::InputReader};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn source(dir: &Path, script: &str, fields: Value) -> Dataset {
    fs::write(dir.join("plugin.lua"), script).unwrap();
    let mut dataset: Dataset = serde_json::from_value(
        json!({"id":"data", "format":"lua", "path":dir.join("plugin.lua"), "fields":fields}),
    )
    .unwrap();
    dataset.allow_unsafe_lua();
    dataset
}

const TRACKED: &str = r#"
return { open = function(options)
    local function log(message)
        local file = assert(io.open(dxt.resolve_path("lifecycle.txt"), "ab"))
        file:write(message .. "\n"); file:close()
    end
    log("open")
    local i = 0
    return {
        next = function()
            i = i + 1
            if i <= (options.count or 2) then return {id=i, text="日本"} end
        end,
        close = function() log("close") end,
    }
end }
"#;

fn job(dir: &Path, input: Value, sql: &str) -> Value {
    let job = json!({"input":[input], "output":[{
        "id":"result", "format":"csv", "path":"out.csv", "encoding":"utf-8",
        "mode":"overwrite", "sql":sql
    }]});
    fs::write(dir.join("job.json"), job.to_string()).unwrap();
    job
}

fn invoke(dir: &Path, command: &str, scan: bool, threads: usize) -> Output {
    fs::create_dir_all(dir.join("spill")).unwrap();
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

fn assert_status(output: Output, code: i32) -> Output {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn typed_rows_preserve_null_empty_binary_and_exact_integers() {
    // Adding a native plugin backend must preserve the public reader's Send bound.
    fn assert_send<T: Send>() {}
    assert_send::<InputReader>();
    let dir = tempfile::tempdir().unwrap();
    let dataset = source(
        dir.path(),
        r#"
return {open=function()
  local done = false
  return {next=function()
    if done then return nil end
    done = true
    return {s="", i="-9223372036854775808", u="18446744073709551615", f=1.5, b=false, raw="\0\255"}
  end}
end}
"#,
        json!([
            {"name":"s"}, {"name":"i","type":"int64"}, {"name":"u","type":"uint64"},
            {"name":"f","type":"float64"}, {"name":"b","type":"boolean"},
            {"name":"raw","type":"binary"}, {"name":"missing","nullable":true}
        ]),
    );
    let layout = RecordLayout::input(&dataset).unwrap();
    let mut reader = InputReader::open(&dataset, &layout).unwrap();
    assert_eq!(
        reader.next_row().unwrap().unwrap(),
        vec![
            ScalarValue::Utf8(Some("".into())),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::Float64(Some(1.5)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Binary(Some(vec![0, 255])),
            ScalarValue::Utf8(None),
        ]
    );
    assert!(reader.next_row().unwrap().is_none());
}

#[test]
fn metadata_is_lazy_and_scans_close_the_cursor_once() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("plugin.lua"), TRACKED).unwrap();
    job(
        dir.path(),
        json!({"id":"data", "format":"lua", "path":"plugin.lua",
        "fields":[{"name":"id","type":"int64"},{"name":"text"}]}),
        "SELECT * FROM data",
    );
    for command in ["check", "explain"] {
        assert_status(invoke(dir.path(), command, false, 4), 0);
        assert!(!dir.path().join("lifecycle.txt").exists());
        assert!(!dir.path().join("out.csv").exists());
    }
    assert_status(invoke(dir.path(), "check", true, 4), 0);
    assert_eq!(
        fs::read_to_string(dir.path().join("lifecycle.txt")).unwrap(),
        "open\nclose\n"
    );
    assert!(!dir.path().join("out.csv").exists());
    for threads in [1, 4] {
        assert_status(invoke(dir.path(), "run", false, threads), 0);
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "id,text\n1,日本\n2,日本\n"
        );
    }
    assert_eq!(
        fs::read_to_string(dir.path().join("lifecycle.txt")).unwrap(),
        "open\nclose\n".repeat(3)
    );
}

#[test]
fn partial_read_and_invalid_rows_always_close_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let dataset = source(
        dir.path(),
        TRACKED,
        json!([{"name":"id","type":"int64"},{"name":"text"}]),
    );
    let layout = RecordLayout::input(&dataset).unwrap();
    {
        let mut reader = InputReader::open(&dataset, &layout).unwrap();
        assert!(reader.next_row().unwrap().is_some());
    }
    assert_eq!(
        fs::read_to_string(dir.path().join("lifecycle.txt")).unwrap(),
        "open\nclose\n"
    );
    for (value, kind) in [
        ("{v=nil}", "string"),
        ("false", "string"),
        ("{v=1}", "string"),
        ("{v='\\255'}", "string"),
        ("{v='9223372036854775808'}", "int64"),
        ("{v=9007199254740992}", "int64"),
        ("{v=-1}", "uint64"),
        ("{v=1.5}", "int64"),
        ("{v=0/0}", "float64"),
        ("{v='x', extra='x'}", "string"),
        ("{v=string.rep('x', 16777216)}", "string"),
    ] {
        let script = format!(
            r#"return {{open=function()
          return {{next=function() return {value} end, close=function()
            local f=assert(io.open(dxt.resolve_path("closed"),"w")); f:write("yes"); f:close()
          end}}
        end}}"#
        );
        let dataset = source(dir.path(), &script, json!([{"name":"v","type":kind}]));
        let layout = RecordLayout::input(&dataset).unwrap();
        fs::remove_file(dir.path().join("closed")).ok();
        let mut reader = InputReader::open(&dataset, &layout).unwrap();
        let error = reader.next_row().unwrap_err();
        assert_eq!(error.code, 3, "{value}: {error}");
        assert!(error.message.contains("lua record 1"), "{error}");
        assert_eq!(
            fs::read_to_string(dir.path().join("closed")).unwrap(),
            "yes",
            "{value}"
        );
    }
}

#[test]
fn batches_projection_and_source_order_work_with_multiple_threads() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("plugin.lua"), TRACKED).unwrap();
    let input = json!({"id":"data", "format":"lua", "path":"plugin.lua",
        "lua":{"options":{"count":9000}}, "fields":[{"name":"id","type":"int64"},{"name":"text"}]});
    for threads in [1, 4] {
        for (sql, expected) in [
            (
                "SELECT id FROM data LIMIT 3 OFFSET 8191",
                "id\n8192\n8193\n8194\n",
            ),
            ("SELECT COUNT(*) AS n FROM data", "n\n9000\n"),
            (
                "SELECT id FROM data WHERE id IN (1, 8192, 8193, 9000)",
                "id\n1\n8192\n8193\n9000\n",
            ),
        ] {
            job(dir.path(), input.clone(), sql);
            assert_status(invoke(dir.path(), "run", false, threads), 0);
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                expected
            );
        }
    }
}

#[test]
fn check_failure_precedes_later_plugin_error_and_preserves_output() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("plugin.lua"),
        TRACKED.replace(
            "if i <= (options.count or 2)",
            "if i > 1 then error('later failure') end; if i <= (options.count or 2)",
        ),
    )
    .unwrap();
    job(
        dir.path(),
        json!({"id":"data", "format":"lua", "path":"plugin.lua",
        "check":{"when":"id = 1", "level":"error", "message":"first check"},
        "fields":[{"name":"id","type":"int64"},{"name":"text"}]}),
        "SELECT * FROM data",
    );
    fs::write(dir.path().join("out.csv"), "previous").unwrap();
    for command in ["check", "run"] {
        let output = assert_status(invoke(dir.path(), command, command == "check", 4), 3);
        assert!(String::from_utf8_lossy(&output.stderr).contains("first check"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("later failure"));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "previous"
        );
    }
    assert_eq!(
        fs::read_to_string(dir.path().join("lifecycle.txt")).unwrap(),
        "open\nclose\n".repeat(2)
    );
}

#[test]
fn malformed_plugins_and_invalid_configurations_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for script in ["this is not Lua", "return {}", "return false"] {
        let dataset = source(dir.path(), script, json!([{"name":"v"}]));
        let layout = RecordLayout::input(&dataset).unwrap();
        assert_eq!(InputReader::open(&dataset, &layout).err().unwrap().code, 2);
    }
    for fields in [
        json!([]),
        json!([{"name":"v","type":"bogus"}]),
        json!([{"name":"v","length":3}]),
        json!([{"name":"v"},{"name":"v"}]),
    ] {
        let dataset = source(dir.path(), "return {}", fields);
        assert_eq!(RecordLayout::input(&dataset).err().unwrap().code, 2);
    }
}

#[test]
fn empty_tables_cursor_errors_and_close_errors_have_input_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let input = json!({"id":"data", "format":"lua", "path":"plugin.lua", "fields":[{"name":"v"}]});
    for (script, code, diagnostic) in [
        (
            "return {open=function() return {next=function() end} end}",
            0,
            "",
        ),
        (
            "return {open=function() error('open failed') end}",
            3,
            "open failed",
        ),
        (
            "return {open=function() return false end}",
            3,
            "lua record 1",
        ),
        (
            "return {open=function() return {next=function() error('next failed') end} end}",
            3,
            "next failed",
        ),
        (
            "return {open=function() return {next=function() end, close=function() error('close failed') end} end}",
            3,
            "close failed",
        ),
        (
            "return {open=function() return {next=false,close=function() local f=assert(io.open(dxt.resolve_path('closed'),'w')); f:close() end} end}",
            3,
            "lua record 1",
        ),
    ] {
        fs::write(dir.path().join("plugin.lua"), script).unwrap();
        job(dir.path(), input.clone(), "SELECT * FROM data");
        fs::write(dir.path().join("out.csv"), "previous").unwrap();
        assert_status(invoke(dir.path(), "check", false, 1), 0);
        let output = assert_status(invoke(dir.path(), "run", false, 1), code);
        assert!(String::from_utf8_lossy(&output.stderr).contains(diagnostic));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            if code == 0 { "v\n" } else { "previous" }
        );
    }
    assert!(dir.path().join("closed").exists());
}

#[test]
fn byte_bounded_batches_keep_every_row_and_check_each_row_once() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("plugin.lua"),
        TRACKED.replace("\"日本\"", "string.rep('x',4096)"),
    )
    .unwrap();
    job(
        dir.path(),
        json!({"id":"data", "format":"lua", "path":"plugin.lua",
        "lua":{"options":{"count":5000}},
        "check":{"when":"id = 4033", "level":"warn", "message":"once"},
        "fields":[{"name":"id","type":"int64"},{"name":"text"}]}),
        "SELECT id FROM data WHERE id IN (1, 4000, 4095, 4096, 4097, 5000)",
    );
    let output = assert_status(invoke(dir.path(), "run", false, 4), 0);
    assert_eq!(
        String::from_utf8_lossy(&output.stderr)
            .matches("\"event\":\"check\"")
            .count(),
        1
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "id\n1\n4000\n4095\n4096\n4097\n5000\n"
    );
}

#[test]
fn imports_resolve_script_options_and_each_library_at_their_origins() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("defs")).unwrap();
    fs::write(dir.path().join("defs/plugin.lua"), TRACKED).unwrap();
    fs::write(
        dir.path().join("defs/input.yml"),
        r#"
id: data
format: lua
path: plugin.lua
lua:
  libraries: {original: ./lib-original.so, search: libsearch.so}
  options: {count: 1}
fields: [{name: id, type: int64}, {name: text}]
"#,
    )
    .unwrap();
    fs::write(
        dir.path().join("job.json"),
        r#"
input:
  - import: defs/input.yml
    lua:
      libraries: {local: ./lib-local.so}
output:
  - {id: result, format: csv, path: out.csv, encoding: utf-8, sql: 'SELECT * FROM data'}
"#,
    )
    .unwrap();
    let loaded = dxtran::config::load(&dir.path().join("job.json")).unwrap();
    assert_eq!(loaded.input[0].path, dir.path().join("defs/plugin.lua"));
    let lua = loaded.input[0].lua.as_ref().unwrap();
    assert_eq!(lua.base_dir, dir.path().join("defs"));
    assert_eq!(
        Path::new(&lua.libraries["original"]),
        dir.path().join("defs/./lib-original.so")
    );
    assert_eq!(
        Path::new(&lua.libraries["local"]),
        dir.path().join("./lib-local.so")
    );
    assert_eq!(lua.libraries["search"], "libsearch.so");
    assert_status(invoke(dir.path(), "run", false, 4), 0);
    assert_eq!(
        fs::read_to_string(dir.path().join("defs/lifecycle.txt")).unwrap(),
        "open\nclose\n"
    );
    assert!(!dir.path().join("lifecycle.txt").exists());
}

#[cfg(target_os = "linux")]
#[test]
fn sqlite_example_works_with_loader_search_and_explicit_library_paths() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sqlite");
    fs::create_dir_all(dir.path().join("data")).unwrap();
    fs::create_dir_all(dir.path().join("out")).unwrap();
    fs::copy(example.join("sqlite.lua"), dir.path().join("sqlite.lua")).unwrap();
    fs::copy(
        example.join("data/sales.csv"),
        dir.path().join("data/sales.csv"),
    )
    .unwrap();
    assert_status(
        Command::new("python3")
            .args([
                "-c",
                "import runpy, sys; runpy.run_path(sys.argv[1])['create_db'](sys.argv[2])",
            ])
            .arg(example.join("create_db.py"))
            .arg(dir.path().join("data/customers.sqlite"))
            .output()
            .unwrap(),
        0,
    );
    let library = assert_status(
        Command::new("python3")
            .args([
                "-c",
                r#"
import ctypes
ctypes.CDLL('libsqlite3.so.0')
print(next(line.split()[-1] for line in open('/proc/self/maps') if '/libsqlite3.so' in line))
"#,
            ])
            .output()
            .unwrap(),
        0,
    );
    let library = String::from_utf8(library.stdout).unwrap().trim().to_owned();
    fs::create_dir(dir.path().join("native")).unwrap();
    symlink(&library, dir.path().join("native/libdxt_sqlite_test.so")).unwrap();
    let original = fs::read_to_string(example.join("job.yml")).unwrap();
    for library_name in [
        "libdxt_sqlite_test.so",
        "./native/libdxt_sqlite_test.so",
        library.as_str(),
    ] {
        let yaml = original.replace("libsqlite3.so.0", library_name);
        fs::write(dir.path().join("job.yml"), yaml).unwrap();
        for threads in [1, 4] {
            for (command, scan) in [
                ("check", false),
                ("check", true),
                ("explain", false),
                ("run", false),
            ] {
                let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
                process
                    .arg(command)
                    .arg("--allow-lua")
                    .arg(dir.path().join("job.yml"))
                    .args(["--threads", &threads.to_string()])
                    .current_dir(std::env::temp_dir());
                if scan {
                    process.arg("--scan");
                }
                process.env("LD_LIBRARY_PATH", dir.path().join("native"));
                assert_status(process.output().unwrap(), 0);
            }
            for name in ["details.csv", "customers.csv", "totals.csv"] {
                assert_eq!(
                    fs::read(dir.path().join("out").join(name)).unwrap(),
                    fs::read(example.join("expected").join(name)).unwrap(),
                    "{name}: {library_name}"
                );
            }
        }
    }
    for (needle, replacement, diagnostic) in [
        ("libsqlite3.so.0", "./native/missing.so", "missing.so"),
        (
            "data/customers.sqlite",
            "data/missing.sqlite",
            "sqlite3_open_v2",
        ),
        (
            "SELECT customer_id, customer_name, region, credit_limit, score, payload FROM customers ORDER BY customer_id",
            "SELECT * FROM nonexistent_table",
            "sqlite3_prepare_v2",
        ),
        (
            "SELECT customer_id, customer_name, region, credit_limit, score, payload FROM customers ORDER BY customer_id",
            "SELECT * FROM customers; SELECT * FROM customers",
            "exactly one statement",
        ),
    ] {
        fs::write(
            dir.path().join("job.yml"),
            original.replace(needle, replacement),
        )
        .unwrap();
        for (command, scan) in [("check", false), ("check", true), ("run", false)] {
            let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
            process
                .arg(command)
                .arg("--allow-lua")
                .arg(dir.path().join("job.yml"));
            if scan {
                process.arg("--scan");
            }
            let output = assert_status(
                process.output().unwrap(),
                if scan || command == "run" { 3 } else { 0 },
            );
            if scan || command == "run" {
                assert!(String::from_utf8_lossy(&output.stderr).contains(diagnostic));
            }
        }
        assert!(!dir.path().join("data/missing.sqlite").exists());
        for name in ["details.csv", "customers.csv", "totals.csv"] {
            assert_eq!(
                fs::read(dir.path().join("out").join(name)).unwrap(),
                fs::read(example.join("expected").join(name)).unwrap()
            );
        }
    }
}
