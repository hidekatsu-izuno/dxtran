//! Permission must be checked before evaluating any operator-supplied Lua file.
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

const MARKER: &str = r#"
local f=assert(io.open(dxt.resolve_path('executed'),'wb')); f:write('executed'); f:close()
return {open=function() return {next=function()end} end,
        open_writer=function() return {write=function()end,commit=function()end,rollback=function()end} end}
"#;

fn source() -> Value {
    json!({"id":"data","format":"lua","path":"plugin.lua","fields":[{"name":"value"}]})
}
fn destination() -> Value {
    json!({"id":"result","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT * FROM data"})
}
fn setup(script: &[u8]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("plugin.lua"), script).unwrap();
    fs::write(dir.path().join("in.csv"), "value\nhello\n").unwrap();
    job(
        dir.path(),
        &json!({"input":[source()],"output":[destination()]}),
    );
    dir
}
fn job(dir: &Path, value: &Value) {
    fs::write(dir.join("job.json"), value.to_string()).unwrap();
}
fn command(dir: &Path, action: &str, allow: bool) -> Command {
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    process.arg(action).arg(dir.join("job.json"));
    if allow {
        process.arg("--allow-lua");
    }
    process.current_dir(std::env::temp_dir());
    process
}
fn status(output: Output, code: i32) -> String {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stderr).unwrap()
}

#[test]
fn no_command_evaluates_lua_without_explicit_operator_permission() {
    for (action, scan) in [
        ("check", false),
        ("check", true),
        ("explain", false),
        ("run", false),
    ] {
        for kind in ["input", "output", "job", "variables"] {
            let dir = setup(MARKER.as_bytes());
            let csv = json!({"id":"data","format":"csv","path":"in.csv","encoding":"utf-8","fields":[{"name":"value"}]});
            let mut value = json!({"input":[csv],"output":[destination()]});
            match kind {
                "input" => value["input"] = json!([source()]),
                "output" => {
                    value["output"] = json!([{"id":"saved","format":"lua","path":"plugin.lua","sql":"SELECT * FROM data"}])
                }
                "job" => value["lua"] = json!({"path":"plugin.lua"}),
                _ => value["lua"] = json!({"variables":{"counter":0}}),
            }
            job(dir.path(), &value);
            let mut process = command(dir.path(), action, false);
            if scan {
                process.arg("--scan");
            }
            let message = status(process.output().unwrap(), 2);
            assert!(message.contains("--allow-lua"), "{kind}: {message}");
            assert!(!dir.path().join("executed").exists());
            assert!(!dir.path().join("out.csv").exists());
        }
    }
}

#[test]
fn yaml_cannot_self_authorize_unsafe_lua() {
    for key in ["allow_lua", "allow_unsafe", "allow_unsafe_lua", "trusted"] {
        let dir = setup(MARKER.as_bytes());
        let mut input = source();
        input["lua"] = json!({key:true});
        job(
            dir.path(),
            &json!({"input":[input],"output":[destination()]}),
        );
        status(command(dir.path(), "check", false).output().unwrap(), 2);
        assert!(!dir.path().join("executed").exists());
    }
}

#[test]
fn trusted_mode_is_an_explicit_full_privilege_choice_including_check() {
    let dir = setup(MARKER.as_bytes());
    status(command(dir.path(), "check", true).output().unwrap(), 0);
    assert_eq!(
        fs::read_to_string(dir.path().join("executed")).unwrap(),
        "executed"
    );
    assert!(!dir.path().join("out.csv").exists());
}

#[test]
fn library_entrypoints_also_require_explicit_authorization() {
    use dxtran::{config::Dataset, layout::RecordLayout, source::InputReader};
    let dir = setup(MARKER.as_bytes());
    let mut input = source();
    input["path"] = json!(dir.path().join("plugin.lua"));
    let mut input: Dataset = serde_json::from_value(input).unwrap();
    let layout = RecordLayout::input(&input).unwrap();
    assert_eq!(InputReader::open(&input, &layout).err().unwrap().code, 2);
    assert!(!dir.path().join("executed").exists());
    input.allow_unsafe_lua();
    InputReader::open(&input, &layout).unwrap();
    assert!(dir.path().join("executed").exists());
}

#[test]
fn source_and_helper_bytecode_are_rejected_even_in_trusted_mode() {
    let lua = mlua::Lua::new();
    let bytecode = lua
        .load("return {open=function() return {next=function()end} end}")
        .into_function()
        .unwrap()
        .dump(false);
    let dir = setup(&bytecode);
    let message = status(command(dir.path(), "check", true).output().unwrap(), 2);
    assert!(
        message.contains("binary") || message.contains("text"),
        "{message}"
    );
    fs::write(dir.path().join("plugin.lua"), "return require('helper')").unwrap();
    fs::write(dir.path().join("helper.lua"), bytecode).unwrap();
    let message = status(command(dir.path(), "check", true).output().unwrap(), 2);
    assert!(
        message.contains("binary") || message.contains("text"),
        "{message}"
    );
}

#[test]
fn oversized_sources_and_helpers_are_rejected_before_compilation() {
    let bytes = vec![b' '; 1024 * 1024 + 1];
    let dir = setup(&bytes);
    assert!(status(command(dir.path(), "check", true).output().unwrap(), 2).contains("1 MiB"));
    fs::write(dir.path().join("plugin.lua"), "return require('large')").unwrap();
    fs::write(dir.path().join("large.lua"), bytes).unwrap();
    assert!(status(command(dir.path(), "check", true).output().unwrap(), 2).contains("1 MiB"));
}

#[test]
fn ambient_lua_paths_are_not_used_to_resolve_helpers() {
    let dir = setup(b"return require('ambient')");
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("ambient.lua"), MARKER).unwrap();
    let message = status(
        command(dir.path(), "check", true)
            .env("LUA_PATH", outside.path().join("?.lua"))
            .env("LUA_CPATH", outside.path().join("?.so"))
            .output()
            .unwrap(),
        2,
    );
    assert!(
        message.contains("not found in plugin directory"),
        "{message}"
    );
    assert!(!dir.path().join("executed").exists());
}

#[test]
fn builtin_ffi_cannot_be_shadowed_by_an_adjacent_file() {
    let dir=setup(b"assert(require('ffi').typeof('int')); return {open=function() return {next=function()end} end}");
    fs::write(dir.path().join("ffi.lua"), MARKER).unwrap();
    status(command(dir.path(), "check", true).output().unwrap(), 0);
    assert!(!dir.path().join("executed").exists());
}

#[cfg(unix)]
#[test]
fn helper_symlinks_cannot_escape_the_plugin_directory() {
    let dir = setup(b"return require('outside')");
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("outside.lua"), MARKER).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("outside.lua"),
        dir.path().join("outside.lua"),
    )
    .unwrap();
    assert!(
        status(command(dir.path(), "check", true).output().unwrap(), 2)
            .contains("escapes the plugin directory")
    );
    assert!(!dir.path().join("executed").exists());
}
