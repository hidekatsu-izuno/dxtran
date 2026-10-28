use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

const HOOKS: &str = r#"
local ffi = require('ffi')
return {
  init=function(job, options)
    assert(job == dxt.job)
    job.native = ffi.new('int[1]', 0)
    job.active, job.reads, job.writes = 0, 0, 0
    job.fail_read, job.fail_write = options.fail_read, options.fail_write
    job.fail_close = options.fail_close
    local f=assert(io.open(dxt.resolve_path('lifecycle.log'),'ab')); f:write('init\n'); f:close()
    if options.fail_init then error('init failed') end
  end,
  close=function(job, success)
    assert(job.active == 0, 'cursor still active at job close')
    assert(job.native[0] == job.reads + job.writes)
    local f=assert(io.open(dxt.resolve_path('lifecycle.log'),'ab'))
    f:write('close:' .. tostring(success) .. ':' .. job.reads .. ':' .. job.writes .. '\n'); f:close()
    job.native=nil
    if job.fail_close then error('job close failed') end
  end,
}
"#;

const PLUGIN: &str = r#"
return {
  open=function(options)
    local job = dxt.job
    assert(job.native ~= nil)
    job.active = job.active + 1
    local i = 0
    return {
      next=function()
        i=i+1
        if i > (options.count or 3) then return nil end
        if job.fail_read and i == 2 then error('read failed') end
        job.reads=job.reads+1; job.native[0]=job.native[0]+1
        -- Reuse a shared row to test that decode is included in serialization.
        job.row = job.row or {}
        job.row.id=tostring(i)
        return job.row
      end,
      close=function() job.active=job.active-1 end,
    }
  end,
  open_writer=function(options, columns, mode)
    local job=dxt.job
    assert(job.native ~= nil)
    job.active=job.active+1
    return {
      write=function(row)
        if job.fail_write and job.writes == 1 then error('write failed') end
        job.writes=job.writes+1; job.native[0]=job.native[0]+1
      end,
      commit=function() end,
      rollback=function() end,
      close=function() job.active=job.active-1 end,
    }
  end,
}
"#;

fn setup() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("hooks.lua"), HOOKS).unwrap();
    fs::write(dir.path().join("plugin.lua"), PLUGIN).unwrap();
    dir
}
fn input(id: &str) -> Value {
    json!({"id":id,"format":"lua","path":"plugin.lua","fields":[{"name":"id"}]})
}
fn output(id: &str) -> Value {
    json!({"id":id,"format":"lua","path":"plugin.lua","sql":"SELECT * FROM data"})
}
fn job(dir: &Path, inputs: Value, outputs: Value, options: Value) -> Value {
    let job = json!({"lua":{"path":"hooks.lua","variables":{"label":"shared"},"options":options},"input":inputs,"output":outputs});
    write_job(dir, &job);
    job
}
fn write_job(dir: &Path, value: &Value) {
    fs::write(dir.join("job.json"), value.to_string()).unwrap();
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
fn lifecycle(dir: &Path) -> String {
    fs::read_to_string(dir.join("lifecycle.log")).unwrap()
}

#[test]
fn variables_and_native_values_survive_rescans_joins_and_multiple_outputs() {
    for threads in [1, 4] {
        let dir = setup();
        let mut first = output("first");
        first["sql"] = json!("SELECT a.id FROM a INNER JOIN b ON a.id=b.id");
        let mut second = first.clone();
        second["id"] = json!("second");
        job(
            dir.path(),
            json!([input("a"), input("b")]),
            json!([first, second]),
            json!({}),
        );
        for command in ["check", "explain"] {
            status(invoke(dir.path(), command, false, threads), 0);
            assert!(!dir.path().join("lifecycle.log").exists());
        }
        status(invoke(dir.path(), "run", false, threads), 0);
        assert_eq!(lifecycle(dir.path()), "init\nclose:true:12:6\n");
    }
}

#[test]
fn literal_variables_work_without_a_lifecycle_script() {
    let dir = setup();
    fs::write(dir.path().join("plugin.lua"),r#"
return {
  open=function()
    local done=false
    return {next=function()
      if done then return nil end; done=true
      dxt.job.counter=dxt.job.counter+1
      assert(dxt.job.settings.flag == true)
      return {id=tostring(dxt.job.counter)}
    end}
  end,
  open_writer=function()
    assert(dxt.job.counter == 2)
    return {write=function(row) assert(row.id == '2') end,commit=function()end,rollback=function()end}
  end,
}
"#).unwrap();
    write_job(
        dir.path(),
        &json!({"lua":{"variables":{"counter":0,"settings":{"flag":true}}},"input":[input("data")],"output":[
      {"id":"first","format":"csv","path":"first.csv","encoding":"utf-8","sql":"SELECT * FROM data"},output("last")]}),
    );
    status(invoke(dir.path(), "run", false, 4), 0);
    assert_eq!(
        fs::read_to_string(dir.path().join("first.csv")).unwrap(),
        "id\n1\n"
    );
}

#[test]
fn check_scan_initializes_and_finalizes_but_never_opens_writers() {
    let dir = setup();
    job(
        dir.path(),
        json!([input("data")]),
        json!([output("saved")]),
        json!({}),
    );
    status(invoke(dir.path(), "check", true, 4), 0);
    assert_eq!(lifecycle(dir.path()), "init\nclose:true:3:0\n");
}

#[test]
fn failures_finalize_once_and_report_the_original_error() {
    for (options, code, diagnostic, expected) in [
        (
            json!({"fail_init":true}),
            3,
            "init failed",
            "init\nclose:false:0:0\n",
        ),
        (
            json!({"fail_read":true}),
            3,
            "read failed",
            "init\nclose:false:1:0\n",
        ),
        (
            json!({"fail_write":true}),
            5,
            "write failed",
            "init\nclose:false:3:1\n",
        ),
        (
            json!({"fail_close":true}),
            5,
            "job close failed",
            "init\nclose:true:3:3\n",
        ),
        (
            json!({"fail_read":true,"fail_close":true}),
            3,
            "read failed",
            "init\nclose:false:1:0\n",
        ),
    ] {
        let dir = setup();
        job(
            dir.path(),
            json!([input("data")]),
            json!([output("saved")]),
            options,
        );
        let result = status(invoke(dir.path(), "run", false, 4), code);
        assert!(String::from_utf8_lossy(&result.stderr).contains(diagnostic));
        assert_eq!(lifecycle(dir.path()), expected);
    }
}

#[test]
fn job_hooks_also_run_for_file_only_jobs() {
    let dir = setup();
    fs::write(dir.path().join("in.csv"), "id\n1\n").unwrap();
    job(
        dir.path(),
        json!([{"id":"data","format":"csv","path":"in.csv","encoding":"utf-8","fields":[{"name":"id"}]}]),
        json!([{"id":"file","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT * FROM data"}]),
        json!({}),
    );
    status(invoke(dir.path(), "run", false, 1), 0);
    assert_eq!(lifecycle(dir.path()), "init\nclose:true:0:0\n");
}

#[test]
fn limit_closes_live_cursors_before_finalizing_job_variables() {
    let dir = setup();
    let mut source = input("data");
    source["lua"] = json!({"options":{"count":1000000}});
    job(
        dir.path(),
        json!([source]),
        json!([{"id":"file","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT * FROM data LIMIT 1"}]),
        json!({}),
    );
    status(invoke(dir.path(), "run", false, 4), 0);
    let log = lifecycle(dir.path());
    assert!(log.starts_with("init\nclose:true:"), "{log}");
    assert_eq!(log.lines().count(), 2);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "id\n1\n"
    );
}

#[test]
fn invalid_configuration_and_invalid_hooks_do_not_initialize() {
    let dir = setup();
    let original = job(
        dir.path(),
        json!([input("data")]),
        json!([output("saved")]),
        json!({}),
    );
    for invalid in [
        json!({"variables":[]}),
        json!({"path":""}),
        json!({"unknown":true}),
    ] {
        let mut value = original.clone();
        value["lua"] = invalid;
        write_job(dir.path(), &value);
        status(invoke(dir.path(), "check", false, 1), 2);
    }
    for script in [
        "return {init=false}",
        "return {close=false}",
        "return false",
    ] {
        fs::write(dir.path().join("hooks.lua"), script).unwrap();
        write_job(dir.path(), &original);
        status(invoke(dir.path(), "check", false, 1), 2);
    }
    assert!(!dir.path().join("lifecycle.log").exists());
}

#[test]
fn plugin_environments_and_relative_helpers_remain_isolated() {
    let dir = setup();
    for (name, label) in [("left", "L"), ("right", "R")] {
        let sub = dir.path().join(name);
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("helper.lua"),format!("assert(dxt.job.label == 'shared'); return {{label='{label}', path=dxt.resolve_path('value')}}")).unwrap();
        fs::write(
            sub.join("plugin.lua"),
            r#"
local helper=require('helper')
return {open=function()
  local done=false
  return {next=function()
    if done then return nil end; done=true
    assert(helper.path == dxt.resolve_path('value'))
    return {id=helper.label}
  end}
end}
"#,
        )
        .unwrap();
    }
    let mut left = input("left");
    left["path"] = json!("left/plugin.lua");
    let mut right = input("right");
    right["path"] = json!("right/plugin.lua");
    job(
        dir.path(),
        json!([left, right]),
        json!([{"id":"file","format":"csv","path":"out.csv","encoding":"utf-8","sql":"SELECT left.id AS a, right.id AS b FROM left CROSS JOIN right"}]),
        json!({}),
    );
    status(invoke(dir.path(), "run", false, 4), 0);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "a,b\nL,R\n"
    );
}

#[test]
fn shared_variables_example_matches_expected_files_and_resets_each_run() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/lua_job");
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("out")).unwrap();
    for name in ["job.yml", "lifecycle.lua", "table.lua"] {
        fs::copy(source.join(name), dir.path().join(name)).unwrap();
    }
    for threads in [1, 4] {
        status(
            Command::new(env!("CARGO_BIN_EXE_dxt"))
                .arg("run")
                .arg("--allow-lua")
                .arg(dir.path().join("job.yml"))
                .args(["--threads", &threads.to_string()])
                .output()
                .unwrap(),
            0,
        );
        for name in ["records.csv", "summary.txt"] {
            assert_eq!(
                fs::read(dir.path().join("out").join(name)).unwrap(),
                fs::read(source.join("expected").join(name)).unwrap()
            );
        }
    }
}

#[test]
fn imported_job_settings_resolve_paths_and_protect_the_lifecycle_script() {
    let dir = setup();
    fs::create_dir(dir.path().join("defs")).unwrap();
    fs::write(dir.path().join("defs/hooks.lua"), HOOKS).unwrap();
    fs::write(
        dir.path().join("defs/job_lua.yml"),
        "path: hooks.lua\nvariables: {label: shared}\nlibraries: {native: ./libnative.so}\n",
    )
    .unwrap();
    let mut value = job(
        dir.path(),
        json!([input("data")]),
        json!([output("saved")]),
        json!({}),
    );
    value["lua"] = json!({"import":"defs/job_lua.yml"});
    write_job(dir.path(), &value);
    let loaded = dxtran::config::load(&dir.path().join("job.json")).unwrap();
    let config = loaded.lua.as_ref().unwrap();
    assert_eq!(
        config.path.as_ref().unwrap(),
        &dir.path().join("defs/hooks.lua")
    );
    assert_eq!(
        Path::new(&config.libraries["native"]),
        dir.path().join("defs/./libnative.so")
    );
    status(invoke(dir.path(), "run", false, 4), 0);
    assert_eq!(
        lifecycle(&dir.path().join("defs")),
        "init\nclose:true:3:3\n"
    );
    value["output"] = json!([{"id":"bad","format":"csv","path":"defs/hooks.lua","mode":"overwrite","encoding":"utf-8","sql":"SELECT * FROM data"}]);
    write_job(dir.path(), &value);
    status(invoke(dir.path(), "run", false, 4), 2);
    assert_eq!(
        fs::read_to_string(dir.path().join("defs/hooks.lua")).unwrap(),
        HOOKS
    );
}

#[cfg(unix)]
#[test]
fn signal_interrupt_finalizes_after_the_active_callback() {
    use std::{
        process::Stdio,
        time::{Duration, Instant},
    };
    let dir = setup();
    let slow=PLUGIN.replace("return {\n  open=function", "local ffi=require('ffi'); ffi.cdef[[int usleep(unsigned int);]]\nreturn {\n  open=function")
        .replace("i=i+1", "ffi.C.usleep(5000); i=i+1");
    fs::write(dir.path().join("plugin.lua"), slow).unwrap();
    let mut source = input("data");
    source["lua"] = json!({"options":{"count":1000000}});
    job(
        dir.path(),
        json!([source]),
        json!([output("saved")]),
        json!({}),
    );
    let mut child = Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg("--allow-lua")
        .arg(dir.path().join("job.json"))
        .args(["--threads", "4", "--temp-dir"])
        .arg(dir.path().join("spill"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !dir.path().join("lifecycle.log").exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before init"
        );
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("job did not start");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(50));
    rustix::process::kill_process(
        rustix::process::Pid::from_child(&child),
        rustix::process::Signal::TERM,
    )
    .unwrap();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("job did not close after signal");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let log = lifecycle(dir.path());
    assert!(log.contains("close:false:"), "{log}");
    assert_eq!(log.lines().count(), 2);
}
