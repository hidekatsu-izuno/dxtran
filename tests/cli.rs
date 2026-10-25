use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
fn invoke(dir: &Path, command: &str, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(dir.join("job.yml"))
        .args(extra)
        .current_dir("/")
        .output()
        .unwrap()
}
fn ok(o: Output) -> Output {
    assert!(
        o.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    o
}
fn write_job(dir: &Path, input: Value, process: Value, output: Value) {
    fs::write(
        dir.join("job.yml"),
        serde_json::to_vec(&json!({"input":input,"process":process,"output":output})).unwrap(),
    )
    .unwrap();
}
fn csv(id: &str, path: &str, fields: Value) -> Value {
    json!({"id":id,"path":path,"format":"csv","encoding":"utf-8","fields":fields})
}
fn out(sql: &str, path: &str) -> Value {
    json!({"id":"result","sql":sql,"format":"csv","encoding":"utf-8","path":path})
}
#[test]
fn csv_null_empty_quotes_and_partial_fields() {
    let d = tempfile::tempdir().unwrap();
    fs::write(
        d.path().join("in.csv"),
        "a,b,c\n,\"\",1\n\"a,b\",\"x\"\"y\",2\n\"line\nbreak\",z,3\n",
    )
    .unwrap();
    let mut output = out("SELECT a,b,c FROM data", "out.csv");
    output["fields"] =
        json!([{"name":"c","quote_style":"always"},{"name":"b","quote_style":"always"}]);
    write_job(
        d.path(),
        json!([csv(
            "data",
            "in.csv",
            json!([{"name":"a","nullable":true},{"name":"b"},{"name":"c","type":"decimal","precision":3}])
        )]),
        json!([]),
        json!([output]),
    );
    ok(invoke(d.path(), "check", &["--scan"]));
    assert!(!d.path().join("out.csv").exists());
    ok(invoke(d.path(), "run", &["--threads", "1"]));
    assert_eq!(
        fs::read_to_string(d.path().join("out.csv")).unwrap(),
        "a,\"b\",\"c\"\n,\"\",\"1\"\n\"a,b\",\"x\"\"y\",\"2\"\n\"line\nbreak\",\"z\",\"3\"\n"
    );
}
#[test]
fn joins_groups_union_and_stable_limit() {
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join("a.csv"), "k,v\nb,2\na,1\nb,3\n").unwrap();
    fs::write(d.path().join("b.csv"), "k,label\nb,B2\nb,B1\na,A\n").unwrap();
    let input = json!([
        csv(
            "a",
            "a.csv",
            json!([{"name":"k"},{"name":"v","type":"decimal","precision":3}])
        ),
        csv("b", "b.csv", json!([{"name":"k"},{"name":"label"}]))
    ]);
    let queries = [
        (
            "SELECT a.v,b.label FROM later a JOIN b ON a.k=b.k",
            "v,label\n2,B2\n2,B1\n1,A\n3,B2\n3,B1\n",
        ),
        (
            "SELECT k,COUNT(*) AS n FROM a GROUP BY k",
            "k,n\nb,2\na,1\n",
        ),
        ("SELECT DISTINCT k FROM a", "k\nb\na\n"),
        (
            "SELECT k FROM b UNION ALL SELECT k FROM a",
            "k\nb\nb\na\nb\na\nb\n",
        ),
        (
            "SELECT v,k FROM a ORDER BY k LIMIT 2 OFFSET 1",
            "v,k\n2,b\n3,b\n",
        ),
        ("SELECT k FROM a UNION SELECT k FROM b", "k\nb\na\n"),
        (
            "SELECT k FROM a INTERSECT ALL SELECT k FROM b WHERE label='B2'",
            "k\nb\n",
        ),
        (
            "SELECT k,COUNT(*) AS n FROM a GROUP BY GROUPING SETS ((k),())",
            "k,n\nb,2\n,3\na,1\n",
        ),
        ("SELECT k FROM a INTERSECT SELECT k FROM b", "k\nb\na\n"),
        (
            "SELECT k FROM a INTERSECT ALL SELECT k FROM b",
            "k\nb\na\nb\n",
        ),
        (
            "SELECT k FROM a EXCEPT ALL SELECT k FROM b WHERE label='B2'",
            "k\nb\na\n",
        ),
        (
            "SELECT k FROM (SELECT k FROM a ORDER BY k) sorted UNION ALL SELECT k FROM b",
            "k\na\nb\nb\nb\nb\na\n",
        ),
        (
            "SELECT a.k,b.label FROM a RIGHT JOIN b ON a.k=b.k",
            "k,label\nb,B2\nb,B2\nb,B1\nb,B1\na,A\n",
        ),
        (
            "SELECT k,ROW_NUMBER() OVER (ORDER BY k) AS rn,RANK() OVER (ORDER BY k) AS r FROM a",
            "k,rn,r\nb,2,2\na,1,1\nb,3,2\n",
        ),
    ];
    for (i, (sql, expected)) in queries.iter().enumerate() {
        let mut output = out(sql, "out.csv");
        output["mode"] = json!("overwrite");
        write_job(
            d.path(),
            input.clone(),
            json!([{"id":"later","sql":"SELECT * FROM first"},{"id":"first","sql":"WITH a AS (SELECT * FROM a) SELECT * FROM a"}]),
            json!([output]),
        );
        ok(invoke(
            d.path(),
            "run",
            &["--threads", if i % 2 == 0 { "4" } else { "1" }],
        ));
        assert_eq!(
            fs::read_to_string(d.path().join("out.csv")).unwrap(),
            *expected,
            "{sql}"
        );
    }
}
#[test]
fn import_origin_merge_and_errors() {
    let d = tempfile::tempdir().unwrap();
    fs::create_dir(d.path().join("defs")).unwrap();
    fs::write(d.path().join("defs/in.csv"), "value\nold\n").unwrap();
    fs::write(d.path().join("in.csv"), "value\nnew\n").unwrap();
    fs::write(
        d.path().join("defs/input.yml"),
        "id: data\nformat: csv\npath: in.csv\nencoding: utf-8\nfields: [{name: value}]\n",
    )
    .unwrap();
    fs::write(d.path().join("sql.yml"), "SELECT value FROM data").unwrap();
    fs::write(d.path().join("job.yml"),"input:\n  - import: defs/input.yml\n    path: in.csv\nprocess: []\noutput:\n  - id: result\n    sql: {import: sql.yml}\n    format: csv\n    path: out.csv\n    encoding: utf-8\n").unwrap();
    ok(invoke(d.path(), "run", &[]));
    assert_eq!(
        fs::read_to_string(d.path().join("out.csv")).unwrap(),
        "value\nnew\n"
    );
    fs::write(d.path().join("cycle.yml"), "import: cycle.yml").unwrap();
    fs::write(d.path().join("job.yml"), "import: cycle.yml").unwrap();
    let o = invoke(d.path(), "check", &[]);
    assert_eq!(o.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&o.stderr).contains("cyclic"));
}
#[test]
fn fixed_types_and_rdw_rest() {
    let d = tempfile::tempdir().unwrap();
    fs::write(
        d.path().join("in.dat"),
        [0xff, 0xfe, 0x12, 0x3d, 0x81, 0x01, b'A', b' '],
    )
    .unwrap();
    let fields = json!([{"name":"integer","type":"int-be","length":2},{"name":"amount","type":"packed","length":2,"precision":3},{"name":"flags","type":"bits","length":2,"bits":[{"name":"active","bit":0},{"name":"error","bit":7},{"name":"archived","bit":15}]},{"name":"label","length":2}]);
    let input = json!([{"id":"data","format":"fixed","path":"in.dat","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":fields}]);
    let mut output = json!({"id":"result","sql":"SELECT integer,amount,active,error,archived,label FROM data","format":"fixed","path":"out.dat","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":fields});
    write_job(d.path(), input.clone(), json!([]), json!([output]));
    ok(invoke(d.path(), "run", &[]));
    assert_eq!(
        fs::read(d.path().join("out.dat")).unwrap(),
        fs::read(d.path().join("in.dat")).unwrap()
    );
    output = json!({"id":"result","sql":"SELECT CAST(42 AS BIGINT) AS integer,label FROM data","format":"rdw","path":"out.rdw","encoding":"utf-8","fields":[{"name":"integer","type":"int-be","length":2},{"name":"label","length":"rest"}]});
    write_job(d.path(), input, json!([]), json!([output]));
    ok(invoke(d.path(), "run", &[]));
    assert_eq!(
        fs::read(d.path().join("out.rdw")).unwrap(),
        [0, 7, 0, 0, 0, 42, b'A']
    );
}
#[test]
fn utf16_output_and_stateful_encoding() {
    for encoding in ["utf-16le", "utf-16be", "iso-2022-jp", "cp932"] {
        let d = tempfile::tempdir().unwrap();
        let enc = dxtran::encoding::TextEncoding::resolve(encoding).unwrap();
        fs::write(
            d.path().join("in.csv"),
            enc.encode("name\n日本\n東京\n").unwrap(),
        )
        .unwrap();
        let mut input = csv("data", "in.csv", json!([{"name":"name"}]));
        input["encoding"] = json!(encoding);
        let mut output = out("SELECT * FROM data", "out.csv");
        output["encoding"] = json!(encoding);
        write_job(d.path(), json!([input]), json!([]), json!([output]));
        ok(invoke(d.path(), "run", &[]));
        assert_eq!(
            fs::read(d.path().join("out.csv")).unwrap(),
            enc.encode("name\n日本\n東京\n").unwrap()
        );
    }
}
#[test]
fn failures_preserve_outputs_and_temp_cleanup() {
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join("in.csv"), "v\nhello\n").unwrap();
    fs::write(d.path().join("old.csv"), b"previous").unwrap();
    let input = json!([csv("data", "in.csv", json!([{"name":"v"}]))]);
    let mut first = out("SELECT * FROM data", "old.csv");
    first["mode"] = json!("overwrite");
    let second = json!({"id":"bad","sql":"SELECT v FROM data","format":"fixed","path":"bad.dat","encoding":"utf-8","fixed":{"record_separator":"none"},"fields":[{"name":"v","length":1}]});
    write_job(d.path(), input, json!([]), json!([first, second]));
    let tmp = d.path().join("tmp");
    let o = invoke(d.path(), "run", &["--temp-dir", tmp.to_str().unwrap()]);
    assert_eq!(
        o.status.code(),
        Some(5),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert_eq!(fs::read(d.path().join("old.csv")).unwrap(), b"previous");
    assert!(!d.path().join("bad.dat").exists());
    assert_eq!(fs::read_dir(tmp).unwrap().count(), 0);
}
#[test]
fn check_metadata_vs_scan_and_explain() {
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join("in.csv"), "v\ninvalid\n").unwrap();
    write_job(
        d.path(),
        json!([csv(
            "data",
            "in.csv",
            json!([{"name":"v","type":"decimal","precision":4}])
        )]),
        json!([]),
        json!([out("SELECT v FROM data", "out.csv")]),
    );
    let temp = d.path().join("not-created");
    ok(invoke(
        d.path(),
        "check",
        &["--temp-dir", temp.to_str().unwrap()],
    ));
    let e = ok(invoke(
        d.path(),
        "explain",
        &["--temp-dir", temp.to_str().unwrap()],
    ));
    assert!(String::from_utf8_lossy(&e.stdout).contains("DxtranSource"));
    assert!(!temp.exists());
    assert_eq!(
        invoke(d.path(), "check", &["--scan"]).status.code(),
        Some(3)
    );
    assert_eq!(invoke(d.path(), "run", &[]).status.code(), Some(3));
    assert!(!d.path().join("out.csv").exists());
}

#[test]
fn specification_full_example() {
    let d = tempfile::tempdir().unwrap();
    fs::create_dir(d.path().join("data")).unwrap();
    fs::create_dir(d.path().join("out")).unwrap();
    let spec = include_str!("../docs/SPEC.md");
    let example = spec
        .split("## 5. 設定の全体例")
        .nth(1)
        .unwrap()
        .split("```yaml\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    fs::write(d.path().join("job.yml"), example).unwrap();
    fs::write(
        d.path().join("data/sales.dat"),
        b"000001000012345A\r\n000002000000200A\r\n000001000000100I\r\n",
    )
    .unwrap();
    fs::write(
        d.path().join("data/customers.csv"),
        b"customer_id,customer_name,region\n000001,Alice,east\n000002,Bob,west\n",
    )
    .unwrap();
    ok(invoke(d.path(), "run", &[]));
    assert_eq!(
        fs::read_to_string(d.path().join("out/details.csv")).unwrap(),
        "\"customer_id\",\"customer_name\",region,amount\n\"000001\",\"Alice\",east,123.45\n\"000002\",\"Bob\",west,2.00\n"
    );
    assert_eq!(
        fs::read(d.path().join("out/region_totals.dat")).unwrap(),
        b"east  000001000000012345\r\nwest  000001000000000200\r\n"
    );
}

#[test]
fn rdw_null_empty_binary_and_variable_lengths() {
    let d = tempfile::tempdir().unwrap();
    let mut bytes = vec![];
    for body in [b"\0\x01hello".as_slice(), b"\0\x02", b"\0\x03<NULL>"] {
        dxtran::framing::write_rdw(&mut bytes, body, 32760).unwrap();
    }
    fs::write(d.path().join("in.rdw"), &bytes).unwrap();
    let fields = json!([{"name":"id","length":2,"type":"uint-be"},{"name":"text","length":"rest","nullable":true,"null_if_pattern":"<NULL>","null_output":"<NULL>"}]);
    let input =
        json!([{"id":"data","format":"rdw","path":"in.rdw","encoding":"utf-8","fields":fields}]);
    let output = json!({"id":"result","format":"rdw","path":"out.rdw","sql":"SELECT * FROM data","encoding":"utf-8","fields":fields});
    write_job(d.path(), input, json!([]), json!([output]));
    ok(invoke(d.path(), "run", &[]));
    assert_eq!(fs::read(d.path().join("out.rdw")).unwrap(), bytes);
}

#[test]
fn rejects_path_collisions_and_invalid_configuration() {
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join("in.csv"), "v\n1\n").unwrap();
    let input = json!([csv("data", "in.csv", json!([{"name":"v"}]))]);
    let mut output = out("SELECT v FROM data", "in.csv");
    output["mode"] = json!("overwrite");
    write_job(d.path(), input.clone(), json!([]), json!([output]));
    assert_eq!(invoke(d.path(), "check", &[]).status.code(), Some(2));
    #[cfg(unix)]
    {
        fs::hard_link(d.path().join("in.csv"), d.path().join("alias.csv")).unwrap();
        let mut output = out("SELECT v FROM data", "alias.csv");
        output["mode"] = json!("overwrite");
        write_job(d.path(), input.clone(), json!([]), json!([output]));
        assert_eq!(invoke(d.path(), "check", &[]).status.code(), Some(2));
    }
    let mut output = out("SELECT v FROM data", "out.csv");
    output["fields"] = json!([{"name":"missing","quote_style":"always"}]);
    write_job(d.path(), input.clone(), json!([]), json!([output]));
    assert_eq!(invoke(d.path(), "check", &[]).status.code(), Some(2));
    write_job(
        d.path(),
        input,
        json!([{"id":"first","sql":"SELECT * FROM second"},{"id":"second","sql":"SELECT * FROM first"}]),
        json!([out("SELECT * FROM first", "out.csv")]),
    );
    assert_eq!(invoke(d.path(), "check", &[]).status.code(), Some(4));
    assert!(!d.path().join("out.csv").exists());
}

#[test]
fn input_larger_than_pool_and_external_sort() {
    use std::io::{BufRead, BufReader, BufWriter, Write};
    let d = tempfile::tempdir().unwrap();
    let mut file = BufWriter::new(fs::File::create(d.path().join("in.csv")).unwrap());
    writeln!(file, "id,payload").unwrap();
    let payload = "x".repeat(96);
    let rows = 200_000;
    for i in 0..rows {
        writeln!(file, "{i:06},{payload}").unwrap();
    }
    file.flush().unwrap();
    drop(file);
    assert!(fs::metadata(d.path().join("in.csv")).unwrap().len() > 16 * 1024 * 1024);
    write_job(
        d.path(),
        json!([csv(
            "data",
            "in.csv",
            json!([{"name":"id"},{"name":"payload"}])
        )]),
        json!([]),
        json!([out(
            "SELECT id,payload FROM data ORDER BY id DESC",
            "out.csv"
        )]),
    );
    let o = ok(invoke(
        d.path(),
        "run",
        &["--memory-limit", "16MiB", "--threads", "1"],
    ));
    let log = String::from_utf8(o.stderr).unwrap();
    let metrics: Value = log
        .lines()
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .find(|v| v["event"] == "output_staged")
        .unwrap();
    assert!(metrics["spill_count"].as_u64().unwrap() > 0, "{metrics}");
    let mut lines = BufReader::new(fs::File::open(d.path().join("out.csv")).unwrap()).lines();
    assert_eq!(lines.next().unwrap().unwrap(), "id,payload");
    for i in (0..rows).rev() {
        assert_eq!(lines.next().unwrap().unwrap(), format!("{i:06},{payload}"));
    }
    assert!(lines.next().is_none());
}

#[cfg(unix)]
#[test]
fn sigterm_cleans_only_its_own_work_directory() {
    use std::{
        io::Write,
        process::Stdio,
        time::{Duration, Instant},
    };
    let d = tempfile::tempdir().unwrap();
    let mut input_file = fs::File::create(d.path().join("in.csv")).unwrap();
    writeln!(input_file, "v").unwrap();
    for i in 0..100_000 {
        writeln!(input_file, "{i}").unwrap();
    }
    drop(input_file);
    write_job(
        d.path(),
        json!([csv("data", "in.csv", json!([{"name":"v"}]))]),
        json!([]),
        json!([out(
            "SELECT a.v,b.v AS other FROM data a CROSS JOIN data b",
            "out.csv"
        )]),
    );
    let tmp = d.path().join("tmp");
    fs::create_dir(&tmp).unwrap();
    fs::write(tmp.join("keep.txt"), b"keep").unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(d.path().join("job.yml"))
        .args(["--threads", "1", "--memory-limit", "16MiB", "--temp-dir"])
        .arg(&tmp)
        .stdout(Stdio::null())
        .stderr(fs::File::create(d.path().join("execution.log")).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if fs::read_dir(&tmp).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("dxtran-")
        }) {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited before work started: {status}");
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("child did not start");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!(
                "SIGTERM was not handled promptly: {}",
                fs::read_to_string(d.path().join("execution.log")).unwrap()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.code(), Some(4));
    assert!(!d.path().join("out.csv").exists());
    assert_eq!(fs::read_dir(&tmp).unwrap().count(), 1);
    assert_eq!(fs::read(tmp.join("keep.txt")).unwrap(), b"keep");
}

#[test]
fn fixed_rows_survive_byte_and_row_batch_boundaries() {
    let d = tempfile::tempdir().unwrap();
    // Wide strings reach the byte budget before the 8192-row budget. The next
    // row must be retained exactly once, with its original order key.
    for width in [32, 2048] {
        let mut input = Vec::new();
        for i in 0..8300 {
            input.extend_from_slice(
                format!("{i:0text_width$}{i:016}", text_width = width - 16).as_bytes(),
            );
        }
        fs::write(d.path().join("in.dat"), &input).unwrap();
        let fields = json!([{"name":"text","length":width-16,"trim":"none"}, {"name":"number","type":"uzoned","zone":"ascii","length":16}]);
        let common = json!({"format":"fixed","encoding":"utf-8","fixed":{"record_separator":"none","record_length":width},"fields":fields});
        let mut source = common.clone();
        source["id"] = json!("data");
        source["path"] = json!("in.dat");
        let mut target = common;
        target["id"] = json!("result");
        target["path"] = json!("out.dat");
        target["sql"] = json!("SELECT * FROM data");
        write_job(d.path(), json!([source]), json!([]), json!([target]));
        ok(invoke(d.path(), "run", &["--threads", "4"]));
        assert_eq!(fs::read(d.path().join("out.dat")).unwrap(), input);
        fs::remove_file(d.path().join("out.dat")).unwrap();
    }
}
