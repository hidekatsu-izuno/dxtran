use dxtran::{config::Dataset, encoding::TextEncoding, layout::RecordLayout, source::InputReader};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Output},
};

const COMMON: &str =
    "127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";

fn input(format: &str) -> Value {
    let mut fields = json!([
        {"name":"host"}, {"name":"ident","nullable":true}, {"name":"user","nullable":true},
        {"name":"time"},
        {"name":"request"}, {"name":"status","type":"decimal","precision":3},
        {"name":"bytes","type":"decimal","precision":12,"nullable":true}
    ]);
    if format == "combined" {
        fields.as_array_mut().unwrap().extend([
            json!({"name":"referer","nullable":true}),
            json!({"name":"agent","nullable":true}),
        ]);
    }
    json!({"id":"access","format":"apache","encoding":"utf-8","path":"access.log",
        "apache":{"format":format},"fields":fields})
}

fn job(format: &str) -> Value {
    json!({"input":[input(format)],"output":[{"id":"result","format":"csv","encoding":"utf-8",
        "path":"out.csv","mode":"overwrite","sql":"SELECT * FROM access"}]})
}

fn invoke(dir: &Path, config: &Value, command: &str, threads: usize) -> Output {
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    process
        .arg(command)
        .arg(dir.join("job.json"))
        .args(["--threads", &threads.to_string()]);
    if command == "check" {
        process.arg("--scan");
    }
    process.output().unwrap()
}

fn success(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn common_and_combined_logs_support_types_nulls_checks_and_sql() {
    let dir = tempfile::tempdir().unwrap();
    for format in ["common", "combined"] {
        let suffix = if format == "common" {
            ""
        } else {
            " \"-\" \"Browser/1.0 (test)\""
        };
        let second = COMMON.replace("frank", "-").replace("200 2326", "404 -");
        fs::write(
            dir.path().join("access.log"),
            format!("{COMMON}{suffix}\r\n{second}{suffix}"),
        )
        .unwrap();
        let mut config = job(format);
        config["input"][0]["check"] = json!({"when":"status < 100 OR status > 599","level":"error","message":"invalid status"});
        config["output"][0]["sql"] = json!(
            "SELECT host, ident, \"user\", to_char(from_unixtime(to_unixtime(\"time\", '%d/%b/%Y:%H:%M:%S %z')), '%Y-%m-%dT%H:%M:%SZ') AS stamp, request, status, bytes FROM access"
        );
        let checked = success(invoke(dir.path(), &config, "check", 4));
        assert!(String::from_utf8_lossy(&checked.stdout).contains("2 records scanned"));
        for threads in [1, 4] {
            success(invoke(dir.path(), &config, "run", threads));
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "host,ident,user,stamp,request,status,bytes\n127.0.0.1,,frank,2000-10-10T20:55:36Z,GET /apache_pb.gif HTTP/1.0,200,2326\n127.0.0.1,,,2000-10-10T20:55:36Z,GET /apache_pb.gif HTTP/1.0,404,\n"
            );
        }
        config["output"][0]["sql"] = json!(
            "SELECT status, COUNT(*) AS requests, SUM(bytes) AS total_bytes FROM access GROUP BY status ORDER BY status"
        );
        success(invoke(dir.path(), &config, "run", 4));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "status,requests,total_bytes\n200,1,2326\n404,1,\n"
        );
    }
}

#[test]
fn combined_is_default_and_escapes_preserve_empty_strings_and_literal_dashes() {
    let dir = tempfile::tempdir().unwrap();
    let data = format!(
        "{COMMON} \"\" \"a\\\"b\\\\c\\t\\xE6\\x97\\xA5\\nend\"\n{COMMON} \"-\" \"\\x2d\"\n"
    );
    fs::write(dir.path().join("access.log"), data).unwrap();
    let mut config = job("combined");
    config["input"][0].as_object_mut().unwrap().remove("apache");
    config["output"][0]["sql"] = json!("SELECT referer, agent FROM access");
    success(invoke(dir.path(), &config, "run", 1));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "referer,agent\n\"\",\"a\"\"b\\c\t日\nend\"\n,-\n"
    );
}

#[test]
fn encoded_compressed_inputs_and_bom_are_streamed() {
    let dir = tempfile::tempdir().unwrap();
    for label in ["utf-8", "cp932", "utf-16le", "utf-16be", "iso-2022-jp"] {
        let encoding = TextEncoding::resolve(label).unwrap();
        let data = encoding
            .encode(&format!("{COMMON} \"-\" \"日本語\\\"x\\\"\"\n"))
            .unwrap();
        for compression in [None, Some("gzip"), Some("zstd")] {
            let bytes = match compression {
                None => data.clone(),
                Some("gzip") => {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(&data).unwrap();
                    encoder.finish().unwrap()
                }
                Some("zstd") => zstd::stream::encode_all(data.as_slice(), 0).unwrap(),
                _ => unreachable!(),
            };
            fs::write(dir.path().join("access.log"), bytes).unwrap();
            let mut config = job("combined");
            config["input"][0]["encoding"] = json!(label);
            if let Some(compression) = compression {
                config["input"][0]["compression"] = json!(compression);
            }
            config["output"][0]["sql"] = json!("SELECT agent FROM access");
            success(invoke(dir.path(), &config, "run", 4));
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "agent\n\"日本語\"\"x\"\"\"\n",
                "{label}/{compression:?}"
            );
        }
    }
    fs::write(dir.path().join("access.log"), format!("\u{feff}{COMMON}\n")).unwrap();
    success(invoke(dir.path(), &job("common"), "run", 1));

    fs::write(
        dir.path().join("access.log"),
        format!("{COMMON} \"-\" \"\\x93\\xfa\\x96{{\\x95\\\\\"\n"),
    )
    .unwrap();
    let mut config = job("combined");
    config["input"][0]["encoding"] = json!("cp932");
    config["output"][0]["sql"] = json!("SELECT agent FROM access");
    success(invoke(dir.path(), &config, "run", 1));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "agent\n日本表\n"
    );
}

#[test]
fn malformed_lines_and_typed_values_report_record_and_column_without_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let config = job("common");
    for (bad, diagnostic) in [
        (COMMON.replace("\"GET", "GET"), "enclosed in quotes"),
        (COMMON.replace("200 2326", "bad 2326"), "column status"),
        (COMMON.replace("127.0.0.1", "-"), "column host"),
        (format!("{COMMON} extra"), "trailing Apache log fields"),
        (COMMON.replace("GET", "\\q"), "invalid Apache log escape"),
        ("".into(), "invalid or missing Apache log field"),
    ] {
        fs::write(dir.path().join("access.log"), format!("{COMMON}\n{bad}\n")).unwrap();
        for command in ["check", "run"] {
            fs::write(dir.path().join("out.csv"), "previous").unwrap();
            let result = invoke(dir.path(), &config, command, 4);
            let error = String::from_utf8_lossy(&result.stderr);
            assert_eq!(result.status.code(), Some(3), "{error}");
            assert!(
                error.contains(diagnostic)
                    && error.contains("input access")
                    && error.contains("record 2"),
                "{error}"
            );
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "previous"
            );
        }
    }
}

#[test]
fn invalid_options_and_apache_output_are_configuration_errors() {
    let mut source = input("common");
    let mut cases = Vec::new();
    for (key, value) in [
        ("apache", json!({"format":"custom"})),
        ("csv", json!({})),
        ("fields", json!([{"name":"only_one"}])),
    ] {
        let mut invalid = source.clone();
        invalid[key] = value;
        cases.push(invalid);
    }
    for field in [
        json!({"name":"host","length":12}),
        json!({"name":"host","when":"true"}),
        json!({"name":"host","source_name":"remote_host"}),
        json!({"name":"host","type":"int-be"}),
        json!({"length":12}),
    ] {
        let mut invalid = source.clone();
        invalid["fields"][0] = field;
        cases.push(invalid);
    }
    for format in ["csv", "lua", "parquet"] {
        let mut invalid = source.clone();
        invalid["format"] = json!(format);
        cases.push(invalid);
    }
    for invalid in cases {
        let dataset: Dataset = serde_json::from_value(invalid.clone()).unwrap();
        assert!(RecordLayout::input(&dataset).is_err(), "{invalid}");
    }
    let dataset: Dataset = serde_json::from_value(source.clone()).unwrap();
    let layout = RecordLayout::input(&dataset).unwrap();
    assert!(
        RecordLayout::output(&dataset, layout.schema)
            .unwrap_err()
            .message
            .contains("input-only")
    );

    let dir = tempfile::tempdir().unwrap();
    let mut config = job("common");
    source["sql"] = json!("SELECT * FROM access");
    config["output"] = json!([source]);
    let result = invoke(dir.path(), &config, "check", 1);
    assert_eq!(result.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&result.stderr).contains("input-only"));
}

#[test]
fn empty_inputs_and_multiple_batches_keep_the_original_order() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("access.log"), "").unwrap();
    let mut config = job("common");
    config["output"][0]["sql"] = json!("SELECT request FROM access");
    success(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "request\n"
    );
    let mut data = String::new();
    let mut expected = String::from("request\n");
    for number in 0..8300 {
        data.push_str(&COMMON.replace("/apache_pb.gif", &format!("/{number}")));
        data.push('\n');
        expected.push_str(&format!("GET /{number} HTTP/1.0\n"));
    }
    fs::write(dir.path().join("access.log"), data).unwrap();
    for threads in [1, 4] {
        success(invoke(dir.path(), &config, "run", threads));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            expected
        );
    }
    // The public row reader and SQL batch path use the same input conversions.
    let mut source = input("common");
    source["path"] = json!(dir.path().join("access.log"));
    let dataset: Dataset = serde_json::from_value(source).unwrap();
    let layout = RecordLayout::input(&dataset).unwrap();
    let mut reader = InputReader::open(&dataset, &layout).unwrap();
    assert!(reader.next_row().unwrap().is_some());
}

fn custom_job(log_format: &str, fields: Value) -> Value {
    let mut config = job("common");
    config["input"][0]["apache"] = json!({"format":"custom","log_format":log_format});
    config["input"][0]["fields"] = fields;
    config
}

#[test]
fn custom_log_format_supports_virtual_hosts_request_parts_and_typed_columns() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = custom_job(
        r#"%v:%p %a [%{%Y-%m-%dT%H:%M:%S%z}t] "%m %U%q %H" %>s %b %D "%{User-Agent}i" %%"#,
        json!([
            {"name":"vhost"}, {"name":"port","type":"decimal","precision":5}, {"name":"host"},
            {"name":"logged_at"}, {"name":"method"}, {"name":"path"}, {"name":"query_string"},
            {"name":"protocol"}, {"name":"status","type":"decimal","precision":3},
            {"name":"bytes","type":"decimal","nullable":true}, {"name":"duration","type":"decimal"},
            {"name":"agent","nullable":true}
        ]),
    );
    config["input"][0]["check"] =
        json!({"when":"port <> 443","level":"error","message":"unexpected port"});
    fs::write(dir.path().join("access.log"), concat!(
        "example.com:443 192.0.2.1 [2026-11-14T20:00:00+0900] \"GET /items?id=1 HTTP/2\" 200 321 1200 \"Browser\\\"test\" %\r\n",
        "example.com:443 192.0.2.2 [2026-11-14T20:00:01+0900] \"POST /submit HTTP/1.1\" 204 - 850 \"-\" %",
    )).unwrap();
    let scanned = success(invoke(dir.path(), &config, "check", 4));
    assert!(String::from_utf8_lossy(&scanned.stdout).contains("2 records scanned"));
    for threads in [1, 4] {
        success(invoke(dir.path(), &config, "run", threads));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            concat!(
                "vhost,port,host,logged_at,method,path,query_string,protocol,status,bytes,duration,agent\n",
                "example.com,443,192.0.2.1,2026-11-14T20:00:00+0900,GET,/items,?id=1,HTTP/2,200,321,1200,\"Browser\"\"test\"\n",
                "example.com,443,192.0.2.2,2026-11-14T20:00:01+0900,POST,/submit,\"\",HTTP/1.1,204,,850,\n",
            )
        );
    }
}

#[test]
fn custom_formats_keep_encoding_nulls_conditional_headers_and_literal_dashes() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = custom_job(
        r#"prefix.+(%h)\t%t\t\"%{User-Agent}i\"\t%X\t%200X\t\"%!200,304{Referer}i\""#,
        json!([
            {"name":"host"}, {"name":"logged_at"}, {"name":"agent"}, {"name":"connection"},
            {"name":"conditional","nullable":true}, {"name":"referer","nullable":true},
        ]),
    );
    config["input"][0]["encoding"] = json!("cp932");
    config["input"][0]["compression"] = json!("zstd");
    let data = TextEncoding::resolve("cp932").unwrap().encode(
        "prefix.+(127.0.0.1)\t[10/Oct/2000:13:55:36 -0700]\t\"\\x93\\xfa\\x96{\\x95\\\\\"\t-\t-\t\"-\"\n",
    ).unwrap();
    fs::write(
        dir.path().join("access.log"),
        zstd::stream::encode_all(data.as_slice(), 0).unwrap(),
    )
    .unwrap();
    success(invoke(dir.path(), &config, "check", 4));
    success(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "host,logged_at,agent,connection,conditional,referer\n127.0.0.1,10/Oct/2000:13:55:36 -0700,日本表,-,,\n"
    );
}

#[test]
fn custom_configuration_errors_are_reported_before_reading_inputs() {
    let valid = custom_job("%v %>s", json!([{"name":"host"},{"name":"status"}]));
    for options in [
        json!({"format":"custom"}),
        json!({"format":"custom","log_format":""}),
        json!({"format":"custom","log_format":"literal %%"}),
        json!({"format":"custom","log_format":"%v%s"}),
        json!({"format":"custom","log_format":"%h %i"}),
        json!({"format":"custom","log_format":"%h %z"}),
        json!({"format":"custom","log_format":"%h"}),
        json!({"format":"common","log_format":"%h %s"}),
        json!({"log_format":"%h %s"}),
    ] {
        let mut source = valid["input"][0].clone();
        source["apache"] = options;
        let dataset: Dataset = serde_json::from_value(source.clone()).unwrap();
        assert!(RecordLayout::input(&dataset).is_err(), "{source}");
    }
    let dir = tempfile::tempdir().unwrap();
    let mut config = valid;
    fs::write(dir.path().join("access.log"), "not a log record").unwrap();
    config["input"][0]["apache"]["log_format"] = json!("%v%s");
    let result = invoke(dir.path(), &config, "check", 4);
    assert_eq!(result.status.code(), Some(2));
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(error.contains("literal separator"), "{error}");
    assert!(!dir.path().join("out.csv").exists());
}

#[test]
fn malformed_custom_records_preserve_existing_outputs_and_error_locations() {
    let dir = tempfile::tempdir().unwrap();
    let config = custom_job(
        r#"host=%v "%r" %>s"#,
        json!([
            {"name":"host"}, {"name":"request"}, {"name":"status","type":"decimal","precision":3},
        ]),
    );
    let first = "host=example.com \"GET / HTTP/1.1\" 200\n";
    for bad in [
        "wrong=example.com \"GET / HTTP/1.1\" 200",
        "host=example.com \"GET / HTTP/1.1 200",
        "host=example.com \"GET / HTTP/1.1\" abc",
        "host=example.com \"GET / HTTP/1.1\" 200 extra",
        "host=example.com \"GET / HTTP/1.1\" 1000",
    ] {
        fs::write(dir.path().join("access.log"), format!("{first}{bad}\n")).unwrap();
        fs::write(dir.path().join("out.csv"), "previous").unwrap();
        for command in ["check", "run"] {
            let result = invoke(dir.path(), &config, command, 4);
            let error = String::from_utf8_lossy(&result.stderr);
            assert_eq!(result.status.code(), Some(3), "{error}");
            assert!(
                error.contains("input access")
                    && error.contains(&format!("record 2 byte {}", first.len())),
                "{error}"
            );
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "previous"
            );
        }
    }
}
