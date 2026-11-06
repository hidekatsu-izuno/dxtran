use dxtran::{config::Dataset, encoding::TextEncoding, layout::RecordLayout};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    process::{Command, Output},
};

fn physical() -> Value {
    json!({"format":"lines","encoding":"utf-8",
        "fields":[{"name":"code","length":2,"trim":false},
                  {"name":"label","length":4}]})
}

fn job(common: Value) -> Value {
    let mut input = common.clone();
    input["id"] = json!("data");
    input["path"] = json!("in.dat");
    let mut output = common;
    output["id"] = json!("result");
    output["path"] = json!("out.dat");
    output["mode"] = json!("overwrite");
    output["sql"] = json!("SELECT * FROM data");
    json!({"input":[input],"output":[output]})
}

fn invoke(dir: &Path, config: &Value, command: &str, threads: usize) -> Output {
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_dxt"));
    child
        .arg(command)
        .arg(dir.join("job.json"))
        .args(["--threads", &threads.to_string()]);
    if command == "check" {
        child.arg("--scan");
    }
    child.output().unwrap()
}

fn ok(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn short_blank_and_unterminated_lines_round_trip_with_padding() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), b"01A\n02\n\n03BC").unwrap();
    let mut config = job(physical());
    let checked = ok(invoke(dir.path(), &config, "check", 4));
    assert!(String::from_utf8_lossy(&checked.stdout).contains("4 records scanned"));
    for threads in [1, 4] {
        ok(invoke(dir.path(), &config, "run", threads));
        assert_eq!(
            fs::read(dir.path().join("out.dat")).unwrap(),
            b"01A\n02\n\n03BC\n"
        );
    }
    config["output"][0]["lines"] = json!({"trim_trailing_spaces":false});
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        b"01A   \n02    \n      \n03BC  \n"
    );
    config["output"][0] = json!({"id":"result","sql":"SELECT * FROM data",
        "format":"csv","encoding":"utf-8","path":"out.csv"});
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read(dir.path().join("out.csv")).unwrap(),
        b"code,label\n01,A\n02,\"\"\n  ,\"\"\n03,BC\n"
    );
}

#[test]
fn encoded_lines_and_fillers_preserve_character_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    for label in [
        "utf-8",
        "cp932",
        "utf-16le",
        "utf-16be",
        "cp939",
        "iso-2022-jp",
    ] {
        let encoding = TextEncoding::resolve(label).unwrap();
        for ending in ["lf", "crlf"] {
            let sep = if ending == "lf" { "\n" } else { "\r\n" };
            let input = encoding.encode(&format!("日本{sep}{sep}A")).unwrap();
            fs::write(dir.path().join("in.dat"), &input).unwrap();
            let config = job(json!({"format":"lines","encoding":label,
                "lines":{"line_ending":ending,"record_length":32},
                "fields":[{"name":"text","length":32}]}));
            ok(invoke(dir.path(), &config, "run", 4));
            assert_eq!(
                fs::read(dir.path().join("out.dat")).unwrap(),
                encoding.encode(&format!("日本{sep}{sep}A{sep}")).unwrap(),
                "{label}/{ending}"
            );
        }
        let unit = if label.starts_with("utf-16") { 2 } else { 1 };
        fs::write(dir.path().join("in.dat"), encoding.encode("AB\n").unwrap()).unwrap();
        let config = job(json!({"format":"lines","encoding":label,
            "lines":{"trim_trailing_spaces":false},
            "fields":[{"name":"text","length":unit*2},{"length":unit*2}]}));
        ok(invoke(dir.path(), &config, "run", 1));
        assert_eq!(
            fs::read(dir.path().join("out.dat")).unwrap(),
            encoding.encode("AB  \n").unwrap(),
            "{label}"
        );
    }
}

#[test]
fn oversized_lines_report_physical_offsets_and_protect_existing_output() {
    let dir = tempfile::tempdir().unwrap();
    let config = job(physical());
    for (input, message, record) in [
        (
            &b"A\n1234567\n"[..],
            "exceeds layout length",
            "record 2 byte 2",
        ),
        (
            &b"A\n1234567"[..],
            "exceeds layout length",
            "record 2 byte 2",
        ),
        (&b"A\nB\r\n"[..], "unexpected CR/LF", "record 2 byte 2"),
        (
            &b"A\n\xff\n"[..],
            "invalid encoded byte sequence",
            "record 2 byte 2",
        ),
    ] {
        fs::write(dir.path().join("in.dat"), input).unwrap();
        for threads in [1, 4] {
            fs::write(dir.path().join("out.dat"), b"previous").unwrap();
            let output = invoke(dir.path(), &config, "run", threads);
            let error = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(3), "{error}");
            assert!(error.contains(message) && error.contains(record), "{error}");
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"previous");
        }
    }
    let mut config = config;
    config["input"][0]["lines"] = json!({"line_ending":"crlf"});
    fs::write(dir.path().join("in.dat"), b"A\n").unwrap();
    let output = invoke(dir.path(), &config, "check", 4);
    assert_eq!(output.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&output.stderr).contains("separator mismatch"));
}

#[test]
fn embedded_line_breaks_in_output_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for text in ["A\nB", "A\rB", "A\r\nB"] {
        fs::write(dir.path().join("in.csv"), format!("text\n\"{text}\"\n")).unwrap();
        fs::write(dir.path().join("out.dat"), b"previous").unwrap();
        let mut config = job(json!({"format":"lines","encoding":"utf-8",
            "fields":[{"name":"text","length":8}]}));
        config["input"][0] = json!({"id":"data","format":"csv","path":"in.csv",
            "encoding":"utf-8","fields":[{"name":"text"}]});
        let output = invoke(dir.path(), &config, "run", 4);
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(5), "{error}");
        assert!(
            error.contains("contains CR/LF") && error.contains("record 1"),
            "{error}"
        );
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"previous");
    }
}

#[test]
fn null_outputs_use_fixed_width_defaults() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), b"text,amount\n,\n").unwrap();
    let mut config = job(json!({"format":"lines","encoding":"utf-8",
        "fields":[{"name":"text","length":3,"nullable":true},
                  {"name":"amount","length":5,"type":"decimal","precision":3,"nullable":true}]}));
    config["input"][0] = json!({"id":"data","format":"csv","path":"in.csv","encoding":"utf-8",
        "fields":[{"name":"text","nullable":true},
                  {"name":"amount","type":"decimal","precision":3,"nullable":true}]});
    ok(invoke(dir.path(), &config, "run", 1));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"       0\n");
}

#[test]
fn empty_files_and_blank_records_remain_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let config = job(physical());
    for input in [&b""[..], &b"\n\n"[..], &b"  \n\n"[..]] {
        fs::write(dir.path().join("in.dat"), input).unwrap();
        ok(invoke(dir.path(), &config, "run", 4));
        let expected = if input.is_empty() {
            &b""[..]
        } else {
            &b"\n\n"[..]
        };
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), expected);
    }
}

#[test]
fn invalid_options_and_binary_storage_are_configuration_errors() {
    let base = json!({"id":"data","path":"unused","format":"lines","encoding":"utf-8",
        "fields":[{"name":"text","length":4}]});
    let mut invalid = Vec::new();
    for options in [
        json!({"line_ending":"none"}),
        json!({"record_length":5}),
        json!({"record_length":0}),
    ] {
        let mut config = base.clone();
        config["lines"] = options;
        invalid.push(config);
    }
    for kind in ["binary", "int-le", "packed", "bits"] {
        let mut config = base.clone();
        config["fields"][0]["type"] = json!(kind);
        invalid.push(config);
    }
    let mut odd_utf16 = base.clone();
    odd_utf16["encoding"] = json!("utf-16le");
    odd_utf16["fields"][0]["length"] = json!(3);
    invalid.push(odd_utf16);
    let mut rest = base.clone();
    rest["fields"][0]["length"] = json!("rest");
    invalid.push(rest);
    let mut wrong_block = base.clone();
    wrong_block["fixed"] = json!({"record_separator":"lf"});
    invalid.push(wrong_block);
    for format in ["csv", "fixed", "rdw", "parquet", "lua"] {
        let mut config = base.clone();
        config["format"] = json!(format);
        config["lines"] = json!({});
        invalid.push(config);
    }
    for config in invalid {
        let dataset: Dataset = serde_json::from_value(config.clone()).unwrap();
        assert_eq!(
            RecordLayout::input(&dataset).unwrap_err().code,
            2,
            "{config}"
        );
    }
}

#[test]
fn bstrings_and_conditional_fields_use_line_padding() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), b"Hfirst\nDtwo\nH\n").unwrap();
    let config = job(json!({"format":"lines","encoding":"utf-8",
        "fields":[{"name":"kind","length":1},
                  {"name":"value","type":"bstring","length":5,"when":"kind = 'H'"},
                  {"name":"value","type":"bstring","offset":1,"length":5,"when":"kind = 'D'"}]}));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        b"Hfirst\nDtwo\nH\n"
    );
}

fn compressed(codec: &str, bytes: &[u8]) -> Vec<u8> {
    if codec == "gzip" {
        let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    } else {
        zstd::stream::encode_all(bytes, 1).unwrap()
    }
}

#[test]
fn compression_and_row_batches_preserve_line_order() {
    let dir = tempfile::tempdir().unwrap();
    for label in ["utf-8", "utf-16le"] {
        let encoding = TextEncoding::resolve(label).unwrap();
        let text: String = (0..9001).map(|n| format!("{n:05}\r\n")).collect();
        let bytes = encoding.encode(&text).unwrap();
        for codec in ["gzip", "zstd"] {
            let cut = 7; // Split an encoded character or a delimiter between members.
            let input = [
                compressed(codec, &bytes[..cut]),
                compressed(codec, &bytes[cut..]),
            ]
            .concat();
            fs::write(dir.path().join("in.dat"), input).unwrap();
            let config = job(
                json!({"format":"lines","encoding":label,"compression":codec,
                "lines":{"line_ending":"crlf"},"fields":[{"name":"text","length":16}]}),
            );
            for threads in [1, 4] {
                ok(invoke(dir.path(), &config, "run", threads));
                let output = fs::read(dir.path().join("out.dat")).unwrap();
                let decoded = if codec == "gzip" {
                    let mut decoded = Vec::new();
                    flate2::read::MultiGzDecoder::new(output.as_slice())
                        .read_to_end(&mut decoded)
                        .unwrap();
                    decoded
                } else {
                    zstd::stream::decode_all(output.as_slice()).unwrap()
                };
                assert_eq!(decoded, bytes, "{label}/{codec}/{threads}");
            }
        }
    }
}
