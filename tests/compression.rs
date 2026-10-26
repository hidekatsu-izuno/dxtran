use dxtran::encoding::TextEncoding;
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    process::{Command, Output},
};

fn encode(codec: &str, bytes: &[u8]) -> Vec<u8> {
    if codec == "gzip" {
        let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    } else {
        let mut writer = zstd::stream::Encoder::new(Vec::new(), 1).unwrap();
        writer.include_checksum(true).unwrap();
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    }
}
fn decode(codec: &str, bytes: &[u8]) -> Vec<u8> {
    if codec == "gzip" {
        let mut result = Vec::new();
        flate2::read::MultiGzDecoder::new(bytes)
            .read_to_end(&mut result)
            .unwrap();
        result
    } else {
        zstd::stream::decode_all(bytes).unwrap()
    }
}
fn invoke(dir: &Path, job: &Value, command: &str, scan: bool, threads: usize) -> Output {
    fs::create_dir_all(dir.join("tmp")).unwrap();
    fs::write(dir.join("job.json"), job.to_string()).unwrap();
    let mut p = Command::new(env!("CARGO_BIN_EXE_dxt"));
    p.arg(command)
        .arg(dir.join("job.json"))
        .args([
            "--threads",
            &threads.to_string(),
            "--memory-limit",
            "128MiB",
            "--temp-dir",
        ])
        .arg(dir.join("tmp"));
    if scan {
        p.arg("--scan");
    }
    p.output().unwrap()
}
fn ok(output: Output) -> Output {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn job(common: Value) -> Value {
    let mut input = common.clone();
    input["id"] = json!("data");
    input["path"] = json!("in.bin");
    let mut output = common;
    output["id"] = json!("result");
    output["path"] = json!("out.bin");
    output["sql"] = json!("SELECT * FROM data");
    output["mode"] = json!("overwrite");
    json!({"input":[input],"output":[output]})
}
fn fixed(format: &str) -> Value {
    let mut common = json!({"format":format,"encoding":"utf-8",
        "fields":[{"name":"text","length":8,"trim":"none"},
                  {"name":"n","type":"uzoned","zone":"ascii","length":8,"precision":8}]});
    if format == "fixed" {
        common["fixed"] = json!({"record_separator":"crlf"});
    }
    common
}

#[test]
fn compressed_csv_preserves_encoding_nulls_and_concatenated_members() {
    let dir = tempfile::tempdir().unwrap();
    let text = "text,amount\r\n\"日本,\"\"語\"\"\r\n続き\",12.30\r\n\"\",-0.50\r\n,0.00\r\n";
    for encoding in ["utf-8", "utf-16le", "shift_jis", "iso-2022-jp"] {
        let bytes = TextEncoding::resolve(encoding)
            .unwrap()
            .encode(text)
            .unwrap();
        for input_codec in ["gzip", "zstd"] {
            // Codec boundaries intentionally split encoded characters and CSV syntax.
            let mid = bytes.len() / 3;
            let compressed = [
                encode(input_codec, &bytes[..mid]),
                encode(input_codec, b""),
                encode(input_codec, &bytes[mid..]),
            ]
            .concat();
            fs::write(dir.path().join("in.bin"), compressed).unwrap();
            let mut config = job(json!({"format":"csv","encoding":encoding,
                "csv":{"line_ending":"crlf"},"fields":[{"name":"text","nullable":true},
                    {"name":"amount","type":"decimal","precision":8,"scale":2}]}));
            config["input"][0].as_object_mut().unwrap().remove("csv");
            config["input"][0]["compression"] = json!(input_codec);
            let scanned = ok(invoke(dir.path(), &config, "check", true, 4));
            assert!(String::from_utf8_lossy(&scanned.stdout).contains("3 records scanned"));
            for output_codec in ["gzip", "zstd"] {
                config["output"][0]["compression"] = json!(output_codec);
                ok(invoke(dir.path(), &config, "run", false, 4));
                let output = fs::read(dir.path().join("out.bin")).unwrap();
                assert_eq!(
                    decode(output_codec, &output),
                    bytes,
                    "{encoding}: {input_codec} -> {output_codec}"
                );
            }
        }
    }
}

#[test]
fn compressed_physical_records_keep_order_across_parallel_output_batches() {
    let dir = tempfile::tempdir().unwrap();
    for format in ["fixed", "rdw"] {
        let mut bytes = Vec::new();
        for n in 0..24001 {
            if format == "rdw" {
                bytes.extend_from_slice(&[0, 20, 0, 0]);
            }
            bytes.extend_from_slice(format!("{n:08}{n:08}").as_bytes());
            if format == "fixed" {
                bytes.extend_from_slice(b"\r\n");
            }
        }
        for input_codec in [None, Some("gzip"), Some("zstd")] {
            fs::write(
                dir.path().join("in.bin"),
                input_codec.map_or_else(|| bytes.clone(), |c| encode(c, &bytes)),
            )
            .unwrap();
            let mut config = job(fixed(format));
            if let Some(c) = input_codec {
                config["input"][0]["compression"] = json!(c);
            }
            for output_codec in ["gzip", "zstd"] {
                config["output"][0]["compression"] = json!(output_codec);
                let mut first = None;
                for threads in [1, 4] {
                    ok(invoke(dir.path(), &config, "run", false, threads));
                    let output = fs::read(dir.path().join("out.bin")).unwrap();
                    assert_eq!(decode(output_codec, &output), bytes);
                    if let Some(first) = &first {
                        assert!(
                            &output == first,
                            "compression must be reproducible: {format} {input_codec:?} -> {output_codec}"
                        );
                    }
                    first = Some(output);
                }
            }
            config["output"][0]
                .as_object_mut()
                .unwrap()
                .remove("compression");
            config["output"][0]["sql"] =
                json!("SELECT * FROM data ORDER BY n % 7 LIMIT 9000 OFFSET 8000");
            let stride = if format == "fixed" { 18 } else { 20 };
            let mut indices: Vec<usize> = (0..24001).collect();
            indices.sort_by_key(|n| n % 7);
            let expected: Vec<u8> = indices[8000..17000]
                .iter()
                .flat_map(|n| bytes[n * stride..(n + 1) * stride].iter().copied())
                .collect();
            ok(invoke(dir.path(), &config, "run", false, 4));
            assert_eq!(fs::read(dir.path().join("out.bin")).unwrap(), expected);
        }
    }
}

#[test]
fn empty_compressed_outputs_are_complete_streams() {
    let dir = tempfile::tempdir().unwrap();
    for common in [
        json!({"format":"csv","encoding":"utf-8","csv":{"header":false},"fields":[{"name":"text"}]}),
        fixed("fixed"),
        fixed("rdw"),
    ] {
        for codec in ["gzip", "zstd"] {
            fs::write(dir.path().join("in.bin"), encode(codec, b"")).unwrap();
            let mut config = job(common.clone());
            config["input"][0]["compression"] = json!(codec);
            config["output"][0]["compression"] = json!(codec);
            ok(invoke(dir.path(), &config, "run", false, 4));
            let output = fs::read(dir.path().join("out.bin")).unwrap();
            assert!(!output.is_empty());
            assert!(decode(codec, &output).is_empty());
            let scanned = ok(invoke(dir.path(), &config, "check", true, 4));
            assert!(String::from_utf8_lossy(&scanned.stdout).contains("0 records scanned"));
        }
    }
}

#[test]
fn corruption_truncation_and_wrong_compression_preserve_existing_outputs() {
    let dir = tempfile::tempdir().unwrap();
    for codec in ["gzip", "zstd"] {
        let bytes = encode(codec, b"0000000100000001\r\n");
        let mut checksum = bytes.clone();
        let crc = checksum.len() - if codec == "gzip" { 8 } else { 1 };
        checksum[crc] ^= 0x80;
        let mut trailing = bytes.clone();
        trailing.extend_from_slice(b"garbage");
        for invalid in [
            Vec::new(),
            b"plain bytes".to_vec(),
            bytes[..bytes.len() - 1].to_vec(),
            checksum,
            trailing,
            encode(
                if codec == "gzip" { "zstd" } else { "gzip" },
                b"0000000100000001\r\n",
            ),
        ] {
            fs::write(dir.path().join("in.bin"), invalid).unwrap();
            let mut config = job(fixed("fixed"));
            config["input"][0]["compression"] = json!(codec);
            config["output"][0]["compression"] = json!(codec);
            fs::write(dir.path().join("out.bin"), b"previous").unwrap();
            for (command, scan) in [("check", true), ("run", false)] {
                let output = invoke(dir.path(), &config, command, scan, 4);
                assert_eq!(
                    output.status.code(),
                    Some(3),
                    "{codec}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(fs::read(dir.path().join("out.bin")).unwrap(), b"previous");
                assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
            }
        }
    }
}

#[test]
fn compression_is_explicit_and_validated_before_output_creation() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = b"0000000100000001\r\n";
    fs::write(dir.path().join("in.gz"), bytes).unwrap();
    let mut config = job(fixed("fixed"));
    config["input"][0]["path"] = json!("in.gz");
    config["output"][0]["path"] = json!("out.zst");
    ok(invoke(dir.path(), &config, "run", false, 4));
    assert_eq!(fs::read(dir.path().join("out.zst")).unwrap(), bytes);
    for invalid in [
        json!("zip"),
        json!("auto"),
        json!("none"),
        json!(true),
        json!({"type":"gzip"}),
    ] {
        let mut job = config.clone();
        job["output"][0]["compression"] = invalid;
        let result = invoke(dir.path(), &job, "run", false, 4);
        assert_eq!(
            result.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(fs::read(dir.path().join("out.zst")).unwrap(), bytes);
    }
    config["output"][0] = json!({"id":"result","format":"parquet","path":"out.parquet","sql":"SELECT * FROM data","compression":"gzip"});
    let result = invoke(dir.path(), &config, "run", false, 4);
    assert_eq!(result.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&result.stderr).contains("parquet.compression"));
    assert!(!dir.path().join("out.parquet").exists());
}

#[test]
fn compressed_fixed_errors_use_decompressed_record_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = b"0000000100000001\r\n".repeat(9000);
    bytes[8192 * 18 + 8] = b'x';
    for codec in ["gzip", "zstd"] {
        fs::write(dir.path().join("in.bin"), encode(codec, &bytes)).unwrap();
        let mut config = job(fixed("fixed"));
        config["input"][0]["compression"] = json!(codec);
        config["output"][0] = json!({"id":"result","path":"out.bin","mode":"overwrite","format":"csv","encoding":"utf-8","compression":codec,"sql":"SELECT text FROM data"});
        fs::write(dir.path().join("out.bin"), b"previous").unwrap();
        let result = invoke(dir.path(), &config, "run", false, 4);
        let error = String::from_utf8_lossy(&result.stderr);
        assert_eq!(result.status.code(), Some(3), "{error}");
        assert!(
            error.contains("record 8193 byte 147456") && error.contains("column n"),
            "{error}"
        );
        assert_eq!(fs::read(dir.path().join("out.bin")).unwrap(), b"previous");
    }
}
