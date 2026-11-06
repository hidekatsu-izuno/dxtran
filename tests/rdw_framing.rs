use dxtran::{config::Dataset, encoding::TextEncoding, layout::RecordLayout};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
    process::{Command, Output},
};

fn segment(flag: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = ((body.len() + 4) as u16).to_be_bytes().to_vec();
    bytes.extend_from_slice(&[flag, 0]);
    bytes.extend_from_slice(body);
    bytes
}
fn block(records: &[Vec<u8>]) -> Vec<u8> {
    segment(0, &records.concat())
}
fn job(input_options: Value, output_options: Value, fields: Value) -> Value {
    json!({"input":[{"id":"data","format":"rdw","path":"in.dat","encoding":"utf-8","rdw":input_options,"fields":fields}],
        "output":[{"id":"result","format":"rdw","path":"out.dat","encoding":"utf-8","rdw":output_options,
                   "fields":fields,"sql":"SELECT * FROM data","mode":"overwrite"}]})
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
fn binary_fields() -> Value {
    json!([{"name":"payload","type":"binary","length":"rest"}])
}

#[test]
fn all_four_modes_convert_to_legacy_rdw_without_changing_logical_records() {
    let dir = tempfile::tempdir().unwrap();
    let records = [b"ABCDEFGHIJ".as_slice(), b"KLM".as_slice()];
    let legacy = records
        .iter()
        .map(|body| segment(0, body))
        .collect::<Vec<_>>()
        .concat();
    for bdw in [false, true] {
        for spanned in [false, true] {
            fs::write(dir.path().join("in.dat"), &legacy).unwrap();
            let options = json!({"bdw":bdw,"spanned":spanned,"max_block_length":18});
            let mut config = job(json!({}), options.clone(), binary_fields());
            let mut expected = None;
            for threads in [1, 4] {
                ok(invoke(dir.path(), &config, "run", threads));
                let bytes = fs::read(dir.path().join("out.dat")).unwrap();
                if let Some(expected) = &expected {
                    assert_eq!(&bytes, expected);
                } else {
                    expected = Some(bytes);
                }
            }
            fs::write(dir.path().join("in.dat"), expected.unwrap()).unwrap();
            config["input"][0]["rdw"] = options;
            config["output"][0]["rdw"] = json!({});
            let checked = ok(invoke(dir.path(), &config, "check", 4));
            assert!(String::from_utf8_lossy(&checked.stdout).contains("2 records scanned"));
            ok(invoke(dir.path(), &config, "run", 4));
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), legacy);
        }
    }
}

#[test]
fn reassembly_precedes_character_decoding_and_packed_field_conversion() {
    let dir = tempfile::tempdir().unwrap();
    for label in ["utf-8", "utf-16le", "cp939"] {
        let encoding = TextEncoding::resolve(label).unwrap();
        let text = encoding.encode("日本語").unwrap();
        let body = [text.clone(), vec![0x12, 0x3c]].concat();
        let segments: Vec<_> = body
            .iter()
            .enumerate()
            .map(|(i, byte)| {
                let flag = if i == 0 {
                    1
                } else if i + 1 == body.len() {
                    2
                } else {
                    3
                };
                block(&[segment(flag, &[*byte])])
            })
            .collect();
        fs::write(dir.path().join("in.dat"), segments.concat()).unwrap();
        let mut config = job(
            json!({"bdw":true,"spanned":true,"max_block_length":9}),
            json!({}),
            json!([{"name":"text","length":text.len(),"trim":false},
                   {"name":"amount","type":"packed","length":2,"precision":3}]),
        );
        config["input"][0]["encoding"] = json!(label);
        config["output"][0]["encoding"] = json!(label);
        config["input"][0]["check"] =
            json!({"when":"amount > 0","level":"info","message":"input-logical"});
        config["output"][0]["check"] =
            json!({"when":"amount > 0","level":"info","message":"output-logical"});
        let output = ok(invoke(dir.path(), &config, "run", 4));
        for message in ["input-logical", "output-logical"] {
            let count = String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .filter(|event| event.get("message") == Some(&json!(message)))
                .count();
            assert_eq!(count, 1);
        }
        assert_eq!(
            fs::read(dir.path().join("out.dat")).unwrap(),
            segment(0, &body),
            "{label}"
        );
    }
}

#[test]
fn blocks_survive_batch_boundaries_and_checks_count_logical_records() {
    let dir = tempfile::tempdir().unwrap();
    let records: Vec<_> = (0..9001).map(|n| format!("{n:05}").into_bytes()).collect();
    let legacy = records
        .iter()
        .map(|body| segment(0, body))
        .collect::<Vec<_>>()
        .concat();
    for spanned in [false, true] {
        fs::write(dir.path().join("in.dat"), &legacy).unwrap();
        let mut config = job(
            json!({}),
            json!({"bdw":true,"spanned":spanned}),
            json!([{"name":"text","length":5}]),
        );
        let mut expected = None;
        for threads in [1, 4] {
            ok(invoke(dir.path(), &config, "run", threads));
            let bytes = fs::read(dir.path().join("out.dat")).unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&bytes, expected);
            } else {
                expected = Some(bytes);
            }
        }
        let expected = expected.unwrap();
        // Spanning fills the last five bytes with a header and one payload byte.
        let first_block_length = if spanned { 32760 } else { 32755 };
        assert_eq!(
            u16::from_be_bytes([expected[0], expected[1]]),
            first_block_length
        );
        fs::write(dir.path().join("in.dat"), &expected).unwrap();
        config["input"][0]["rdw"] = json!({"bdw":true,"spanned":spanned});
        config["input"][0]["check"] =
            json!({"when":"text = '08192'","level":"warn","message":"once"});
        config["output"][0]["rdw"] = json!({});
        let output = ok(invoke(dir.path(), &config, "run", 4));
        let messages: Vec<Value> = String::from_utf8_lossy(&output.stderr)
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|event| event.get("message") == Some(&json!("once")))
            .collect();
        assert_eq!(messages.len(), 1);
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), legacy);
    }
}

#[test]
fn failures_report_block_positions_and_preserve_existing_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let first = block(&[segment(0, b"A")]);
    for (tail, message, spanned) in [
        (vec![0, 8, 0, 0, 0, 9, 0, 0], "exceeds remaining BDW", false),
        (vec![0, 9, 0, 0, 0, 5, 0, 0], "truncated record", false),
        (block(&[segment(1, b"B")]), "invalid RDW/SDW", false),
        (
            block(&[segment(2, b"B")]),
            "invalid spanned record sequence",
            true,
        ),
        (
            block(&[segment(1, b"B")]),
            "unterminated spanned record",
            true,
        ),
        (
            block(&[segment(0, b"\xff")]),
            "invalid encoded byte sequence",
            false,
        ),
    ] {
        fs::write(dir.path().join("in.dat"), [first.clone(), tail].concat()).unwrap();
        let config = job(
            json!({"bdw":true,"spanned":spanned}),
            json!({}),
            json!([{"name":"text","length":1}]),
        );
        for threads in [1, 4] {
            fs::write(dir.path().join("out.dat"), b"previous").unwrap();
            let output = invoke(dir.path(), &config, "run", threads);
            let error = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(3), "{error}");
            assert!(
                error.contains(message) && error.contains("record 2") && error.contains("block 2"),
                "{error}"
            );
            assert!(error.contains("byte 9"), "{error}");
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"previous");
        }
    }
}

#[test]
fn empty_inputs_and_zero_length_records_have_explicit_behavior() {
    let dir = tempfile::tempdir().unwrap();
    for bdw in [false, true] {
        for spanned in [false, true] {
            let config = job(
                json!({}),
                json!({"bdw":bdw,"spanned":spanned}),
                binary_fields(),
            );
            fs::write(dir.path().join("in.dat"), []).unwrap();
            ok(invoke(dir.path(), &config, "run", 4));
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"");
            fs::write(dir.path().join("in.dat"), segment(0, b"")).unwrap();
            let output = invoke(dir.path(), &config, "run", 4);
            if spanned {
                assert_eq!(output.status.code(), Some(5));
            } else {
                ok(output);
                let expected = if bdw {
                    block(&[segment(0, b"")])
                } else {
                    segment(0, b"")
                };
                assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), expected);
            }
        }
    }
}

#[test]
fn an_oversized_output_record_does_not_publish_preceding_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = [
        segment(0, b"12345678"),
        segment(0, b"A"),
        segment(0, b"123456789"),
    ]
    .concat();
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    fs::write(dir.path().join("out.dat"), b"previous").unwrap();
    let config = job(
        json!({}),
        json!({"bdw":true,"max_block_length":16}),
        binary_fields(),
    );
    let output = invoke(dir.path(), &config, "run", 4);
    let error = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(5), "{error}");
    assert!(
        error.contains("record 3") && error.contains("does not fit in a BDW block"),
        "{error}"
    );
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"previous");
}

#[test]
fn spanned_records_can_exceed_the_16_bit_header_length() {
    let dir = tempfile::tempdir().unwrap();
    let body = vec![0x9a; 70000];
    fs::write(
        dir.path().join("in.dat"),
        [
            segment(1, &body[..30000]),
            segment(3, &body[30000..60000]),
            segment(2, &body[60000..]),
        ]
        .concat(),
    )
    .unwrap();
    for bdw in [false, true] {
        let options = json!({"spanned":true,"max_record_length":70004});
        let mut output_options = options.clone();
        output_options["bdw"] = json!(bdw);
        let config = job(options, output_options, binary_fields());
        ok(invoke(dir.path(), &config, "run", 4));
        let bytes = fs::read(dir.path().join("out.dat")).unwrap();
        let path = dir.path().join("readback.dat");
        fs::write(&path, bytes).unwrap();
        let mut dataset: Dataset = serde_json::from_value(config["output"][0].clone()).unwrap();
        dataset.path = path;
        let layout = RecordLayout::input(&dataset).unwrap();
        let mut reader = dxtran::source::InputReader::open(&dataset, &layout).unwrap();
        let row = reader.next_row().unwrap().unwrap();
        assert_eq!(
            row[0],
            datafusion::common::ScalarValue::Binary(Some(body.clone()))
        );
        assert!(reader.next_row().unwrap().is_none());
    }
}

#[test]
fn invalid_boolean_types_limits_and_unknown_keys_are_rejected() {
    for options in [
        json!({"bdw":"true"}),
        json!({"spanned":1}),
        json!({"bwd":true}),
    ] {
        let value = json!({"id":"data","format":"rdw","path":"unused","encoding":"utf-8","rdw":options,"fields":binary_fields()});
        assert!(serde_json::from_value::<Dataset>(value).is_err());
    }
    for options in [
        json!({"max_block_length":7}),
        json!({"max_block_length":32761}),
        json!({"bdw":true,"spanned":true,"max_block_length":8}),
        json!({"max_record_length":65536}),
        json!({"spanned":true,"max_record_length":4}),
        json!({"spanned":true,"max_record_length":16777221}),
    ] {
        let dataset: Dataset = serde_json::from_value(json!({"id":"data","format":"rdw","path":"unused","encoding":"utf-8","rdw":options,"fields":binary_fields()})).unwrap();
        assert_eq!(RecordLayout::input(&dataset).unwrap_err().code, 2);
    }
}

fn compress(codec: &str, bytes: &[u8]) -> Vec<u8> {
    if codec == "gzip" {
        let mut writer = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        writer.write_all(bytes).unwrap();
        writer.finish().unwrap()
    } else {
        zstd::stream::encode_all(bytes, 1).unwrap()
    }
}

#[test]
fn compression_member_boundaries_can_split_bdw_sdw_and_payload() {
    let dir = tempfile::tempdir().unwrap();
    let input = [
        block(&[segment(1, b"ABC")]),
        block(&[segment(2, b"DEF"), segment(0, b"G")]),
    ]
    .concat();
    for codec in ["gzip", "zstd"] {
        let chunks: Vec<_> = input
            .chunks(3)
            .map(|chunk| compress(codec, chunk))
            .collect();
        fs::write(dir.path().join("in.dat"), chunks.concat()).unwrap();
        let mut config = job(
            json!({"bdw":true,"spanned":true}),
            json!({"bdw":true,"spanned":true,"max_block_length":16}),
            binary_fields(),
        );
        config["input"][0]["compression"] = json!(codec);
        config["output"][0]["compression"] = json!(codec);
        let mut expected = None;
        for threads in [1, 4] {
            ok(invoke(dir.path(), &config, "run", threads));
            let bytes = fs::read(dir.path().join("out.dat")).unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&bytes, expected);
            } else {
                expected = Some(bytes.clone());
            }
            let decoded = if codec == "gzip" {
                let mut decoded = Vec::new();
                flate2::read::MultiGzDecoder::new(bytes.as_slice())
                    .read_to_end(&mut decoded)
                    .unwrap();
                decoded
            } else {
                zstd::stream::decode_all(bytes.as_slice()).unwrap()
            };
            assert_eq!(
                decoded,
                [block(&[segment(0, b"ABCDEF")]), block(&[segment(0, b"G")])].concat()
            );
        }
    }
}
