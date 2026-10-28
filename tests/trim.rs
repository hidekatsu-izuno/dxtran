use datafusion::common::ScalarValue;
use dxtran::{
    config::Format,
    encoding::TextEncoding,
    field::{CompiledField, Direction},
};
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command};

fn field(spec: Value, format: Format, direction: Direction, encoding: &str) -> CompiledField {
    CompiledField::compile(
        serde_json::from_value(spec).unwrap(),
        format,
        direction,
        0,
        TextEncoding::resolve(encoding).unwrap(),
    )
    .unwrap()
}

fn run(dir: &Path, config: &Value, threads: usize) {
    fs::write(dir.join("job.yml"), config.to_string()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(dir.join("job.yml"))
        .args(["--threads", &threads.to_string(), "--temp-dir"])
        .arg(dir)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn physical_input_trim_preserves_other_whitespace_and_multibyte_characters() {
    for encoding in ["utf-8", "utf-16le", "shift_jis"] {
        let codec = TextEncoding::resolve(encoding).unwrap();
        for pad in [" ", "　"] {
            let original = format!("{pad}{pad}A\tB{pad}{pad}");
            let bytes = codec.encode(&original).unwrap();
            for (trim, expected) in [
                (json!(false), original.clone()),
                (json!("none"), original.clone()),
                (json!("left"), format!("A\tB{pad}{pad}")),
                (json!("right"), format!("{pad}{pad}A\tB")),
                (json!("both"), "A\tB".into()),
            ] {
                for format in [Format::Fixed, Format::Rdw] {
                    let f = field(
                        json!({"name":"text", "length":bytes.len(), "trim":trim, "pad_char":pad}),
                        format,
                        Direction::Input,
                        encoding,
                    );
                    assert_eq!(
                        f.decode(&bytes).unwrap(),
                        [ScalarValue::Utf8(Some(expected.clone()))],
                        "{encoding}, {pad:?}, {trim}, {format:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn fixed_output_trims_before_width_validation_and_pads_again() {
    let value = [ScalarValue::Utf8(Some("  A  ".into()))];
    for (trim, expected) in [
        (json!(false), "  A  "),
        (json!("none"), "  A  "),
        (json!("left"), "A    "),
        (json!("right"), "  A  "),
        (json!("both"), "A    "),
    ] {
        let f = field(
            json!({"name":"text", "length":5, "trim":trim}),
            Format::Fixed,
            Direction::Output,
            "utf-8",
        );
        assert_eq!(
            f.encode(&value, Format::Fixed).unwrap(),
            expected.as_bytes()
        );
        let mut bytes = Vec::new();
        f.encode_string_into("  A  ", &mut bytes).unwrap();
        assert_eq!(bytes, expected.as_bytes());
    }

    // A value that is too wide before trimming must fit after trimming.
    for (trim, value) in [("left", "    A"), ("right", "A    "), ("both", "  A  ")] {
        let f = field(
            json!({"name":"text", "length":2, "trim":trim}),
            Format::Fixed,
            Direction::Output,
            "utf-8",
        );
        assert_eq!(
            f.encode(&[ScalarValue::Utf8(Some(value.into()))], Format::Fixed)
                .unwrap(),
            b"A "
        );
    }

    let f = field(
        json!({"name":"text", "length":8, "trim":"both", "pad_char":"　"}),
        Format::Fixed,
        Direction::Output,
        "utf-16le",
    );
    assert_eq!(
        f.encode(&[ScalarValue::Utf8(Some("　A　".into()))], Format::Fixed)
            .unwrap(),
        [0x41, 0, 0, 0x30, 0, 0x30, 0, 0x30]
    );
}

#[test]
fn rdw_rest_output_trims_before_max_length_and_null_collision_checks() {
    let f = field(
        json!({"name":"text", "length":"rest", "max_length":2, "trim":"both",
            "nullable":true, "null_if":["N"], "null_output":"N"}),
        Format::Rdw,
        Direction::Output,
        "utf-8",
    );
    assert_eq!(
        f.encode(&[ScalarValue::Utf8(Some("  A  ".into()))], Format::Rdw)
            .unwrap(),
        b"A"
    );
    assert!(
        f.encode(&[ScalarValue::Utf8(Some("  N  ".into()))], Format::Rdw)
            .unwrap_err()
            .message
            .contains("collides with NULL")
    );
    assert_eq!(
        f.decode(b" N ").unwrap_err().code,
        3,
        "max_length still applies to physical input bytes"
    );
    assert_eq!(f.decode(b"N ").unwrap(), [ScalarValue::Utf8(None)]);
    assert_eq!(
        f.encode(&[ScalarValue::Utf8(None)], Format::Rdw).unwrap(),
        b"N"
    );
}

#[test]
fn csv_input_and_output_support_all_trim_modes_in_serial_and_parallel() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"disabled", "trim":false},
        {"name":"left_side", "trim":"left"},
        {"name":"right_side", "trim":"right"},
        {"name":"both_sides", "trim":"both"},
        {"name":"legacy", "trim":"none"},
        {"name":"unchanged"}
    ]);
    let input = "  A B  ,  A B  ,  A B  ,  A B  ,  A B  ,  A B  \n   ,   ,   ,   ,   ,   \n";
    let expected =
        "  A B  ,A B  ,  A B,A B,  A B  ,  A B  \n   ,\"\",\"\",\"\",   ,   \n".repeat(9001);
    fs::write(dir.path().join("in.csv"), input.repeat(9001)).unwrap();
    let config = json!({
        "input":[{"id":"data", "format":"csv", "path":"in.csv", "encoding":"utf-8",
            "csv":{"header":false}, "fields":fields}],
        "output":[
            {"id":"input_trim", "format":"csv", "path":"input-trim.csv", "encoding":"utf-8",
                "mode":"overwrite", "csv":{"header":false}, "sql":"SELECT * FROM data"},
            {"id":"output_trim", "format":"csv", "path":"output-trim.csv", "encoding":"utf-8",
                "mode":"overwrite", "csv":{"header":false}, "fields":fields,
                "sql":"SELECT unchanged AS disabled, unchanged AS left_side, unchanged AS right_side, unchanged AS both_sides, unchanged AS legacy, unchanged FROM data"}
        ]
    });
    for threads in [1, 4] {
        run(dir.path(), &config, threads);
        for path in ["input-trim.csv", "output-trim.csv"] {
            assert_eq!(fs::read_to_string(dir.path().join(path)).unwrap(), expected);
        }
    }
}

#[test]
fn csv_trim_keeps_null_distinct_from_empty_and_preserves_other_whitespace() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), ",\"\",   ,\"   \",\" \t \",　\n").unwrap();
    let config = json!({
        "input":[{"id":"data", "format":"csv", "path":"in.csv", "encoding":"utf-8",
            "csv":{"header":false}, "fields":[
                {"name":"null_value", "trim":"both", "nullable":true},
                {"name":"empty", "trim":"both"},
                {"name":"spaces", "trim":"both"},
                {"name":"quoted_spaces", "trim":"both"},
                {"name":"tab", "trim":"both"},
                {"name":"wide_space", "trim":"both"}]}],
        "output":[{"id":"result", "format":"csv", "path":"out.csv", "encoding":"utf-8",
            "mode":"overwrite", "csv":{"header":false}, "sql":"SELECT * FROM data"}]
    });
    for threads in [1, 4] {
        run(dir.path(), &config, threads);
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            ",\"\",\"\",\"\",\t,　\n"
        );
    }
}

#[test]
fn csv_trim_applies_before_type_parsing_and_after_boolean_formatting() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "  1.20  ,   Y ,2026-10-28  \n").unwrap();
    let config = json!({
        "input":[{"id":"data", "format":"csv", "path":"in.csv", "encoding":"utf-8",
            "csv":{"header":false}, "fields":[
                {"name":"amount", "type":"decimal", "precision":4, "scale":2, "trim":"both"},
                {"name":"flag", "type":"boolean", "trim":"left", "true_values":[" Y "], "false_values":[" N "]},
                {"name":"day", "type":"date", "trim":"right"}]}],
        "output":[{"id":"result", "format":"csv", "path":"out.csv", "encoding":"utf-8",
            "mode":"overwrite", "csv":{"header":false}, "sql":"SELECT * FROM data",
            "fields":[{"name":"flag", "trim":"left", "true_values":["  Y "], "false_values":["  N "]}]}]
    });
    for threads in [1, 4] {
        run(dir.path(), &config, threads);
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "1.20,Y ,2026-10-28\n"
        );
    }
}

#[test]
fn fixed_and_rdw_output_trim_is_used_by_batch_writers() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "  AB  ,  CD  \n".repeat(9001)).unwrap();
    let config = json!({
        "input":[{"id":"data", "format":"csv", "path":"in.csv", "encoding":"utf-8",
            "csv":{"header":false}, "fields":[{"name":"first"}, {"name":"second"}]}],
        "output":[
            {"id":"fixed_result", "format":"fixed", "path":"out.dat", "encoding":"utf-8",
                "mode":"overwrite", "fixed":{"record_separator":"none"}, "sql":"SELECT * FROM data",
                "fields":[{"name":"first", "length":4, "trim":"both"}, {"name":"second", "length":6, "trim":false}]},
            {"id":"rdw_result", "format":"rdw", "path":"out.rdw", "encoding":"utf-8",
                "mode":"overwrite", "sql":"SELECT * FROM data",
                "fields":[{"name":"first", "length":4, "trim":"both"}, {"name":"second", "length":"rest", "trim":"right"}]}
        ]
    });
    for threads in [1, 4] {
        run(dir.path(), &config, threads);
        assert_eq!(
            fs::read(dir.path().join("out.dat")).unwrap(),
            b"AB    CD  ".repeat(9001)
        );
        assert_eq!(
            fs::read(dir.path().join("out.rdw")).unwrap(),
            b"\x00\x0c\x00\x00AB    CD".repeat(9001)
        );
    }
}

#[test]
fn invalid_trim_values_and_binary_trim_are_configuration_errors() {
    for trim in [
        json!(true),
        json!(1),
        json!("false"),
        json!("center"),
        json!([]),
    ] {
        let spec = serde_json::from_value(json!({"name":"text", "trim":trim}));
        let result = spec.map_err(|e| e.to_string()).and_then(|spec| {
            CompiledField::compile(
                spec,
                Format::Csv,
                Direction::Input,
                0,
                TextEncoding::resolve("utf-8").unwrap(),
            )
            .map_err(|e| {
                assert_eq!(e.code, 2);
                e.message
            })
        });
        assert!(result.is_err(), "{trim}");
    }
    for format in [Format::Csv, Format::Fixed, Format::Rdw] {
        let mut spec = json!({"name":"bytes", "type":"binary", "trim":false});
        if format != Format::Csv {
            spec["length"] = json!(2);
        }
        let error = CompiledField::compile(
            serde_json::from_value(spec).unwrap(),
            format,
            Direction::Input,
            0,
            TextEncoding::resolve("utf-8").unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.code, 2);
        assert!(error.message.contains("text options"));
    }
}
