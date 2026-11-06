use datafusion::{arrow::datatypes::DataType, common::ScalarValue};
use dxtran::{
    config::{Field, Format},
    encoding::TextEncoding,
    field::{CompiledField, Direction, decimal_text, parse_decimal},
};
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command};

fn compile(spec: Value, direction: Direction) -> dxtran::error::Result<CompiledField> {
    CompiledField::compile(
        serde_json::from_value::<Field>(spec).unwrap(),
        Format::Fixed,
        direction,
        0,
        TextEncoding::UTF_8,
    )
}

#[test]
fn binary_integer_scaling_preserves_all_widths_endianness_and_extreme_values() {
    for kind in ["int-be", "int-le", "uint-be", "uint-le"] {
        for length in 1..=8 {
            let signed = kind.starts_with("int");
            let bits = length * 8;
            let max = if signed {
                (1i128 << (bits - 1)) - 1
            } else {
                (1i128 << bits) - 1
            };
            let min = if signed { -(1i128 << (bits - 1)) } else { 0 };
            let precision = min.unsigned_abs().max(max as u128).to_string().len() as u8;
            for scale in [-38i8, -3, 0, 2, 6, 38] {
                let f = compile(
                    json!({"name":"n", "type":kind, "length":length, "scale":scale}),
                    Direction::Output,
                )
                .unwrap();
                let arrow_precision = precision.max(scale.max(0) as u8);
                assert_eq!(f.data_type(), DataType::Decimal128(arrow_precision, scale));
                for coefficient in [min, 0, 1, max] {
                    let mut expected = coefficient.to_be_bytes()[16 - length..].to_vec();
                    if kind.ends_with("le") {
                        expected.reverse();
                    }
                    let value = ScalarValue::Decimal128(Some(coefficient), arrow_precision, scale);
                    assert_eq!(
                        f.decode(&expected).unwrap().as_slice(),
                        std::slice::from_ref(&value)
                    );
                    assert_eq!(f.encode(&[value], Format::Fixed).unwrap(), expected);
                }
                assert_eq!(
                    f.encode(&[f.null()], Format::Fixed).unwrap(),
                    vec![0; length]
                );
            }
        }
    }
}

#[test]
fn zoned_and_packed_scaling_supports_both_positions_of_p() {
    for (kind, length, bytes) in [
        ("zoned", 3, "f1f2c3"),
        ("lzoned", 4, "4ef1f2f3"),
        ("tzoned", 4, "f1f2f34e"),
        ("uzoned", 3, "f1f2f3"),
        ("packed", 2, "123c"),
        ("upacked", 2, "123f"),
        ("npacked", 2, "0123"),
    ] {
        for scale in [-3i8, 2, 6, 38] {
            let f = compile(
                json!({"name":"n", "type":kind, "length":length,
                    "precision":3, "scale":scale}),
                Direction::Output,
            )
            .unwrap();
            let value = ScalarValue::Decimal128(Some(123), 3.max(scale.max(0) as u8), scale);
            let bytes = hex::decode(bytes).unwrap();
            assert_eq!(
                f.decode(&bytes).unwrap().as_slice(),
                std::slice::from_ref(&value),
                "{kind}/{scale}"
            );
            assert_eq!(
                f.encode(&[value], Format::Fixed).unwrap(),
                bytes,
                "{kind}/{scale}"
            );
        }
    }
    for (scale, expected) in [(-3, "123000"), (2, "1.23"), (6, "0.000123")] {
        assert_eq!(decimal_text(123, scale), expected);
        assert_eq!(parse_decimal(expected, 3, scale).unwrap(), 123);
    }
}

#[test]
fn signed_scale_text_handles_zero_signs_and_values_beyond_i128_logical_range() {
    for (text, scale, coefficient) in [
        ("123000", -3, 123),
        ("-123000.00", -3, -123),
        ("+100.00", -2, 1),
        ("0", -38, 0),
        ("-0.000", -38, 0),
        ("0.000123", 6, 123),
    ] {
        assert_eq!(parse_decimal(text, 3, scale).unwrap(), coefficient);
    }
    for text in ["1", "123001", "123000.01", "1e3", "", "000001"] {
        assert!(parse_decimal(text, 3, -3).is_err(), "{text}");
    }
    assert_eq!(decimal_text(0, -38), "0");
    assert_eq!(decimal_text(-123, -3), "-123000");
    let coefficient = 10i128.pow(38) - 1;
    let text = format!("{}{}", "9".repeat(38), "0".repeat(38));
    assert_eq!(decimal_text(coefficient, -38), text);
    assert_eq!(parse_decimal(&text, 38, -38).unwrap(), coefficient);
    let f = compile(
        json!({"name":"n", "type":"decimal", "length":76,
            "precision":38, "scale":-38}),
        Direction::Output,
    )
    .unwrap();
    let value = ScalarValue::Decimal128(Some(coefficient), 38, -38);
    assert_eq!(
        f.decode(text.as_bytes()).unwrap().as_slice(),
        std::slice::from_ref(&value)
    );
    assert_eq!(f.encode(&[value], Format::Fixed).unwrap(), text.as_bytes());
}

#[test]
fn scaled_integer_output_rejects_fraction_loss_overflow_and_negative_unsigned_values() {
    let f = compile(
        json!({"name":"n", "type":"int-be", "length":1, "scale":2}),
        Direction::Output,
    )
    .unwrap();
    for (coefficient, scale) in [(128, 2), (-129, 2), (1234, 3)] {
        let err = f
            .encode(
                &[ScalarValue::Decimal128(Some(coefficient), 8, scale)],
                Format::Fixed,
            )
            .unwrap_err();
        assert_eq!(err.code, 5);
    }
    assert_eq!(
        f.encode(&[ScalarValue::Decimal128(Some(12300), 8, 4)], Format::Fixed)
            .unwrap(),
        [123]
    );
    assert!(
        f.encode(&[ScalarValue::Int64(Some(1))], Format::Fixed)
            .is_err()
    );
    let f = compile(
        json!({"name":"n", "type":"uint-le", "length":1, "scale":-2}),
        Direction::Output,
    )
    .unwrap();
    for coefficient in [-100, 25600, 12301] {
        assert!(
            f.encode(
                &[ScalarValue::Decimal128(Some(coefficient), 8, 0)],
                Format::Fixed
            )
            .is_err()
        );
    }
    assert_eq!(
        f.encode(&[ScalarValue::Decimal128(Some(12300), 8, 0)], Format::Fixed)
            .unwrap(),
        [123]
    );
}

#[test]
fn physical_precision_is_enforced_even_when_arrow_precision_is_larger() {
    for (kind, bytes) in [("uint-be", "007b"), ("npacked", "0123")] {
        let f = compile(
            json!({"name":"n", "type":kind, "length":2, "precision":2, "scale":6}),
            Direction::Output,
        )
        .unwrap();
        assert_eq!(f.data_type(), DataType::Decimal128(6, 6));
        assert_eq!(f.decode(&hex::decode(bytes).unwrap()).unwrap_err().code, 3);
        assert_eq!(
            f.encode(&[ScalarValue::Decimal128(Some(123), 6, 6)], Format::Fixed)
                .unwrap_err()
                .code,
            5
        );
    }
    let f = compile(
        json!({"name":"n", "type":"uint-be", "length":2, "scale":6,
            "nullable":true, "null_bytes":"ffff"}),
        Direction::Input,
    )
    .unwrap();
    assert_eq!(f.decode(&[255, 255]).unwrap(), [f.null()]);
}

#[test]
fn scale_configuration_rejects_non_numeric_storage_and_invalid_ranges() {
    for kind in [
        "string",
        "binary",
        "bstring",
        "boolean",
        "float-be",
        "hfloat-be",
    ] {
        let err = compile(
            json!({"name":"n", "type":kind, "length":4, "scale":2}),
            Direction::Input,
        )
        .unwrap_err();
        assert_eq!(err.code, 2, "{kind}");
    }
    for kind in ["decimal", "packed", "int-be", "uint-le"] {
        for scale in [-39, 39, 127, -128] {
            let err = compile(
                json!({"name":"n", "type":kind, "length":4, "scale":scale}),
                Direction::Input,
            )
            .unwrap_err();
            assert_eq!(err.code, 2, "{kind}/{scale}");
        }
    }
    let f = compile(
        json!({"name":"n", "type":"int-be", "length":2}),
        Direction::Input,
    )
    .unwrap();
    assert_eq!(f.data_type(), DataType::Int64);
}

fn invoke(dir: &Path, config: &Value, command: &str, threads: usize) {
    fs::write(dir.join("job.json"), config.to_string()).unwrap();
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    process
        .arg(command)
        .arg(dir.join("job.json"))
        .args(["--threads", &threads.to_string(), "--temp-dir"])
        .arg(dir.join("tmp"));
    if command == "check" {
        process.arg("--scan");
    }
    let output = process.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn scaled_binary_zoned_and_packed_fields_round_trip_through_batches_and_csv() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"amount", "type":"int-le", "length":2, "scale":2},
        {"name":"big", "type":"uint-be", "length":8, "scale":-2},
        {"name":"fraction", "type":"uzoned", "zone":"ascii", "length":3,
            "precision":3, "scale":6},
        {"name":"thousands", "type":"packed", "length":2, "scale":-3},
        {"name":"text", "type":"decimal", "length":7, "precision":3, "scale":-2}
    ]);
    let body = hex::decode("7b00ffffffffffffffff313233123c20203132333030").unwrap();
    let rows = 8193;
    for format in ["fixed", "rdw"] {
        let mut record = Vec::new();
        if format == "rdw" {
            record.extend_from_slice(&((body.len() + 4) as u16).to_be_bytes());
            record.extend_from_slice(&[0, 0]);
        }
        record.extend_from_slice(&body);
        let expected = record.repeat(rows);
        fs::write(dir.path().join("in.dat"), &expected).unwrap();
        let mut input = json!({"id":"data", "format":format, "path":"in.dat",
            "encoding":"utf-8", "fields":fields});
        if format == "fixed" {
            input["fixed"] = json!({"record_separator":"none"});
        }
        let mut output = input.clone();
        output["id"] = json!("copy");
        output["path"] = json!("out.dat");
        output["sql"] = json!("SELECT * FROM data");
        output["mode"] = json!("overwrite");
        let config = json!({"input":[input], "output":[output,
            {"id":"readable", "format":"csv", "path":"out.csv", "encoding":"utf-8",
                "sql":"SELECT * FROM data", "mode":"overwrite"},
            {"id":"calculated", "format":"csv", "path":"calculated.csv", "encoding":"utf-8",
                "sql":"SELECT CAST(amount + CAST('1.00' AS DECIMAL(5,2)) AS DECIMAL(8,2)) AS amount, CAST(thousands + CAST(1 AS DECIMAL(10,0)) AS DECIMAL(12,0)) AS thousands, CAST(fraction * CAST(1000000 AS DECIMAL(7,0)) AS DECIMAL(12,6)) AS fraction FROM data",
                "mode":"overwrite"}]});
        invoke(dir.path(), &config, "check", 1);
        for threads in [1, 4] {
            invoke(dir.path(), &config, "run", threads);
            assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), expected);
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                format!(
                    "amount,big,fraction,thousands,text\n{}",
                    "1.23,1844674407370955161500,0.000123,123000,12300\n".repeat(rows)
                )
            );
            assert_eq!(
                fs::read_to_string(dir.path().join("calculated.csv")).unwrap(),
                format!(
                    "amount,thousands,fraction\n{}",
                    "2.23,123001,123.000000\n".repeat(rows)
                )
            );
        }
        let mut readback = config.clone();
        readback["input"] = json!([{
            "id":"data", "format":"csv", "path":"out.csv", "encoding":"utf-8",
            "fields":[
                {"name":"amount", "type":"decimal", "precision":5, "scale":2},
                {"name":"big", "type":"decimal", "precision":20, "scale":-2},
                {"name":"fraction", "type":"decimal", "precision":3, "scale":6},
                {"name":"thousands", "type":"decimal", "precision":3, "scale":-3},
                {"name":"text", "type":"decimal", "precision":3, "scale":-2}
            ]
        }]);
        readback["output"] = json!([config["output"][0]]);
        invoke(dir.path(), &readback, "run", 4);
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), expected);
    }
}
