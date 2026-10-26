use datafusion::common::ScalarValue;
use dxtran::{
    config::{Field, Format},
    encoding::TextEncoding,
    field::{CompiledField, FieldType},
};
use serde_json::json;

#[test]
fn decimal_codecs_preserve_sign_precision_and_zero_padding() {
    // Build expected bytes from decimal text, independently of the arithmetic encoder.
    for kind in [
        "zoned", "lzoned", "tzoned", "uzoned", "packed", "upacked", "npacked",
    ] {
        for precision in [1u8, 16, 19, 20, 38] {
            let signed = matches!(kind, "zoned" | "lzoned" | "tzoned" | "packed");
            let packed = matches!(kind, "packed" | "upacked" | "npacked");
            let length = if packed {
                usize::from(precision) / 2 + 2
            } else if kind == "uzoned" && precision == 19 {
                19 // Exercise the largest width of the u64 decoding path.
            } else {
                usize::from(precision) + 3
            };
            for ascii in [false, true] {
                if packed && ascii {
                    continue;
                }
                let mut spec =
                    json!({"name":"n", "type":kind, "length":length, "precision":precision});
                if !packed {
                    spec["zone"] = json!(if ascii { "ascii" } else { "ebcdic" });
                }
                let f = CompiledField::compile(
                    serde_json::from_value(spec).unwrap(),
                    Format::Fixed,
                    true,
                    0,
                    TextEncoding::resolve("utf-8").unwrap(),
                )
                .unwrap();
                let limit = 10i128.pow(u32::from(precision));
                let mut buffer = vec![0xa5; 256];
                for n in [limit - 1, 1, 0, -1, 1 - limit] {
                    let value = [ScalarValue::Decimal128(Some(n), precision, 0)];
                    if n < 0 && !signed {
                        assert!(f.encode_into(&value, Format::Fixed, &mut buffer).is_err());
                        continue;
                    }
                    let mut expected = if packed {
                        let digits = if kind == "npacked" {
                            2 * length
                        } else {
                            2 * length - 1
                        };
                        let mut text = format!("{:0digits$}", n.unsigned_abs());
                        if kind != "npacked" {
                            text.push(if kind == "upacked" {
                                'f'
                            } else if n < 0 {
                                'd'
                            } else {
                                'c'
                            });
                        }
                        hex::decode(text).unwrap()
                    } else {
                        let digits = if matches!(kind, "lzoned" | "tzoned") {
                            length - 1
                        } else {
                            length
                        };
                        let text = format!("{:0digits$}", n.unsigned_abs());
                        let mut bytes = text
                            .bytes()
                            .map(|b| if ascii { b } else { b + 0xc0 })
                            .collect::<Vec<_>>();
                        if kind == "zoned" {
                            *bytes.last_mut().unwrap() = (bytes[digits - 1] & 15)
                                | match (ascii, n < 0) {
                                    (true, false) => 0x30,
                                    (true, true) => 0x70,
                                    (false, false) => 0xc0,
                                    (false, true) => 0xd0,
                                };
                        }
                        let sign = match (ascii, n < 0) {
                            (true, false) => b'+',
                            (true, true) => b'-',
                            (false, false) => 0x4e,
                            (false, true) => 0x60,
                        };
                        if kind == "lzoned" {
                            bytes.insert(0, sign);
                        } else if kind == "tzoned" {
                            bytes.push(sign);
                        }
                        bytes
                    };
                    f.encode_into(&value, Format::Fixed, &mut buffer).unwrap();
                    assert_eq!(buffer, expected, "{kind} {precision} {n}");
                    assert_eq!(f.decode(&expected).unwrap(), value);
                    // Invalid digits must still be rejected after the allocation removal.
                    if packed {
                        expected[0] = 0xa0;
                    } else {
                        expected[usize::from(kind == "lzoned")] = if ascii { b'A' } else { 0xfa };
                    }
                    assert!(f.decode(&expected).is_err(), "{kind}");
                }
                for n in [limit, -limit, i128::MIN, i128::MAX] {
                    assert!(
                        f.encode(&[ScalarValue::Decimal128(Some(n), 38, 0)], Format::Fixed)
                            .is_err()
                    );
                }
            }
        }
    }
}

#[test]
fn packed_accepts_only_specified_signs() {
    for kind in ["packed", "upacked"] {
        let f = CompiledField::compile(
            serde_json::from_value(json!({"name":"n","type":kind,"length":2})).unwrap(),
            Format::Fixed,
            false,
            0,
            TextEncoding::resolve("utf-8").unwrap(),
        )
        .unwrap();
        for sign in 0..16 {
            let result = f.decode(&[0x12, 0x30 | sign]);
            if kind == "packed" && sign >= 10 || sign == 15 {
                let value = if matches!(sign, 11 | 13) { -123 } else { 123 };
                assert_eq!(
                    result.unwrap(),
                    vec![ScalarValue::Decimal128(Some(value), 3, 0)]
                );
            } else {
                assert!(result.is_err());
            }
        }
    }
}

#[test]
fn all_23_types_have_independent_expected_bytes_and_null_defaults() {
    let cases = [
        ("string", 4, "41422020", "20202020"),
        ("decimal", 6, "202020313233", "202020202030"),
        ("boolean", 5, "7472756520", "2020202020"),
        ("date", 10, "323032362d31302d3235", "20202020202020202020"),
        (
            "time",
            15,
            "31323a33343a35362e313233343536",
            "202020202020202020202020202020",
        ),
        (
            "timestamp",
            27,
            "323032362d31302d32355431323a33343a35362e3132333435365a",
            "202020202020202020202020202020202020202020202020202020",
        ),
        ("binary", 2, "00ff", "0000"),
        ("bits", 2, "8001", "0000"),
        ("int-le", 2, "feff", "0000"),
        ("int-be", 2, "fffe", "0000"),
        ("uint-le", 2, "0201", "0000"),
        ("uint-be", 2, "0102", "0000"),
        ("float-le", 4, "0000803f", "00000000"),
        ("float-be", 8, "3ff0000000000000", "0000000000000000"),
        ("hfloat-le", 8, "0000000000001041", "0000000000000000"),
        (
            "hfloat-be",
            16,
            "41100000000000003300000000000000",
            "00000000000000000000000000000000",
        ),
        ("zoned", 3, "f1f2c3", "f0f0c0"),
        ("lzoned", 4, "4ef1f2f3", "4ef0f0f0"),
        ("tzoned", 4, "f1f2f34e", "f0f0f04e"),
        ("uzoned", 3, "f1f2f3", "f0f0f0"),
        ("packed", 2, "123c", "000c"),
        ("upacked", 2, "123f", "000f"),
        ("npacked", 2, "0123", "0000"),
    ];
    for (kind, length, bytes, null) in cases {
        let mut raw = json!({"name":"value","type":kind,"length":length});
        if kind == "bits" {
            raw["bits"] = json!([{"name":"first","bit":0},{"name":"last","bit":15}]);
        }
        let field: Field = serde_json::from_value(raw).unwrap();
        let f = CompiledField::compile(
            field,
            Format::Fixed,
            true,
            0,
            TextEncoding::resolve("utf-8").unwrap(),
        )
        .unwrap();
        let bytes = hex::decode(bytes).unwrap();
        let values = f.decode(&bytes).unwrap();
        let expected = match f.kind {
            FieldType::String => vec![ScalarValue::Utf8(Some("AB".into()))],
            FieldType::Boolean => vec![ScalarValue::Boolean(Some(true))],
            FieldType::Binary => vec![ScalarValue::Binary(Some(vec![0, 255]))],
            FieldType::Bits => vec![ScalarValue::Boolean(Some(true)); 2],
            FieldType::IntLe | FieldType::IntBe => vec![ScalarValue::Int64(Some(-2))],
            FieldType::UintLe | FieldType::UintBe => vec![ScalarValue::UInt64(Some(258))],
            FieldType::FloatLe => vec![ScalarValue::Float32(Some(1.0))],
            FieldType::FloatBe | FieldType::HfloatLe | FieldType::HfloatBe => {
                vec![ScalarValue::Float64(Some(1.0))]
            }
            FieldType::Date => vec![ScalarValue::Date32(Some(20751))],
            FieldType::Time => vec![ScalarValue::Time64Microsecond(Some(45_296_123_456))],
            FieldType::Timestamp => vec![ScalarValue::TimestampMicrosecond(
                Some(1_792_931_696_123_456),
                Some("UTC".into()),
            )],
            _ => vec![ScalarValue::Decimal128(Some(123), f.precision, 0)],
        };
        assert_eq!(values, expected, "{kind}");
        assert_eq!(f.encode(&expected, Format::Fixed).unwrap(), bytes, "{kind}");
        assert_eq!(
            f.encode(&vec![f.null(); f.names().len()], Format::Fixed)
                .unwrap(),
            hex::decode(null).unwrap(),
            "{kind}"
        );
    }
}

#[test]
fn null_regex_is_full_match_and_precedes_decimal_conversion() {
    let field:Field=serde_json::from_value(json!({"name":"amount","type":"decimal","precision":6,"scale":2,"length":8,"nullable":true,"null_if":["N/A"],"null_if_pattern":"NULL[0-9]+"})).unwrap();
    let f = CompiledField::compile(
        field,
        Format::Fixed,
        false,
        0,
        TextEncoding::resolve("utf-8").unwrap(),
    )
    .unwrap();
    assert!(f.decode(b"N/A     ").unwrap()[0].is_null());
    assert!(f.decode(b"NULL123 ").unwrap()[0].is_null());
    assert!(f.decode(b"NULLIFY ").is_err());
    assert_eq!(
        f.decode(b"  123.45").unwrap()[0],
        ScalarValue::Decimal128(Some(12345), 6, 2)
    );
}
#[test]
fn large_bits_region_and_mixed_null_defaults() {
    for order in ["msb0", "lsb0"] {
        let field:Field=serde_json::from_value(json!({"name":"flags","type":"bits","length":10,"bit_order":order,"bits":[{"name":"a","bit":0},{"name":"b","bit":7},{"name":"c","bit":8},{"name":"d","bit":79}]})).unwrap();
        let f = CompiledField::compile(
            field,
            Format::Fixed,
            true,
            0,
            TextEncoding::resolve("utf-8").unwrap(),
        )
        .unwrap();
        let mut expected = vec![0; 10];
        expected[0] = if order == "msb0" { 0x80 } else { 1 };
        expected[1] = if order == "msb0" { 0x80 } else { 1 };
        expected[9] = if order == "msb0" { 1 } else { 0x80 };
        let row = [
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(None),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(true)),
        ];
        assert_eq!(f.encode(&row, Format::Fixed).unwrap(), expected);
        assert!(f.encode(&row, Format::Rdw).is_err());
    }
}

#[test]
fn multibyte_pad_cannot_split_characters() {
    for (enc, width, expected) in [
        ("utf-8", 7, "41e38080e38080"),
        ("shift_jis", 5, "4181408140"),
        ("utf-16le", 8, "4100003000300030"),
        ("utf-16be", 8, "0041300030003000"),
    ] {
        let field: Field =
            serde_json::from_value(json!({"name":"name","length":width,"pad_char":"　"})).unwrap();
        let f = CompiledField::compile(
            field,
            Format::Fixed,
            false,
            0,
            TextEncoding::resolve(enc).unwrap(),
        )
        .unwrap();
        let bytes = f.padded("A").unwrap();
        assert_eq!(bytes, hex::decode(expected).unwrap());
        assert_eq!(
            f.decode(&bytes).unwrap(),
            vec![ScalarValue::Utf8(Some("A".into()))]
        );
        assert!(
            f.padded(if enc.starts_with("utf-16") {
                "ABCDE"
            } else {
                "AA"
            })
            .is_err()
        );
    }
}
