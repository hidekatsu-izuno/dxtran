use datafusion::{
    arrow::{array::ArrayRef, datatypes::DataType, record_batch::RecordBatch},
    common::ScalarValue,
    prelude::SessionContext,
};

async fn query(sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
    let context = SessionContext::new();
    dxtran::functions::register(&context);
    context.sql(sql).await?.collect().await
}

fn texts(batches: &[RecordBatch]) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                match ScalarValue::try_from_array(batch.column(0), row).unwrap() {
                    ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) => s,
                    other => panic!("expected text, got {other:?}"),
                }
            })
        })
        .collect()
}

async fn text(sql: &str) -> Vec<Option<String>> {
    texts(&query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")))
}

#[tokio::test]
async fn cobol_reference_numeric_editing_examples() {
    // Independent expected results from IBM's PICTURE editing examples:
    // https://www.ibm.com/docs/en/cobol-zos/6.3?topic=clause-zero-suppression-replacement-editing
    // https://www.ibm.com/docs/en/cobol-zos/6.3.0?topic=clause-fixed-insertion-editing
    // https://www.ibm.com/docs/en/cobol-zos/6.3.0?topic=clause-floating-insertion-editing
    for (value, picture, expected) in [
        ("1234", "99,B999,B000", "01, 234, 000"),
        ("12345", "99,999", "12,345"),
        ("1.234", "999.99", "001.23"),
        ("12.34", "999.99", "012.34"),
        ("123.45", "999.99", "123.45"),
        ("1234.5", "999.99", "234.50"),
        ("6555.556", "999.99+", "555.55+"),
        ("-6555.555", "9999.99+", "6555.55-"),
        ("1234.56", "9999.99", "1234.56"),
        ("-123.45", "$999.99", "$123.45"),
        ("-123.456", "-$999.99", "-$123.45"),
        ("123.456", "-$999.99", " $123.45"),
        ("123.45", "$9999.99CR", "$0123.45  "),
        ("-123.45", "$9999.99CR", "$0123.45CR"),
        ("0.123", "$$$$.99", "   $.12"),
        ("0.12", "$$$9.99", "  $0.12"),
        ("-1234.56", "$,$$$,999.99", "   $1,234.56"),
        ("-123456.789", "+,+++,999.99", " -123,456.78"),
        ("-1234567", "$$,$$$,$$$.99CR", "$1,234,567.00CR"),
        ("0", "++,+++,+++.+++", "              "),
        ("0", "****.**", "****.**"),
        ("0", "ZZZZ.ZZ", "       "),
        ("0", "ZZZZ.99", "    .00"),
        ("0", "****.99", "****.00"),
        ("0", "ZZ99.99", "  00.00"),
        ("123.456", "Z,ZZZ.ZZ+", "  123.45+"),
        ("-123.45", "*,***.**+", "**123.45-"),
        ("12345678.9", "**,***,***.**+", "12,345,678.90+"),
        ("12345.67", "$Z,ZZZ,ZZZ.ZZCR", "$   12,345.67  "),
        ("-12345.67", "$B*,***,***.**BBDB", "$ ***12,345.67  DB"),
    ] {
        let sql = format!("SELECT cobol_to_char(CAST('{value}' AS DECIMAL(20, 3)), '{picture}')");
        assert_eq!(text(&sql).await, [Some(expected.into())], "{sql}");
    }
}

#[tokio::test]
async fn repetition_implied_decimals_signs_and_simple_insertion() {
    for (value, picture, expected) in [
        ("1234.56", "Z(5)9.9(2)", "  1234.56"),
        ("123.45", "9(5)V99", "0012345"),
        ("123.45", "9(5)v99", "0012345"),
        ("123", "9(3)V", "123"),
        ("0.12", "V99", "12"),
        ("0.12", ".99", ".12"),
        ("123", "+++9", "+123"),
        ("12", "+++9", " +12"),
        ("12", "---9", "  12"),
        ("-12", "---9", " -12"),
        ("0", "$$$9.99", "  $0.00"),
        ("123.45", "$$$$9.99", " $123.45"),
        ("12", "++++", " +12"),
        ("0", "++++", "    "),
        ("0.12", "+++.++", "  +.12"),
        ("123.45", "+999.99", "+123.45"),
        ("123.45", "999.99-", "123.45 "),
        ("-123.45", "999.99-", "123.45-"),
        ("123.45", "999.99DB", "123.45  "),
        ("-123.45", "999.99db", "123.45DB"),
        ("0.12", "zzzz.zz", "    .12"),
        ("0.12", "****.**", "****.12"),
        ("123456", "9900990099", "1200340056"),
        ("20261028", "9999/99/99", "2026/10/28"),
        ("123", "9B99", "1 23"),
    ] {
        let sql = format!("SELECT cobol_to_char(CAST('{value}' AS DECIMAL(20, 3)), '{picture}')");
        assert_eq!(text(&sql).await, [Some(expected.into())], "{sql}");
    }
}

#[tokio::test]
async fn decimals_preserve_precision_and_truncate_towards_zero() {
    for (value, picture, expected) in [
        (
            "99999999999999999999999999999999999999",
            "9(38)",
            "99999999999999999999999999999999999999",
        ),
        (
            "12345678901234567890.123456789012345678",
            "9(20).9(18)",
            "12345678901234567890.123456789012345678",
        ),
        ("-1.239", "+9.99", "-1.23"),
        ("-1.235", "+9.99", "-1.23"),
        ("0.000000000000000001", "9.9(18)", "0.000000000000000001"),
        ("0.999999999999999999", "9.99", "0.99"),
    ] {
        let scale = value.split_once('.').map_or(0, |(_, s)| s.len());
        let sql =
            format!("SELECT cobol_to_char(CAST('{value}' AS DECIMAL(38, {scale})), '{picture}')");
        assert_eq!(text(&sql).await, [Some(expected.into())], "{sql}");
    }
}

#[tokio::test]
async fn numeric_arrow_types_and_negative_scales() {
    use datafusion::arrow::datatypes::{Field, Schema, i256};
    use std::sync::Arc;

    let cases = [
        (ScalarValue::Int8(Some(-128)), "-128"),
        (ScalarValue::Int16(Some(-32768)), "-32768"),
        (ScalarValue::Int32(Some(i32::MIN)), "-2147483648"),
        (ScalarValue::Int64(Some(i64::MIN)), "-9223372036854775808"),
        (ScalarValue::UInt8(Some(255)), "+255"),
        (ScalarValue::UInt16(Some(65535)), "+65535"),
        (ScalarValue::UInt32(Some(u32::MAX)), "+4294967295"),
        (ScalarValue::UInt64(Some(u64::MAX)), "+18446744073709551615"),
        (ScalarValue::Decimal32(Some(123), 5, 0), "+123"),
        (ScalarValue::Decimal64(Some(123), 5, -2), "+12300"),
        (ScalarValue::Decimal128(Some(123), 5, -2), "+12300"),
        (
            ScalarValue::Decimal256(Some(i256::from(123)), 5, -2),
            "+12300",
        ),
        (ScalarValue::Float32(Some(123.99)), "+123"),
        (ScalarValue::Float64(Some(-123.99)), "-123"),
    ];
    for (value, expected) in cases {
        let context = SessionContext::new();
        dxtran::functions::register(&context);
        let column: ArrayRef = value.to_array_of_size(2).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                value.data_type(),
                false,
            )])),
            vec![column],
        )
        .unwrap();
        context.register_batch("numbers", batch).unwrap();
        let sql = "SELECT cobol_to_char(value, '+9(20)') FROM numbers";
        let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
        let sign = &expected[..1];
        let magnitude = &expected[1..];
        let padded = format!("{sign}{magnitude:0>20}");
        assert_eq!(texts(&batches), [Some(padded.clone()), Some(padded)]);
    }
    let large = "1234567890123456789012345678901234567890123456789012345678901234567890";
    let context = SessionContext::new();
    dxtran::functions::register(&context);
    let value = ScalarValue::Decimal256(Some(large.parse::<i256>().unwrap()), 76, 6);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            value.data_type(),
            false,
        )])),
        vec![value.to_array_of_size(1).unwrap()],
    )
    .unwrap();
    context.register_batch("numbers", batch).unwrap();
    let batches = context
        .sql("SELECT cobol_to_char(value, '9(64).9(6)') FROM numbers")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        texts(&batches),
        [Some(format!("{}.{}", &large[..64], &large[64..]))]
    );
    assert_eq!(
        text("SELECT cobol_to_char(1.239, '9.99')").await,
        [Some("1.23".into())]
    );
    assert_eq!(
        text("SELECT cobol_to_char(1.0e-20, '9.9(20)')").await,
        [Some("0.00000000000000000001".into())]
    );
    assert_eq!(
        text("SELECT cobol_to_char(-0.0, '+9.99')").await,
        [Some("+0.00".into())]
    );
}

#[tokio::test]
async fn string_storage_variants_and_many_dynamic_pictures() {
    use datafusion::arrow::{
        array::{Int64Array, LargeStringArray, StringArray, StringViewArray},
        datatypes::{Field, Schema},
    };
    use std::sync::Arc;

    // Exceed the cache bound, then reuse the first format. The formatting
    // result must not depend on cache eviction or Arrow's string storage.
    let formats: Vec<String> = (1..=140).chain([1]).map(|n| format!("9({n})")).collect();
    let strings: Vec<&str> = formats.iter().map(String::as_str).collect();
    for pictures in [
        Arc::new(StringArray::from(strings.clone())) as ArrayRef,
        Arc::new(LargeStringArray::from(strings.clone())) as ArrayRef,
        Arc::new(StringViewArray::from(strings.clone())) as ArrayRef,
    ] {
        let context = SessionContext::new();
        dxtran::functions::register(&context);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new("picture", pictures.data_type().clone(), false),
            ])),
            vec![Arc::new(Int64Array::from(vec![1; strings.len()])), pictures],
        )
        .unwrap();
        context.register_batch("numbers", batch).unwrap();
        let batches = context
            .sql("SELECT cobol_to_char(value, picture) FROM numbers")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected: Vec<_> = (1..=140)
            .chain([1])
            .map(|n| Some(format!("{:0>n$}", 1)))
            .collect();
        assert_eq!(texts(&batches), expected);
    }
    assert_eq!(
        text("SELECT cobol_to_char(1, '9(4096)')").await,
        [Some(format!("{:0>4096}", 1))]
    );
}

#[tokio::test]
async fn scalar_broadcasting_dynamic_formats_nulls_and_empty_batches() {
    assert_eq!(
        text("SELECT cobol_to_char(value, picture) FROM (VALUES (123, 'ZZZ9'), (-123, '999-'), (NULL, '999'), (123, NULL)) AS numbers(value, picture)").await,
        [Some(" 123".into()), Some("123-".into()), None, None]
    );
    assert_eq!(
        text("SELECT cobol_to_char(123, picture) FROM (VALUES ('ZZZ9'), ('999+'), (NULL)) AS pictures(picture)").await,
        [Some(" 123".into()), Some("123+".into()), None]
    );
    assert_eq!(text("SELECT cobol_to_char(NULL, '999')").await, [None]);
    assert_eq!(text("SELECT cobol_to_char(123, NULL)").await, [None]);
    assert_eq!(text("SELECT cobol_to_char(NULL, NULL)").await, [None]);
    assert!(
        text(
            "SELECT cobol_to_char(value, '999') FROM (VALUES (123)) AS numbers(value) WHERE false"
        )
        .await
        .is_empty()
    );
    // Both ordinary string encodings and Arrow's string view must work.
    assert_eq!(
        query("SELECT cobol_to_char(1, '999') AS result")
            .await
            .unwrap()[0]
            .schema()
            .field(0)
            .data_type(),
        &DataType::Utf8
    );
}

#[tokio::test]
async fn invalid_arguments_and_pictures_fail() {
    for sql in [
        "SELECT cobol_to_char(1)",
        "SELECT cobol_to_char(1, '9', 'extra')",
        "SELECT cobol_to_char('123', '999')",
        "SELECT cobol_to_char(true, '9')",
        "SELECT cobol_to_char(DATE '2026-10-28', '9999')",
        "SELECT cobol_to_char(1, 999)",
        "SELECT cobol_to_char(CAST('NaN' AS DOUBLE), '999')",
        "SELECT cobol_to_char(CAST('Infinity' AS DOUBLE), '999')",
        "SELECT cobol_to_char(CAST('-Infinity' AS DOUBLE), '999')",
    ] {
        assert!(query(sql).await.is_err(), "{sql}");
    }
    for picture in [
        "",
        " ",
        "PIC 999",
        "9.9.9",
        "9V9.9",
        "99VV9",
        "9(0)",
        "9(-1)",
        "9()",
        "9(2",
        "9(2))",
        "9(1.5)",
        "9(999999999999999999999999)",
        "9(4097)",
        "9(4096)9",
        "CR",
        "DB",
        "$",
        "BBB",
        "000",
        ".",
        "V",
        "Z*9",
        "9Z",
        "9*",
        "ZZ9.ZZ",
        "ZZ.Z9",
        "**.*9",
        "++ZZ9",
        "$$***",
        "++$$9",
        "+99-",
        "+99CR",
        "99CRDB",
        "CR99",
        "99DB9",
        "9+9",
        "9$",
        "99$99",
        "9$$",
        "$$9$$",
        "V$$",
        "$$.99$$",
        ".(1)99",
        "V(1)99",
        "9CR(1)",
        "S999",
        "PPP99",
        "9E99",
        "9年",
        "９９９",
    ] {
        let sql = format!("SELECT cobol_to_char(123, '{picture}')");
        let error = query(&sql).await.expect_err(&sql).to_string();
        assert!(error.contains("PICTURE"), "{sql}: {error}");
    }
}

#[test]
fn cli_registers_cobol_formatting_in_queries_checks_and_conditions() {
    use std::{fs, process::Command};
    let work = tempfile::tempdir().unwrap();
    fs::write(work.path().join("input.csv"), "amount\n1234.56\n-12.34\n").unwrap();
    let job = serde_json::json!({
        "input": [{"id": "numbers", "path": "input.csv", "format": "csv", "encoding": "utf-8",
            "fields": [{"name": "amount", "type": "decimal", "precision": 10, "scale": 2}],
            "check": {"when": "cobol_to_char(amount, '9999.99') = ''", "level": "error", "message": "empty number"}}],
        "process": [{"id": "edited", "sql": "SELECT amount, cobol_to_char(amount, 'ZZZ,ZZ9.99CR') AS label FROM numbers"}],
        "output": [{"id": "result", "path": "out.dat", "format": "fixed", "encoding": "utf-8", "mode": "overwrite",
            "fixed": {"record_separator": "lf"},
            "sql": "SELECT label FROM edited",
            "check": {"when": "cobol_to_char(1, '9') <> '1'", "level": "error", "message": "bad format"},
            "fields": [{"name": "label", "length": 12, "trim": false, "when": "cobol_to_char(1, '9') = '1'"}]}]
    });
    let path = work.path().join("job.json");
    fs::write(&path, job.to_string()).unwrap();
    for threads in [1, 4] {
        for command in ["check", "explain", "run"] {
            let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
            process
                .arg(command)
                .arg(&path)
                .args(["--threads", &threads.to_string()]);
            if command == "check" {
                process.arg("--scan");
            }
            let output = process.output().unwrap();
            assert!(
                output.status.success(),
                "{command}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert_eq!(
            fs::read_to_string(work.path().join("out.dat")).unwrap(),
            "  1,234.56  \n     12.34CR\n"
        );
    }
}
