use datafusion::{
    arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::ScalarValue,
    prelude::SessionContext,
};
use std::sync::Arc;

async fn query(sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
    let context = SessionContext::new();
    dxtran::functions::register(&context);
    context.sql(sql).await?.collect().await
}

fn values(batches: &[RecordBatch]) -> Vec<ScalarValue> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                match ScalarValue::try_from_array(batch.column(0), row).unwrap() {
                    ScalarValue::Utf8View(s) => ScalarValue::Utf8(s),
                    other => other,
                }
            })
        })
        .collect()
}

async fn check(sql: &str, expected: &[ScalarValue]) {
    let batches = query(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(values(&batches), expected, "{sql}");
}

async fn bytes(sql: &str, expected: &str) {
    check(
        sql,
        &[ScalarValue::Binary(Some(hex::decode(expected).unwrap()))],
    )
    .await;
}

async fn text(sql: &str, expected: &str) {
    check(sql, &[ScalarValue::Utf8(Some(expected.into()))]).await;
}

#[tokio::test]
async fn lengths_and_slices_use_bytes_including_shift_controls() {
    for (value, encoding, expected) in [
        ("あ", "cp932", 2),
        ("あ", "utf-8", 3),
        ("日本", "utf-16le", 4),
        ("日本", "jef", 4),
        ("日本", "cp939", 6),
        ("日本", "cp930", 6),
    ] {
        check(
            &format!("SELECT icu_length('{value}', '{encoding}')"),
            &[ScalarValue::Int64(Some(expected))],
        )
        .await;
        check(
            &format!("SELECT icu_octet_length(icu_encode('{value}', '{encoding}'), '{encoding}')"),
            &[ScalarValue::Int64(Some(expected))],
        )
        .await;
        check(
            &format!("SELECT icu_bit_length('{value}', '{encoding}')"),
            &[ScalarValue::Int64(Some(expected * 8))],
        )
        .await;
    }
    for (expression, expected) in [
        ("icu_substr(X'82A082A2', 'cp932', 3, 2)", "82a2"),
        ("icu_substring('あい', 'cp932', 3)", "82a2"),
        ("icu_substr('あい', 'cp932', 2, 1)", "a0"),
        ("icu_substr('あい', 'cp932', 0, 3)", "82a0"),
        ("icu_substr('あい', 'cp932', -1, 4)", "82a0"),
        ("icu_substr('あい', 'cp932', 99)", ""),
        ("icu_substr('あい', 'cp932', 1, 0)", ""),
        (
            "icu_substr('あい', 'cp932', arrow_cast('18446744073709551615', 'UInt64'))",
            "",
        ),
        ("icu_left('あい', 'cp932', 2)", "82a0"),
        ("icu_right('あい', 'cp932', 2)", "82a2"),
        ("icu_left('あい', 'cp932', -2)", "82a0"),
        ("icu_right('あい', 'cp932', -2)", "82a2"),
        ("icu_left('あい', 'cp932', -99)", ""),
        ("icu_right('あい', 'cp932', 99)", "82a082a2"),
        ("icu_left('あい', 'cp932', 0)", ""),
        // Opaque Binary is not silently decoded by byte operations.
        ("icu_substr(X'FF0082', 'utf-8', 1, 2)", "ff00"),
    ] {
        bytes(&format!("SELECT {expression}"), expected).await;
    }
}

#[tokio::test]
async fn fullwidth_spaces_work_in_cp932_jef_and_utf16() {
    for (encoding, kanji, space) in [
        ("cp932", "93fa967b", "8140"),
        ("jef", "c6fccbdc", "4040"),
        ("utf-16le", "e5652c67", "0030"),
        ("utf-16be", "65e5672c", "3000"),
    ] {
        let input = format!("{space}{space}{kanji}{space}{space}");
        bytes(
            &format!("SELECT icu_trim(X'{input}', '{encoding}', '　')"),
            kanji,
        )
        .await;
        bytes(
            &format!("SELECT icu_btrim(X'{input}', '{encoding}', X'{space}')"),
            kanji,
        )
        .await;
        bytes(
            &format!("SELECT icu_ltrim(X'{input}', '{encoding}', '　')"),
            &format!("{kanji}{space}{space}"),
        )
        .await;
        bytes(
            &format!("SELECT icu_rtrim(X'{input}', '{encoding}', '　')"),
            &format!("{space}{space}{kanji}"),
        )
        .await;
        bytes(
            &format!("SELECT icu_lpad(X'{kanji}', '{encoding}', 8, '　')"),
            &format!("{space}{space}{kanji}"),
        )
        .await;
        bytes(
            &format!("SELECT icu_rpad(X'{kanji}', '{encoding}', 8, '　')"),
            &format!("{kanji}{space}{space}"),
        )
        .await;
        bytes(
            &format!("SELECT icu_trim(X'{space}{space}', '{encoding}', '　')"),
            "",
        )
        .await;
        assert!(
            query(&format!(
                "SELECT icu_rpad(X'{kanji}', '{encoding}', 7, '　')"
            ))
            .await
            .is_err()
        );
        // Only a complete encoded space is removed, not one of its bytes.
        bytes(
            &format!("SELECT icu_trim(X'FF99{space}', '{encoding}', '　')"),
            "ff99",
        )
        .await;
    }
    bytes("SELECT icu_trim(X'40C14040', 'cp939')", "c1").await;
    bytes("SELECT icu_lpad(X'C1', 'cp939', 4)", "404040c1").await;
    bytes("SELECT icu_rpad(X'C1', 'cp939', 4)", "c1404040").await;
    bytes("SELECT icu_trim('abXabab', 'utf-8', 'ab')", "58").await;
    bytes("SELECT icu_trim('baXab', 'utf-8', 'ab')", "626158").await;
}

#[tokio::test]
async fn stateful_double_byte_runs_trim_and_pad_without_stranded_shift_markers() {
    for encoding in ["cp930", "cp939", "iso-2022-jp"] {
        for (function, input, expected) in [
            ("icu_trim", "　　日本　　", "日本"),
            ("icu_ltrim", "　　日本　　", "日本　　"),
            ("icu_rtrim", "　　日本　　", "　　日本"),
            ("icu_trim", "　　　", ""),
            ("icu_trim", "日本", "日本"),
        ] {
            text(&format!("SELECT icu_decode({function}(icu_encode('{input}', '{encoding}'), '{encoding}', '　'), '{encoding}')"), expected).await;
        }
        let width = if encoding == "iso-2022-jp" { 12 } else { 8 };
        for (function, expected) in [("icu_lpad", "　日本"), ("icu_rpad", "日本　")] {
            let expression = format!(
                "{function}(icu_encode('日本', '{encoding}'), '{encoding}', {width}, '　')"
            );
            text(
                &format!("SELECT icu_decode({expression}, '{encoding}')"),
                expected,
            )
            .await;
            check(
                &format!("SELECT icu_length({expression}, '{encoding}')"),
                &[ScalarValue::Int64(Some(width))],
            )
            .await;
        }
        // Independent fullwidth-space encoding adds shift markers; appending
        // inside a DBCS run needs only two bytes, including in encode/decode.
        text(
            &format!(
                "SELECT icu_decode(icu_encode('日本', '{encoding}', {width}, '　'), '{encoding}')"
            ),
            "日本　",
        )
        .await;
        text(
            &format!(
                "SELECT icu_decode(icu_encode('日本', '{encoding}'), '{encoding}', {width}, '　')"
            ),
            "日本　",
        )
        .await;
        text(&format!("SELECT icu_decode(icu_rpad(icu_encode('', '{encoding}'), '{encoding}', {}, '　'), '{encoding}')", width - 2), "　　").await;
        assert!(
            query(&format!(
                "SELECT icu_rpad('日本', '{encoding}', {}, '　')",
                width - 1
            ))
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn case_search_replace_and_repeat_respect_encoding_and_byte_positions() {
    for (expression, expected) in [
        ("icu_upper(X'8182', 'cp939')", "c1c2"),
        ("icu_lower(X'C1C2', 'cp939')", "8182"),
        ("icu_upper('ß', 'utf-8')", "5353"),
        ("icu_replace('あいあ', 'cp932', 'あ', 'う')", "82a482a282a4"),
        (
            "icu_replace(X'FFFF00FF', 'utf-8', X'FF', X'41')",
            "41410041",
        ),
        ("icu_replace('aaa', 'utf-8', 'aa', 'b')", "6261"),
        ("icu_replace('abc', 'utf-8', '', 'x')", "616263"),
        ("icu_repeat('あ', 'cp932', 2)", "82a082a0"),
        ("icu_repeat('あ', 'cp932', -1)", ""),
        (
            "icu_repeat('', 'utf-8', arrow_cast('18446744073709551615', 'UInt64'))",
            "",
        ),
    ] {
        bytes(&format!("SELECT {expression}"), expected).await;
    }
    for (function, expected) in [("icu_strpos", 3), ("icu_instr", 3), ("icu_position", 3)] {
        check(
            &format!("SELECT {function}('あAい', 'cp932', 'A')"),
            &[ScalarValue::Int64(Some(expected))],
        )
        .await;
    }
    check(
        "SELECT icu_strpos('あAい', 'utf-8', 'A')",
        &[ScalarValue::Int64(Some(4))],
    )
    .await;
    check(
        "SELECT icu_strpos('abc', 'utf-8', 'x')",
        &[ScalarValue::Int64(Some(0))],
    )
    .await;
    check(
        "SELECT icu_strpos('abc', 'utf-8', '')",
        &[ScalarValue::Int64(Some(1))],
    )
    .await;
    for (expression, expected) in [
        ("icu_starts_with('あAい', 'cp932', 'あ')", true),
        ("icu_ends_with('あAい', 'cp932', 'い')", true),
        ("icu_contains('あAい', 'cp932', 'A')", true),
        ("icu_contains('あAい', 'cp932', 'う')", false),
        ("icu_contains(X'FF00', 'utf-8', X'FF')", true),
        ("icu_contains('', 'utf-8', '')", true),
    ] {
        check(
            &format!("SELECT {expression}"),
            &[ScalarValue::Boolean(Some(expected))],
        )
        .await;
    }
}

#[tokio::test]
async fn nulls_array_broadcasting_dynamic_encodings_and_storage_variants() {
    check("SELECT icu_length(value, encoding) FROM (VALUES ('あ', 'cp932'), ('あ', 'utf-8'), ('日本', 'jef'), (NULL, 'unknown')) AS data(value, encoding)",
        &[ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(3)), ScalarValue::Int64(Some(4)), ScalarValue::Int64(None)]).await;
    check("SELECT icu_rtrim(X'40C140', encoding, pattern) FROM (VALUES ('cp939', ' '), ('utf-8', '@'), ('cp939', NULL)) AS data(encoding, pattern)",
        &[ScalarValue::Binary(Some(vec![0x40, 0xc1])), ScalarValue::Binary(Some(vec![0x40, 0xc1])), ScalarValue::Binary(None)]).await;
    for (expression, data_type) in [
        ("icu_length(NULL, 'unknown')", DataType::Int64),
        ("icu_substr(X'41', 'utf-8', NULL)", DataType::Binary),
        ("icu_trim(NULL, 'utf-8')", DataType::Binary),
        ("icu_rpad(X'41', 'utf-8', 2, NULL)", DataType::Binary),
        ("icu_replace(X'41', NULL, 'A', 'B')", DataType::Binary),
        ("icu_contains(X'41', 'utf-8', NULL)", DataType::Boolean),
    ] {
        check(
            &format!("SELECT {expression}"),
            &[ScalarValue::try_from(&data_type).unwrap()],
        )
        .await;
    }
    assert!(
        values(
            &query(
                "SELECT icu_rtrim(value, 'utf-8') FROM (VALUES (X'41')) AS data(value) WHERE false"
            )
            .await
            .unwrap()
        )
        .is_empty()
    );
    use datafusion::arrow::array::{
        BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
        StringArray, StringViewArray,
    };
    let raw: Vec<Option<&[u8]>> = vec![Some(&[0x40, 0xc1, 0x40]), None];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(raw.clone())),
        Arc::new(LargeBinaryArray::from(raw.clone())),
        Arc::new(BinaryViewArray::from(raw.clone())),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(raw.into_iter(), 3).unwrap()),
        Arc::new(StringArray::from(vec![Some(" A "), None])),
        Arc::new(LargeStringArray::from(vec![Some(" A "), None])),
        Arc::new(StringViewArray::from(vec![Some(" A "), None])),
    ];
    for column in columns {
        let context = SessionContext::new();
        dxtran::functions::register(&context);
        context
            .register_batch(
                "data",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "value",
                        column.data_type().clone(),
                        true,
                    )])),
                    vec![column],
                )
                .unwrap(),
            )
            .unwrap();
        let batches = context
            .sql("SELECT icu_trim(value, 'cp939') FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            values(&batches),
            [
                ScalarValue::Binary(Some(vec![0xc1])),
                ScalarValue::Binary(None)
            ]
        );
    }
}

#[tokio::test]
async fn invalid_arguments_and_oversized_results_fail_without_truncating() {
    for sql in [
        "SELECT icu_trim(X'41')",
        "SELECT icu_length(X'41', 'utf-8', 3)",
        "SELECT icu_length(123, 'utf-8')",
        "SELECT icu_length(X'41', 123)",
        "SELECT icu_length(X'41', 'unknown-encoding')",
        "SELECT icu_substr(X'41', 'utf-8', '1')",
        "SELECT icu_substr(X'41', 'utf-8', 1, -1)",
        "SELECT icu_trim(X'41', 'utf-8', '')",
        "SELECT icu_rtrim(X'41', 'utf-8', 1)",
        "SELECT icu_lpad(X'41', 'utf-8', -1)",
        "SELECT icu_rpad(X'4142', 'utf-8', 1)",
        "SELECT icu_rpad(X'41', 'utf-8', 2, '')",
        "SELECT icu_lpad(X'41', 'utf-8', 16777217)",
        "SELECT icu_lpad('あ', 'cp932', 3, '　')",
        "SELECT icu_rpad('あ', 'cp932', 3, '　')",
        "SELECT icu_repeat('A', 'utf-8', 16777217)",
        "SELECT icu_replace(icu_repeat('A', 'utf-8', 8192), 'utf-8', 'A', repeat('B', 8192))",
        "SELECT icu_upper(X'FF', 'utf-8')",
        "SELECT icu_lower(X'82', 'cp932')",
        "SELECT icu_trim('日本', 'jef')",
        "SELECT icu_length('😀', 'cp939')",
        "SELECT icu_contains(X'41', 'utf-8', true)",
        "SELECT icu_replace(X'41', 'utf-8', X'41')",
        "SELECT icu_trim(X'0E45', 'cp939', '　')",
    ] {
        assert!(query(sql).await.is_err(), "{sql}");
    }
}

#[test]
fn cli_uses_functions_for_double_byte_fields_queries_checks_and_conditions() {
    use std::{fs, process::Command};
    let work = tempfile::tempdir().unwrap();
    fs::write(
        work.path().join("in.dat"),
        hex::decode("0030e5652c670030").unwrap(),
    )
    .unwrap();
    let job = serde_json::json!({
        "input":[{"id":"data", "format":"fixed", "path":"in.dat", "encoding":"utf-16le", "fixed":{"record_separator":"none"},
            "fields":[{"name":"label", "type":"bstring", "length":8, "trim":false, "space_char":"　"}],
            "check":{"when":"icu_length(label, 'utf-16le') <> 8", "level":"error", "message":"bad width"}}],
        "process":[{"id":"edited", "sql":"SELECT icu_rpad(icu_trim(label, 'utf-16le', '　'), 'utf-16le', 12, '　') AS label FROM data"}],
        "output":[{"id":"result", "format":"fixed", "path":"out.dat", "encoding":"utf-16le", "mode":"overwrite", "fixed":{"record_separator":"none"},
            "sql":"SELECT * FROM edited", "fields":[{"name":"label", "type":"bstring", "length":12, "trim":false, "space_char":"　", "when":"icu_length(label, 'utf-16le') = 12"}],
            "check":{"when":"NOT icu_starts_with(label, 'utf-16le', '日本')", "level":"error", "message":"bad prefix"}}]
    });
    let path = work.path().join("job.json");
    fs::write(&path, job.to_string()).unwrap();
    for command in ["check", "explain", "run"] {
        let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
        process.arg(command).arg(&path);
        if command == "check" {
            process.arg("--scan");
        }
        let result = process.output().unwrap();
        assert!(
            result.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    assert_eq!(
        fs::read(work.path().join("out.dat")).unwrap(),
        hex::decode("e5652c670030003000300030").unwrap()
    );
}
