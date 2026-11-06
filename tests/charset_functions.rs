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

#[tokio::test]
async fn known_encodings_and_byte_lengths_preserve_spaces() {
    for (text, encoding, hex) in [
        ("A", "cp939", "c1"),
        ("0", "cp939", "f0"),
        ("a", "cp939", "81"),
        ("A", "x-fujitsu-ebcdic-ascii", "c1"),
        ("あ", "cp932", "82a0"),
        ("あ", "utf-8", "e38182"),
        ("A", "utf-16le", "4100"),
        ("A", "utf-16be", "0041"),
    ] {
        check(
            &format!("SELECT icu_encode('{text}', '{encoding}')"),
            &[ScalarValue::Binary(Some(hex::decode(hex).unwrap()))],
        )
        .await;
        check(
            &format!("SELECT icu_decode(X'{hex}', '{encoding}')"),
            &[ScalarValue::Utf8(Some(text.into()))],
        )
        .await;
    }
    for (sql, expected) in [
        ("SELECT icu_encode('A', 'cp939', 4)", "c1404040"),
        ("SELECT icu_encode('あ', 'cp932', 4)", "82a02020"),
        (
            "SELECT icu_encode('A', 'utf-16le', 8, '　')",
            "4100003000300030",
        ),
        ("SELECT icu_encode('', 'utf-8', 0)", ""),
        ("SELECT icu_encode('A', 'utf-8', 4, '_')", "415f5f5f"),
    ] {
        check(
            sql,
            &[ScalarValue::Binary(Some(hex::decode(expected).unwrap()))],
        )
        .await;
    }
    for (sql, expected) in [
        ("SELECT icu_decode(X'C14040', 'cp939')", "A  "),
        ("SELECT icu_decode(X'C1', 'cp939', 4)", "A   "),
        ("SELECT icu_decode(X'82A0', 'cp932', 4)", "あ  "),
        ("SELECT icu_decode(X'4100', 'utf-16le', 8, '　')", "A　　　"),
        ("SELECT icu_decode(X'', 'utf-8', 0)", ""),
        ("SELECT icu_decode(X'41', 'utf-8', 4, '_')", "A___"),
    ] {
        check(sql, &[ScalarValue::Utf8(Some(expected.into()))]).await;
    }
    // The unused default U+0020 must not prevent JEF conversion without padding.
    check(
        "SELECT icu_decode(icu_encode('日本', 'jef'), 'jef')",
        &[ScalarValue::Utf8(Some("日本".into()))],
    )
    .await;
    // Stateful Japanese encodings include SO/SI in the specified byte length.
    check(
        "SELECT icu_decode(icu_encode('A日本B', 'cp939'), 'cp939')",
        &[ScalarValue::Utf8(Some("A日本B".into()))],
    )
    .await;
}

#[tokio::test]
async fn nulls_dynamic_arguments_and_empty_batches() {
    check("SELECT icu_decode(icu_encode(value, encoding, width, space), encoding, width, space) FROM (VALUES ('A', 'cp939', 3, ' '), ('あ', 'cp932', 4, ' '), ('A', 'utf-16le', 4, '　')) AS data(value, encoding, width, space)",
        &[ScalarValue::Utf8(Some("A  ".into())), ScalarValue::Utf8(Some("あ  ".into())), ScalarValue::Utf8(Some("A　".into()))]).await;
    for sql in [
        "SELECT icu_encode(NULL, 'utf-8')",
        "SELECT icu_encode('A', NULL)",
        "SELECT icu_encode('A', 'utf-8', NULL)",
        "SELECT icu_encode('A', 'utf-8', 3, NULL)",
        "SELECT icu_encode(NULL, 'unknown-encoding')",
    ] {
        check(sql, &[ScalarValue::Binary(None)]).await;
    }
    for sql in [
        "SELECT icu_decode(NULL, 'utf-8')",
        "SELECT icu_decode(X'41', NULL)",
        "SELECT icu_decode(X'41', 'utf-8', NULL)",
        "SELECT icu_decode(X'41', 'utf-8', 3, NULL)",
    ] {
        check(sql, &[ScalarValue::Utf8(None)]).await;
    }
    check("SELECT icu_decode(icu_encode(value, 'utf-8'), 'utf-8') FROM (VALUES ('A'), (NULL)) AS data(value)", &[ScalarValue::Utf8(Some("A".into())), ScalarValue::Utf8(None)]).await;
    assert!(
        values(
            &query(
                "SELECT icu_encode(value, 'utf-8') FROM (VALUES ('A')) AS data(value) WHERE false"
            )
            .await
            .unwrap()
        )
        .is_empty()
    );
    assert!(
        values(
            &query(
                "SELECT icu_decode(value, 'utf-8') FROM (VALUES (X'41')) AS data(value) WHERE false"
            )
            .await
            .unwrap()
        )
        .is_empty()
    );
}

#[tokio::test]
async fn arrow_string_and_binary_storage_variants_work() {
    use datafusion::arrow::array::{
        BinaryArray, BinaryViewArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
        StringArray, StringViewArray,
    };
    for column in [
        Arc::new(StringArray::from(vec![Some("あ"), None])) as ArrayRef,
        Arc::new(LargeStringArray::from(vec![Some("あ"), None])) as ArrayRef,
        Arc::new(StringViewArray::from(vec![Some("あ"), None])) as ArrayRef,
    ] {
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
            .sql("SELECT icu_encode(value, 'cp932') FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            values(&batches),
            [
                ScalarValue::Binary(Some(vec![0x82, 0xa0])),
                ScalarValue::Binary(None)
            ]
        );
    }
    let bytes: Vec<Option<&[u8]>> = vec![Some(&[0x82, 0xa0]), None];
    for column in [
        Arc::new(BinaryArray::from(bytes.clone())) as ArrayRef,
        Arc::new(LargeBinaryArray::from(bytes.clone())) as ArrayRef,
        Arc::new(BinaryViewArray::from(bytes.clone())) as ArrayRef,
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.into_iter(), 2).unwrap(),
        ) as ArrayRef,
    ] {
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
            .sql("SELECT icu_decode(value, 'cp932') FROM data")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            values(&batches),
            [
                ScalarValue::Utf8(Some("あ".into())),
                ScalarValue::Utf8(None)
            ]
        );
    }
}

#[tokio::test]
async fn invalid_types_encodings_and_inexact_widths_fail() {
    for sql in [
        "SELECT icu_encode('A')",
        "SELECT icu_decode(X'41')",
        "SELECT icu_encode('A', 'utf-8', 3, ' ', 5)",
        "SELECT icu_encode(X'41', 'utf-8')",
        "SELECT icu_decode('A', 'utf-8')",
        "SELECT icu_encode(123, 'utf-8')",
        "SELECT icu_decode(123, 'utf-8')",
        "SELECT icu_encode('A', 123)",
        "SELECT icu_encode('A', 'utf-8', '3')",
        "SELECT icu_encode('A', 'utf-8', 3.5)",
        "SELECT icu_encode('A', 'utf-8', -1)",
        "SELECT icu_encode('A', 'utf-8', 16777217)",
        "SELECT icu_encode('A', 'utf-8', 0)",
        "SELECT icu_decode(X'41', 'utf-8', 0)",
        "SELECT icu_encode('あ', 'cp932', 1)",
        "SELECT icu_decode(X'82A0', 'cp932', 1)",
        "SELECT icu_encode('A', 'utf-16le', 3)",
        "SELECT icu_decode(X'4100', 'utf-16le', 3)",
        "SELECT icu_encode('A', 'utf-8', 3, '')",
        "SELECT icu_encode('A', 'utf-8', 3, 'ab')",
        "SELECT icu_encode('A', 'utf-8', 3, 0)",
        "SELECT icu_encode('A', 'unknown-encoding')",
        "SELECT icu_decode(X'41', 'unknown-encoding')",
        "SELECT icu_encode('😀', 'cp939')",
        "SELECT icu_decode(X'FF', 'utf-8')",
        "SELECT icu_decode(X'82', 'cp932')",
        "SELECT icu_decode(X'0E45', 'cp939')",
    ] {
        assert!(query(sql).await.is_err(), "{sql}");
    }
}

#[tokio::test]
async fn binary_comparisons_follow_datafusion_coercion_without_transcoding() {
    check("SELECT X'41' = 'A'", &[ScalarValue::Boolean(Some(true))]).await;
    check("SELECT X'C1' = 'A'", &[ScalarValue::Boolean(Some(false))]).await;
    check(
        "SELECT icu_encode('A', 'cp939') = 'A'",
        &[ScalarValue::Boolean(Some(false))],
    )
    .await;
    check(
        "SELECT icu_decode(X'C1', 'cp939') = 'A'",
        &[ScalarValue::Boolean(Some(true))],
    )
    .await;
    let batches = query("SELECT icu_encode('A', 'cp939') || icu_encode('B', 'cp939') AS joined")
        .await
        .unwrap();
    assert!(matches!(
        batches[0].schema().field(0).data_type(),
        DataType::Binary | DataType::BinaryView | DataType::LargeBinary
    ));
    match &values(&batches)[0] {
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => assert_eq!(b, &[0xc1, 0xc2]),
        other => panic!("expected binary concatenation, got {other:?}"),
    }
}
