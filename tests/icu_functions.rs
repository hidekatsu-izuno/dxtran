use datafusion::{
    arrow::{
        datatypes::{DataType, TimeUnit},
        record_batch::RecordBatch,
    },
    common::ScalarValue,
    prelude::SessionContext,
};

async fn query(sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
    let context = SessionContext::new();
    dxtran::functions::register(&context);
    context.sql(sql).await?.collect().await
}

async fn text(sql: &str) -> Vec<Option<String>> {
    query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(
                    |row| match ScalarValue::try_from_array(batch.column(0), row).unwrap() {
                        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) => value,
                        other => panic!("expected text, got {other:?}"),
                    },
                )
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn formats_with_icu_and_keeps_chrono_functions_available() {
    let result = query("SELECT icu_to_char(DATE '2026-10-28', 'yyyy年MM月dd日') AS unicode, to_char(DATE '2026-10-28', '%Y-%m-%d') AS chrono").await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(result[0].column(0), 0).unwrap(),
        ScalarValue::Utf8(Some("2026年10月28日".into()))
    );
    assert_eq!(
        ScalarValue::try_from_array(result[0].column(1), 0).unwrap(),
        ScalarValue::Utf8(Some("2026-10-28".into()))
    );
    assert_eq!(
        text("SELECT icu_to_char(DATE '2026-10-28', 'GGGGy年M月d日', 'ja-JP-u-ca-japanese')").await,
        [Some("令和8年10月28日".into())]
    );
    assert_eq!(
        text("SELECT icu_to_char(DATE '2026-10-28', 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese')").await,
        [Some("R08/10/28".into())]
    );
    assert_eq!(
        text("SELECT icu_to_char(DATE '2026-10-28', 'EEEE, MMMM d, yyyy', 'en-US')").await,
        [Some("Wednesday, October 28, 2026".into())]
    );
    assert_eq!(
        text("SELECT icu_to_char(DATE '2026-10-28', '''G'' G y', 'ja-JP-u-ca-japanese')").await,
        [Some("G 令和 8".into())]
    );
}

#[tokio::test]
async fn modern_era_boundaries_come_from_icu() {
    let sql = "SELECT icu_to_char(icu_to_date(value, 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese'), 'yyyy-MM-dd') FROM (VALUES ('M06/01/01'), ('M45/07/29'), ('T01/07/30'), ('T15/12/24'), ('S01/12/25'), ('S64/01/07'), ('H01/01/08'), ('H31/04/30'), ('R01/05/01')) AS dates(value)";
    let expected = [
        "1873-01-01",
        "1912-07-29",
        "1912-07-30",
        "1926-12-24",
        "1926-12-25",
        "1989-01-07",
        "1989-01-08",
        "2019-04-30",
        "2019-05-01",
    ];
    assert_eq!(text(sql).await, expected.map(|s| Some(s.into())));
    for value in [
        "M01/10/23",
        "M05/12/31",
        "T01/07/29",
        "S01/12/24",
        "H01/01/07",
        "H31/05/01",
        "R01/04/30",
        "R00/05/01",
        "R02/02/30",
    ] {
        assert!(
            query(&format!(
                "SELECT icu_to_date('{value}', 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese')"
            ))
            .await
            .is_err(),
            "{value}"
        );
    }
    assert_eq!(text("SELECT icu_to_char(icu_to_date('令和元年5月1日', 'GGGGy年M月d日', 'ja-JP-u-ca-japanese'), 'yyyy-MM-dd')").await, [Some("2019-05-01".into())]);
    assert_eq!(text("SELECT icu_to_char(icu_to_date('平31/04/30', 'Gyy/MM/dd', 'ja-JP-u-ca-japanese'), 'yyyy-MM-dd')").await, [Some("2019-04-30".into())]);
    assert_eq!(text("SELECT icu_to_char(icu_to_date('2019-05-01', 'uuuu-MM-dd', 'ja-JP-u-ca-japanese'), 'yyyy-MM-dd')").await, [Some("2019-05-01".into())]);
    assert!(
        query("SELECT icu_to_date('01/05/01', 'yy/MM/dd', 'ja-JP-u-ca-japanese')")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn parses_all_four_types_and_localized_periods() {
    let results = query("SELECT icu_to_date('2026/10/28', 'yyyy/MM/dd'), icu_to_time('午後01:02:03.123456', 'ahh:mm:ss.SSSSSS', 'ja-JP'), icu_to_timestamp('2026/10/28', 'yyyy/MM/dd')").await.unwrap();
    let schema = results[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Date32);
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(text("SELECT icu_to_char(icu_to_time('午後01:02:03.123456', 'ahh:mm:ss.SSSSSS', 'ja-JP'), 'HH:mm:ss.SSSSSS')").await, [Some("13:02:03.123456".into())]);
    assert_eq!(
        text("SELECT icu_to_char(icu_to_date('69-01-01', 'yy-MM-dd'), 'yyyy-MM-dd')").await,
        [Some("2069-01-01".into())]
    );
}

#[tokio::test]
async fn timestamps_use_explicit_zones_and_preserve_microseconds() {
    assert_eq!(text("SELECT icu_to_char(icu_to_timestamp('令和元年5月1日 00:00:00.123456', 'GGGGy年M月d日 HH:mm:ss.SSSSSS', 'ja-JP-u-ca-japanese', 'Asia/Tokyo'), 'yyyy-MM-dd HH:mm:ss.SSSSSS XXX')").await, [Some("2019-04-30 15:00:00.123456 Z".into())]);
    assert_eq!(text("SELECT icu_to_char(icu_to_timestamp('2019-04-30T15:00:00Z', 'yyyy-MM-dd''T''HH:mm:ssX'), 'GGGGy年M月d日 HH:mm:ss XXX', 'ja-JP-u-ca-japanese', 'Asia/Tokyo')").await, [Some("令和1年5月1日 00:00:00 +09:00".into())]);
    assert_eq!(text("SELECT icu_to_char(icu_to_timestamp('2019-05-01 00:00:00+09:00', 'yyyy-MM-dd HH:mm:ssXXX'), 'yyyy-MM-dd HH:mm:ss')").await, [Some("2019-04-30 15:00:00".into())]);
    assert_eq!(text("SELECT icu_to_char(icu_to_timestamp('2026-07-01 12:00', 'yyyy-MM-dd HH:mm', 'en-US', 'America/New_York'), 'HH:mm XXX z', 'en-US', 'America/New_York')").await, [Some("12:00 -04:00 EDT".into())]);
    assert_eq!(text("SELECT icu_to_char(icu_to_timestamp('2026-01-01 12:00', 'yyyy-MM-dd HH:mm', 'en-US', 'America/New_York'), 'HH:mm XXX z', 'en-US', 'America/New_York')").await, [Some("12:00 -05:00 EST".into())]);
    assert_eq!(
        text("SELECT icu_to_char(TIMESTAMP '2026-10-28 12:34:56.123456789', 'HH:mm:ss.SSSSSSSSS')")
            .await,
        [Some("12:34:56.123456789".into())]
    );
    for value in ["2026-03-08 02:30", "2026-11-01 01:30"] {
        assert!(query(&format!("SELECT icu_to_timestamp('{value}', 'yyyy-MM-dd HH:mm', 'en-US', 'America/New_York')")).await.is_err());
    }
}

#[tokio::test]
async fn scalar_array_broadcasting_nulls_and_empty_batches() {
    assert_eq!(text("SELECT icu_to_char(DATE '2026-10-28', pattern) FROM (VALUES ('yyyy-MM-dd'), ('dd/MM/yyyy'), (NULL)) AS formats(pattern)").await, [Some("2026-10-28".into()), Some("28/10/2026".into()), None]);
    assert_eq!(text("SELECT icu_to_char(icu_to_date(value, pattern, locale), 'yyyy-MM-dd') FROM (VALUES ('R08/10/28', 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese'), ('2026-10-28', 'yyyy-MM-dd', 'en-US'), (NULL, 'yyyy-MM-dd', 'en-US')) AS dates(value, pattern, locale)").await, [Some("2026-10-28".into()), Some("2026-10-28".into()), None]);
    let result = query("SELECT icu_to_char(NULL, 'yyyy'), icu_to_date(NULL, 'yyyy-MM-dd'), icu_to_timestamp('2026-10-28', NULL), icu_to_time('12:34', 'HH:mm', NULL)").await.unwrap();
    assert!(result[0].columns().iter().all(|c| c.is_null(0)));
    assert!(text("SELECT icu_to_char(value, 'yyyy-MM-dd') FROM (VALUES (DATE '2026-10-28')) AS dates(value) WHERE false").await.is_empty());
}

#[tokio::test]
async fn invalid_patterns_types_and_loss_of_precision_fail() {
    for sql in [
        "SELECT icu_to_date('2026-10-28')",
        "SELECT icu_to_time('12:34', 'HH:mm', 'en-US', 'UTC')",
        "SELECT icu_to_char('2026-10-28', 'yyyy-MM-dd')",
        "SELECT icu_to_date(20261028, 'yyyyMMdd')",
        "SELECT icu_to_date('2026-02-30', 'yyyy-MM-dd')",
        "SELECT icu_to_date('2026-10-28extra', 'yyyy-MM-dd')",
        "SELECT icu_to_date('2026-10-28', 'YYYY-MM-dd')",
        "SELECT icu_to_date('2026-10-28', 'yyyy-MM-dd GGGGGG')",
        "SELECT icu_to_char(DATE '2026-10-28', 'yyyy-MM-dd''')",
        "SELECT icu_to_char(icu_to_time('12:34', 'HH:mm'), 'yyyy')",
        "SELECT icu_to_time('24:00', 'HH:mm')",
        "SELECT icu_to_time('12:34:56.123456789', 'HH:mm:ss.SSSSSSSSS')",
        "SELECT icu_to_timestamp('2026-10-28 12:34:56.123456789', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS')",
        "SELECT icu_to_timestamp('2026-10-28', 'yyyy-MM-dd', 'en-US', 'unknown')",
        "SELECT icu_to_char(DATE '1800-01-01', 'GGGGy', 'ja-JP-u-ca-japanese')",
        "SELECT icu_to_date('1440-01-01', 'yyyy-MM-dd', 'ar-u-ca-islamic')",
        "SELECT icu_to_date('2026-10-28', 'yyyy-MM-dd', 'en-US-u-ca-unknown')",
    ] {
        assert!(query(sql).await.is_err(), "{sql}");
    }
}

#[test]
fn cli_registers_functions_for_queries_checks_and_layout_conditions() {
    use std::{fs, process::Command};
    let work = tempfile::tempdir().unwrap();
    fs::write(
        work.path().join("input.csv"),
        "wareki,clock\nR08/10/28,13:02:03\n",
    )
    .unwrap();
    let job = serde_json::json!({
        "input": [{"id": "dates", "path": "input.csv", "format": "csv", "encoding": "utf-8",
            "fields": [{"name": "wareki"}, {"name": "clock"}],
            "check": {"when": "icu_to_date(wareki, 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese') <> DATE '2026-10-28'", "level": "error", "message": "unexpected date"}}],
        "process": [{"id": "parsed", "sql": "SELECT icu_to_date(wareki, 'GGGGGyy/MM/dd', 'ja-JP-u-ca-japanese') AS day, icu_to_time(clock, 'HH:mm:ss') AS clock, icu_to_timestamp(wareki || ' ' || clock, 'GGGGGyy/MM/dd HH:mm:ss', 'ja-JP-u-ca-japanese') AS stamp FROM dates"}],
        "output": [
            {"id": "result", "sql": "SELECT *, icu_to_char(day, 'GGGGy年M月d日', 'ja-JP-u-ca-japanese') AS label FROM parsed", "path": "out.csv", "format": "csv", "encoding": "utf-8", "mode": "overwrite",
             "check": {"when": "icu_to_char(day, 'yyyy') <> '2026'", "level": "error", "message": "unexpected year"}},
            {"id": "fixed", "sql": "SELECT day FROM parsed", "path": "out.dat", "format": "fixed", "encoding": "utf-8", "mode": "overwrite", "fixed": {"record_separator": "none"},
             "fields": [{"name": "day", "type": "date", "length": 10, "when": "icu_to_char(day, 'yyyy') = '2026'"}]}
        ]
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
            fs::read_to_string(work.path().join("out.csv")).unwrap(),
            "day,clock,stamp,label\n2026-10-28,13:02:03.000000,2026-10-28T13:02:03.000000Z,令和8年10月28日\n"
        );
        assert_eq!(
            fs::read_to_string(work.path().join("out.dat")).unwrap(),
            "2026-10-28"
        );
    }
}
