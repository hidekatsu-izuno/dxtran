use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn job() -> Value {
    json!({"input":[{"id":"data","format":"csv","path":"in.csv","encoding":"utf-8",
        "fields":[{"name":"value"}]}],
        "output":[{"id":"result","format":"csv","path":"out.csv","encoding":"utf-8",
            "mode":"overwrite","sql":"SELECT * FROM data"}]})
}
fn invoke(root: &Path, job: &Value, command: &str, args: &[&str]) -> Output {
    fs::write(root.join("job.json"), job.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg(command)
        .arg(root.join("job.json"))
        .args(args)
        .output()
        .unwrap()
}
fn success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn cli_values_are_strings_and_defaults_are_typed_sql_values() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\nb\n").unwrap();
    let mut config = job();
    config["params"] = json!({"X":{},"day":{"default":"DATE '2026-11-06'"},
        "limit":{"default":"1 + 1"},"empty":{"default":"NULL"},"enabled":{"default":true}});
    config["input"][0]["after"] =
        json!({"when":"COUNT(*) <> $limit", "level":"error","message":"count"});
    config["process"] = json!([{"id":"p","sql":"SELECT * FROM data LIMIT $limit",
        "before":{"when":"COUNT(*) <> $limit","level":"error","message":"before"},
        "after":{"when":"COUNT(*) <> $limit","level":"error","message":"after"}}]);
    config["output"][0]["sql"] = json!(
        "SELECT value, $X AS supplied, $day AS day, CAST($empty AS VARCHAR) AS empty FROM p WHERE $enabled"
    );
    config["output"][0]["check"] =
        json!({"when":"supplied <> $X","level":"error","message":"escaped"});
    config["output"][0]["before"] =
        json!({"when":"COUNT(*) <> $limit","level":"error","message":"output"});
    let value = "X=O'Brien=$other; SELECT 'bad'";
    let output = invoke(dir.path(), &config, "run", &["--param", value]);
    success(&output);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "value,supplied,day,empty\na,O'Brien=$other; SELECT 'bad',2026-11-06,\nb,O'Brien=$other; SELECT 'bad',2026-11-06,\n"
    );
    let output = invoke(
        dir.path(),
        &config,
        "run",
        &["-p", "X=", "-p", "day=2026-11-07"],
    );
    success(&output);
    assert!(
        fs::read_to_string(dir.path().join("out.csv"))
            .unwrap()
            .contains("a,\"\",2026-11-07,")
    );
    for command in ["check", "explain"] {
        success(&invoke(dir.path(), &config, command, &["-p", "X=hello"]));
    }
}

#[test]
fn missing_undeclared_duplicate_and_malformed_parameters_fail_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\n").unwrap();
    fs::write(dir.path().join("out.csv"), "old").unwrap();
    let mut config = job();
    config["params"] = json!({"X":{}});
    for (args, message) in [
        (vec![], "required parameter 'X'"),
        (vec!["-p", "Y=1"], "undeclared parameter 'Y'"),
        (vec!["-p", "x=1"], "undeclared parameter 'x'"),
        (
            vec!["-p", "X=1", "--param", "X=2"],
            "duplicate parameter 'X'",
        ),
        (vec!["-p", "X"], "NAME=VALUE"),
        (vec!["-p", "=value"], "undeclared parameter"),
    ] {
        for command in ["check", "explain", "run"] {
            let output = invoke(dir.path(), &config, command, &args);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stderr).contains(message));
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "old"
            );
        }
    }
    config["params"] = json!({});
    config["output"][0]["sql"] = json!("SELECT $missing AS value FROM data");
    assert_eq!(
        invoke(dir.path(), &config, "check", &[]).status.code(),
        Some(2)
    );
}

#[test]
fn defaults_must_be_scalar_expressions_and_are_evaluated_once() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\nb\n").unwrap();
    let mut config = job();
    for default in [
        "1; SELECT 2",
        "(SELECT 1)",
        "SUM(1)",
        "ROW_NUMBER() OVER ()",
        "$other",
        "value",
        "CAST('bad' AS INT)",
    ] {
        config["params"] = json!({"test":{"default":default}});
        let output = invoke(dir.path(), &config, "check", &[]);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{default}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("parameter test"));
    }
    config["params"] = json!({"clock":{"default":"CURRENT_TIMESTAMP"},"number":{"default":42}});
    config["output"][0]["sql"] =
        json!("SELECT $clock AS first, $clock AS second, $number AS n FROM data");
    success(&invoke(dir.path(), &config, "run", &[]));
    let output = fs::read_to_string(dir.path().join("out.csv")).unwrap();
    let lines: Vec<_> = output.lines().skip(1).collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], lines[1]);
    let cells: Vec<_> = lines[0].split(',').collect();
    assert_eq!(cells[0], cells[1]);
    assert_eq!(cells[2], "42");

    config["params"] = json!({"amount":{"default":"CAST(1 AS DECIMAL(12,2))"},
        "missing":{"default":"CAST(NULL AS INT)"},"nan":{"default":"CAST('NaN' AS DOUBLE)"}});
    config["output"][0]["sql"] = json!(
        "SELECT $amount AS amount, arrow_typeof($amount) AS amount_type, arrow_typeof($missing) AS missing_type, arrow_typeof($nan) AS nan_type FROM data"
    );
    success(&invoke(dir.path(), &config, "run", &[]));
    let output = fs::read_to_string(dir.path().join("out.csv")).unwrap();
    assert!(
        output.contains("1.00,\"Decimal128(12, 2)\",Int32,Float64"),
        "{output}"
    );
}

#[test]
fn imported_declarations_and_library_planning_resolve_defaults() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\n").unwrap();
    fs::write(
        dir.path().join("params.yml"),
        "X: {}\nmessage:\n  default: \"'hello'\"\n",
    )
    .unwrap();
    let mut config = job();
    config["params"] = json!({"import":"params.yml"});
    config["output"][0]["sql"] = json!("SELECT $X AS value FROM data");
    let output = invoke(dir.path(), &config, "check", &[]);
    assert!(String::from_utf8_lossy(&output.stderr).contains("params.yml:1:"));
    success(&invoke(dir.path(), &config, "run", &["--param=X=value"]));
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let job = dxtran::config::load(&dir.path().join("job.json")).unwrap();
        let ctx = datafusion::prelude::SessionContext::new();
        assert!(
            dxtran::planner::plan(&ctx, &job)
                .await
                .err()
                .unwrap()
                .message
                .contains("required parameter")
        );
    });
}

#[test]
fn parameters_are_available_in_input_and_output_record_conditions() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), "HqDrDs").unwrap();
    let mut config = job();
    config["params"] = json!({"active":{}});
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"header","offset":1,"length":1,"when":"kind = 'H'"},
        {"name":"detail","offset":1,"length":1,"when":"kind = $active"}
    ]);
    config["input"][0] = json!({"id":"data","format":"fixed","path":"in.dat","encoding":"utf-8",
        "fixed":{"record_separator":"none"},"fields":fields,
        "check":{"when":"kind = $active AND detail IS NULL","level":"error","message":"missing"}});
    config["output"][0]["sql"] = json!("SELECT detail FROM data WHERE kind = $active");
    let fixed = json!({"id":"fixed","format":"fixed","path":"out.dat","mode":"overwrite",
        "encoding":"utf-8","fixed":{"record_separator":"none"},"fields":fields,"sql":"SELECT * FROM data"});
    config["output"].as_array_mut().unwrap().push(fixed);
    success(&invoke(dir.path(), &config, "run", &["-p", "active=D"]));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "detail\nr\ns\n"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("out.dat")).unwrap(),
        "HqDrDs"
    );
}

#[test]
fn declared_types_convert_cli_strings_before_sql_and_checks() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\nb\n").unwrap();
    let mut config = job();
    config["params"] = json!({
        "label":{"type":"string"},
        "amount":{"type":"decimal"},
        "day":{"type":"date"},"clock":{"type":"time"},
        "stamp":{"type":"timestamp"},"enabled":{"type":"boolean"}
    });
    config["input"][0]["check"] =
        json!({"when":"NOT $enabled OR $amount <= 0", "level":"error","message":"parameter types"});
    config["input"][0]["after"] =
        json!({"when":"$day <> DATE '2026-11-07'", "level":"error","message":"day"});
    config["output"][0]["sql"] = json!(
        "SELECT $label AS label, $amount AS amount, $day AS day, $clock AS clock, $stamp AS stamp, $enabled AS enabled FROM data WHERE $enabled AND $amount > 0"
    );
    config["output"][0]["before"] =
        json!({"when":"MIN(amount) <> $amount", "level":"error","message":"precision"});
    let args = [
        "-p",
        "label=O'Brien=ok",
        "-p",
        "amount=9007199254740993.01",
        "-p",
        "day=2026-11-07",
        "-p",
        "clock=12:34:56.123456",
        "-p",
        "stamp=2026-11-07T21:34:56.123456+09:00",
        "-p",
        "enabled=TrUe",
    ];
    for command in ["check", "explain", "run"] {
        success(&invoke(dir.path(), &config, command, &args));
    }
    let line = "O'Brien=ok,9007199254740993.01,2026-11-07,12:34:56.123456,2026-11-07T12:34:56.123456Z,true\n";
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        format!("label,amount,day,clock,stamp,enabled\n{line}{line}")
    );

    // FALSE is a Boolean value, rather than a nonempty string used as a condition.
    config["input"][0].as_object_mut().unwrap().remove("check");
    let mut args = args;
    *args.last_mut().unwrap() = "enabled=false";
    success(&invoke(dir.path(), &config, "run", &args));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "label,amount,day,clock,stamp,enabled\n"
    );
}

#[test]
fn declared_types_convert_sql_defaults_and_keep_typed_nulls() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\n").unwrap();
    let mut config = job();
    config["params"] = json!({
        "label":{"type":"string","default":42},
        "amount":{"type":"decimal","default":"1 + 2"},
        "day":{"type":"date","default":"'2026-11-07'"},
        "clock":{"type":"time","default":"TIME '12:34:56'"},
        "stamp":{"type":"timestamp","default":"TIMESTAMP '2026-11-07 12:34:56'"},
        "enabled":{"type":"boolean","default":true}
    });
    config["output"][0]["sql"] = json!(
        "SELECT $label AS label, $amount AS amount, $day AS day, $clock AS clock, $stamp AS stamp, $enabled AS enabled FROM data WHERE $enabled"
    );
    success(&invoke(dir.path(), &config, "run", &[]));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "label,amount,day,clock,stamp,enabled\n42,3,2026-11-07,12:34:56.000000,2026-11-07T12:34:56.000000Z,true\n"
    );

    for param in config["params"].as_object_mut().unwrap().values_mut() {
        param["default"] = json!("NULL");
    }
    config["output"][0]["sql"] = json!(
        "SELECT $label AS label, $amount AS amount, $day AS day, $clock AS clock, $stamp AS stamp, $enabled AS enabled FROM data"
    );
    success(&invoke(dir.path(), &config, "run", &[]));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "label,amount,day,clock,stamp,enabled\n,,,,,\n"
    );
    config["output"][0]["sql"] = json!(
        "SELECT arrow_typeof($label) AS label, arrow_typeof($amount) AS amount, arrow_typeof($day) AS day, arrow_typeof($clock) AS clock, arrow_typeof($stamp) AS stamp, arrow_typeof($enabled) AS enabled FROM data"
    );
    success(&invoke(dir.path(), &config, "run", &[]));
    let types = fs::read_to_string(dir.path().join("out.csv")).unwrap();
    for expected in [
        "Utf8",
        "Decimal128(1, 0)",
        "Date32",
        "Time64(µs)",
        "Timestamp(µs,",
        "UTC",
        "Boolean",
    ] {
        assert!(types.contains(expected), "{expected}: {types}");
    }
}

#[test]
fn invalid_typed_values_are_configuration_errors_even_when_unused() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\n").unwrap();
    fs::write(dir.path().join("out.csv"), "old").unwrap();
    let mut config = job();
    for (declaration, value) in [
        (
            json!({"type":"decimal"}),
            "999999999999999999999999999999999999999",
        ),
        (json!({"type":"decimal"}), "1 + 2"),
        (json!({"type":"decimal"}), "not-a-number"),
        (json!({"type":"date"}), "2026-02-30"),
        (json!({"type":"date"}), "CURRENT_DATE"),
        (json!({"type":"time"}), "25:00:00"),
        (json!({"type":"timestamp"}), "not-a-timestamp"),
        (json!({"type":"boolean"}), "yes"),
        (json!({"type":"boolean"}), "TRUE OR TRUE"),
        (json!({"type":"boolean"}), ""),
    ] {
        config["params"] = json!({"test":declaration});
        let supplied = format!("test={value}");
        for command in ["check", "explain", "run"] {
            let output = invoke(dir.path(), &config, command, &["-p", &supplied]);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{value}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let diagnostic = String::from_utf8_lossy(&output.stderr);
            assert!(diagnostic.contains("parameter test"), "{diagnostic}");
            assert!(diagnostic.contains("job.json:"), "{diagnostic}");
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                "old"
            );
        }
        config["params"]["test"]["default"] = json!(format!("'{value}'"));
        assert_eq!(
            invoke(dir.path(), &config, "check", &[]).status.code(),
            Some(2)
        );
    }
}

#[test]
fn parameter_type_settings_are_validated_without_using_the_parameter() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\n").unwrap();
    let mut config = job();
    for definition in [
        json!({"type":"integer"}),
        json!({"type":"BOOLEAN"}),
        json!({"type":"string","precision":2}),
        json!({"type":"date","scale":2}),
        json!({"scale":2}),
        json!({"type":"decimal","precision":0}),
        json!({"type":"decimal","precision":39}),
        json!({"type":"decimal","scale":39}),
        json!({"type":"decimal","scale":-39}),
        json!({"type":"decimal","scale":"2"}),
        json!({"type":"decimal","precision":10}),
        json!({"type":"decimal","scale":2}),
    ] {
        let mut definition = definition;
        definition["default"] = json!(1);
        config["params"] = json!({"test":definition});
        let output = invoke(dir.path(), &config, "check", &[]);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{definition}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if definition.get("precision").is_some() || definition.get("scale").is_some() {
            assert!(String::from_utf8_lossy(&output.stderr).contains("unknown field"));
        }
    }
}

#[test]
fn decimal_precision_and_scale_are_inferred_from_constants() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "value\na\n").unwrap();
    let mut config = job();
    config["params"] = json!({"amount":{"type":"decimal"}});
    config["output"][0]["sql"] =
        json!("SELECT $amount AS amount, arrow_typeof($amount) AS kind FROM data");
    for (value, expected, kind) in [
        (
            "99999999999999999999999999999999999999",
            "99999999999999999999999999999999999999",
            "Decimal128(38, 0)",
        ),
        ("0.000123", "0.000123", "Decimal128(6, 6)"),
        ("123000", "123000", "Decimal128(6, 0)"),
        ("1.2300", "1.2300", "Decimal128(5, 4)"),
        ("0012.30", "12.30", "Decimal128(4, 2)"),
        ("-123.45", "-123.45", "Decimal128(5, 2)"),
        ("+0.001", "0.001", "Decimal128(3, 3)"),
        ("0.00", "0.00", "Decimal128(2, 2)"),
        ("1e3", "1000", "Decimal128(1, -3)"),
        (
            "1e38",
            "100000000000000000000000000000000000000",
            "Decimal128(1, -38)",
        ),
    ] {
        success(&invoke(
            dir.path(),
            &config,
            "run",
            &["-p", &format!("amount={value}")],
        ));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            format!("amount,kind\n{expected},\"{kind}\"\n")
        );
    }
    for (default, expected, kind) in [
        (
            "9007199254740993.01 + 0.02",
            "9007199254740993.03",
            "Decimal128(18, 2)",
        ),
        ("0.1 + 0.2", "0.3", "Decimal128(1, 1)"),
        ("CAST(1.23 AS DECIMAL(20, 2))", "1.23", "Decimal128(3, 2)"),
        (
            "1e38",
            "100000000000000000000000000000000000000",
            "Decimal128(1, -38)",
        ),
        (
            "99999999999999999999999999999999999999",
            "99999999999999999999999999999999999999",
            "Decimal128(38, 0)",
        ),
    ] {
        config["params"]["amount"]["default"] = json!(default);
        success(&invoke(dir.path(), &config, "run", &[]));
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            format!("amount,kind\n{expected},\"{kind}\"\n")
        );
    }
    // Unquoted YAML numeric defaults also preserve every decimal digit.
    fs::write(
        dir.path().join("amount.yml"),
        "type: decimal\ndefault: 9007199254740993.0100\n",
    )
    .unwrap();
    config["params"]["amount"] = json!({"import":"amount.yml"});
    success(&invoke(dir.path(), &config, "run", &[]));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "amount,kind\n9007199254740993.0100,\"Decimal128(20, 4)\"\n"
    );
}
