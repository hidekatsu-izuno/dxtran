use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn record(body: &[u8]) -> Vec<u8> {
    let mut bytes = ((body.len() + 4) as u16).to_be_bytes().to_vec();
    bytes.extend([0, 0]);
    bytes.extend(body);
    bytes
}

fn dataset(id: &str, format: &str, path: &str, fields: &Value) -> Value {
    let mut d = json!({"id":id,"format":format,"path":path,"encoding":"utf-8","fields":fields});
    if format == "fixed" {
        d["fixed"] = json!({"record_separator":"none"});
    }
    d
}

fn job(format: &str, fields: Value) -> Value {
    let input = dataset("data", format, "in.dat", &fields);
    let mut output = dataset("result", format, "out.dat", &fields);
    output["sql"] = json!("SELECT * FROM data");
    output["mode"] = json!("overwrite");
    json!({"input":[input],"output":[output]})
}

fn invoke(dir: &Path, job: &Value, command: &str, threads: usize) -> Output {
    fs::write(dir.join("job.json"), job.to_string()).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_dxt"));
    cmd.args([command, "job.json"]).current_dir(dir);
    if command != "doc" {
        cmd.args(["--threads", &threads.to_string()]);
    }
    if command == "check" {
        cmd.arg("--scan");
    }
    cmd.output().unwrap()
}

fn ok(output: Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn failed(output: Output, code: i32, message: &str) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(message),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn csv(sql: &str) -> Value {
    json!({"id":"csv_result","format":"csv","encoding":"utf-8","path":"out.csv","mode":"overwrite","sql":sql})
}

fn variable_fields() -> Value {
    json!([
        {"name":"item_count","type":"uint-be","length":1},
        {"name":"items","repeat":{"count_field":"item_count","min":0,"max":3},"fields":[
            {"name":"code","length":2},
            {"name":"quantity","type":"uint-be","length":1}
        ]},
        {"name":"tail","length":1}
    ])
}

#[test]
fn fixed_scalar_and_group_arrays_roundtrip_and_support_sql() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"id","length":1},
        {"name":"samples","repeat":3,"type":"uint-be","length":1},
        {"name":"items","repeat":2,"fields":[
            {"name":"code","length":2},
            {"name":"amount","type":"packed","length":2,"precision":3}
        ]},
        {"name":"tail","length":1}
    ]);
    let bytes = b"R\x01\x02\x03A1\x12\x3cB2\x04\x5d!";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let mut config = job("fixed", fields);
    config["output"].as_array_mut().unwrap().push(csv("SELECT id, samples[2] AS second_value, items[1]['amount'] AS first_amount, items[2]['code'] AS second_code FROM data"));
    for threads in [1, 4] {
        ok(invoke(dir.path(), &config, "check", threads));
        ok(invoke(dir.path(), &config, "run", threads));
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "id,second_value,first_amount,second_code\nR,2,123,B2\n"
        );
    }
    config["output"] = json!([csv(
        "SELECT id, item['code'] AS code, item['amount'] AS amount FROM (SELECT id, UNNEST(items) AS item FROM data) expanded"
    )]);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "id,code,amount\nR,A1,123\nR,B2,-45\n"
    );
}

#[test]
fn variable_rdw_handles_zero_maximum_and_following_fields() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = [
        record(b"\0!"),
        record(b"\x02A1\x01B2\x02?"),
        record(b"\x03C3\x03D4\x04E5\x05#"),
    ]
    .concat();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", variable_fields());
    config["output"].as_array_mut().unwrap().push(csv("SELECT item_count, array_length(items) AS actual_count, items[2]['code'] AS second_code, tail FROM data"));
    for threads in [1, 4] {
        ok(invoke(dir.path(), &config, "run", threads));
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "item_count,actual_count,second_code,tail\n0,0,,!\n2,2,B2,?\n3,3,D4,#\n"
        );
    }
    config["input"][0]["after"] =
        json!({"when":"COUNT(*) <> 3","level":"error","message":"wrong count"});
    config["output"][0]["rdw"] = json!({"bdw":true,"spanned":true,"max_block_length":12});
    ok(invoke(dir.path(), &config, "run", 4));
    fs::copy(dir.path().join("out.dat"), dir.path().join("in.dat")).unwrap();
    config["input"][0]["rdw"] = config["output"][0]["rdw"].clone();
    config["output"][0]["rdw"] = json!({});
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn fixed_reserves_maximum_extent_and_rdw_stores_only_actual_items() {
    let dir = tempfile::tempdir().unwrap();
    // Unused entries intentionally contain invalid UTF-8; they are not decoded.
    let bytes = b"\x01A1\x05\xff\xff\xff\xff\xff\xff!";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let mut config = job("fixed", variable_fields());
    config["output"][0]["format"] = json!("rdw");
    config["output"][0].as_object_mut().unwrap().remove("fixed");
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        record(b"\x01A1\x05!")
    );
    fs::copy(dir.path().join("out.dat"), dir.path().join("in.dat")).unwrap();
    config["input"][0]["format"] = json!("rdw");
    config["input"][0].as_object_mut().unwrap().remove("fixed");
    config["output"][0]["format"] = json!("fixed");
    config["output"][0]["fixed"] = json!({"record_length":11,"record_separator":"none"});
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        b"\x01A1\x05  \0  \0!"
    );
}

#[test]
fn nested_variable_groups_resolve_local_and_enclosing_counts() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"outer_count","type":"uint-be","length":1},
        {"name":"groups","repeat":2,"fields":[
            {"name":"count","type":"uint-be","length":1},
            {"name":"samples","repeat":{"count_field":"count","max":3},"type":"uint-be","length":1},
            {"name":"outer_samples","repeat":{"count_field":"outer_count","max":2},"type":"uint-be","length":1},
            {"name":"tail","length":1}
        ]},
        {"name":"tail","length":1}
    ]);
    let bytes = record(b"\x01\x02\x0a\x0b\x09!\0\x08?#");
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", fields);
    config["output"].as_array_mut().unwrap().push(csv("SELECT groups[1]['samples'][2] AS second_value, array_length(groups[2]['samples']) AS empty_count, groups[2]['outer_samples'][1] AS outer_value, groups[2]['tail'] AS group_tail, tail FROM data"));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "second_value,empty_count,outer_value,group_tail,tail\n11,0,8,?,#\n"
    );
}

#[test]
fn parquet_preserves_nested_types_for_physical_readback() {
    let dir = tempfile::tempdir().unwrap();
    let bytes = [record(b"\0!"), record(b"\x02A1\x01B2\x02?")].concat();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", variable_fields());
    let physical = config["output"][0].clone();
    config["output"] =
        json!([{"id":"result","format":"parquet","path":"out.parquet","sql":"SELECT * FROM data"}]);
    ok(invoke(dir.path(), &config, "run", 4));
    config["input"] = json!([{"id":"data","format":"parquet","path":"out.parquet"}]);
    config["output"] = json!([physical]);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn groups_reorder_output_children_by_name_and_preserve_nullable_elements_and_bits() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"group","fields":[{"name":"a","type":"uint-be","length":1},{"name":"b","length":1}]},
        {"name":"samples","repeat":2,"type":"uint-be","length":1,"nullable":true,"null_bytes":"ff"},
        {"name":"flags","repeat":2,"type":"bits","length":1,"bits":[{"name":"yes","bit":0},{"name":"no","bit":1}]}
    ]);
    let bytes = record(b"\x01X\x02\xff\x80\x40");
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", fields);
    config["output"][0]["sql"] = json!(
        "SELECT flags, samples, named_struct('b', \"group\"['b'], 'a', \"group\"['a']) AS \"group\" FROM data"
    );
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn lines_repeat_keeps_multibyte_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"n","length":2,"type":"decimal","precision":1},
        {"name":"samples","repeat":{"count_field":"n","max":2},"length":4}
    ]);
    let text = "1あ   \n";
    let bytes: Vec<_> = text.encode_utf16().flat_map(u16::to_le_bytes).collect();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("lines", fields);
    config["input"][0]["encoding"] = json!("utf-16le");
    config["output"][0]["encoding"] = json!("utf-16le");
    config["output"][0]["lines"] = json!({"trim_trailing_spaces":false});
    ok(invoke(dir.path(), &config, "run", 1));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn input_count_and_body_errors_include_record_and_field_context() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = job("rdw", variable_fields());
    for (body, message) in [
        (b"\x04!".as_slice(), "repeat.count_field 'item_count'"),
        (b"\x01!".as_slice(), "outside record body"),
        (b"\0!extra".as_slice(), "does not match repeat layout"),
    ] {
        fs::write(dir.path().join("in.dat"), record(body)).unwrap();
        let output = invoke(dir.path(), &config, "run", 4);
        assert!(String::from_utf8_lossy(&output.stderr).contains("record 1"));
        failed(output, 3, message);
        assert!(!dir.path().join("out.dat").exists());
    }
    config["input"][0]["fields"][0]["nullable"] = json!(true);
    config["input"][0]["fields"][0]["null_bytes"] = json!("ff");
    fs::write(dir.path().join("in.dat"), record(b"\xff!")).unwrap();
    failed(
        invoke(dir.path(), &config, "check", 1),
        3,
        "must be non-NULL",
    );
    config["input"][0]["fields"][0]
        .as_object_mut()
        .unwrap()
        .remove("nullable");
    config["input"][0]["fields"][0]
        .as_object_mut()
        .unwrap()
        .remove("null_bytes");
    config["input"][0]["fields"][0]["type"] = json!("int-be");
    failed(
        invoke(dir.path(), &config, "check", 1),
        3,
        "repeat.count_field",
    );
    config["input"][0]["fields"][1]["repeat"]["min"] = json!(1);
    fs::write(dir.path().join("in.dat"), record(b"\0!")).unwrap();
    failed(invoke(dir.path(), &config, "check", 1), 3, "1..=3");
}

#[test]
fn output_mismatches_never_publish_any_staged_output() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), record(b"\x02A1\x01B2\x02!")).unwrap();
    let mut config = job("rdw", variable_fields());
    config["output"]
        .as_array_mut()
        .unwrap()
        .insert(0, csv("SELECT item_count FROM data"));
    for (sql, message) in [
        (
            "SELECT item_count, array_slice(items,1,1) AS items, tail FROM data",
            "array length 1 does not match required count 2",
        ),
        (
            "SELECT item_count, CASE WHEN item_count > 0 THEN NULL ELSE items END AS items, tail FROM data",
            "array must not be NULL",
        ),
        (
            "SELECT CAST(9 AS BIGINT) AS item_count, items, tail FROM data",
            "repeat.count_field",
        ),
    ] {
        fs::write(dir.path().join("out.dat"), b"original").unwrap();
        fs::write(dir.path().join("out.csv"), b"original-csv").unwrap();
        config["output"][1]["sql"] = json!(sql);
        failed(invoke(dir.path(), &config, "run", 4), 5, message);
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"original");
        assert_eq!(
            fs::read(dir.path().join("out.csv")).unwrap(),
            b"original-csv"
        );
    }
}

#[test]
fn invalid_repeat_definitions_are_rejected_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), b"\x01x").unwrap();
    for repeat in [
        json!(0),
        json!(-1),
        json!(true),
        json!({"count_field":"n"}),
        json!({"count_field":"n","max":0}),
        json!({"count_field":"n","min":3,"max":2}),
        json!({"count_field":"missing","max":3}),
        json!({"count_field":"n + 1","max":3}),
        json!({"depending_on":"n","max":3}),
        json!({"count":"n","max":3}),
    ] {
        let fields = json!([{"name":"n","type":"uint-be","length":1},{"name":"samples","repeat":repeat,"length":1}]);
        failed(invoke(dir.path(), &job("fixed", fields), "check", 1), 2, "");
    }
    for fields in [
        json!([{"name":"samples","repeat":{"count_field":"n","max":3},"length":1},{"name":"n","type":"uint-be","length":1}]),
        json!([{"name":"n","length":1},{"name":"samples","repeat":{"count_field":"n","max":3},"length":1}]),
        json!([{"name":"samples","repeat":16777217,"length":1}]),
        json!([{"name":"samples","repeat":2,"fields":[]}]),
        json!([{"name":"samples","repeat":2,"fields":[{"name":"a","length":1},{"name":"a","length":1}]}]),
        json!([{"name":"samples","occurs":2,"length":1}]),
    ] {
        failed(invoke(dir.path(), &job("fixed", fields), "check", 1), 2, "");
    }
    let mut config = job("rdw", variable_fields());
    config["input"][0]["fields"][2]["offset"] = json!(10);
    failed(
        invoke(dir.path(), &config, "check", 1),
        2,
        "omit offset after a variable repeat",
    );
}

#[test]
fn arrays_preserve_order_across_batches_and_empty_input_is_valid() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([{"name":"samples","repeat":2,"type":"uint-be","length":2}]);
    let bytes: Vec<_> = (0..8201u16)
        .flat_map(|n| [n.to_be_bytes(), n.to_be_bytes()].concat())
        .collect();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("fixed", fields);
    config["output"]
        .as_array_mut()
        .unwrap()
        .push(csv("SELECT UNNEST(samples) AS value FROM data"));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    let expected = format!(
        "value\n{}",
        (0..8201).map(|n| format!("{n}\n{n}\n")).collect::<String>()
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        expected
    );
    fs::write(dir.path().join("in.dat"), b"").unwrap();
    ok(invoke(dir.path(), &config, "run", 4));
    assert!(fs::read(dir.path().join("out.dat")).unwrap().is_empty());
}

#[test]
fn documentation_and_imported_nested_errors_show_field_paths_and_source() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("children.yml"),
        "- name: code\n  length: 2\n  description: 商品コード\n",
    )
    .unwrap();
    let fields = json!([{"name":"items","repeat":2,"fields":{"import":"children.yml"}}]);
    let config = job("fixed", fields);
    let doc = ok(invoke(dir.path(), &config, "doc", 1));
    assert!(doc.contains("items[].code"), "{doc}");
    assert!(doc.contains("repeat: 2") && doc.contains("商品コード"));
    fs::write(dir.path().join("in.dat"), b"A1B2").unwrap();
    ok(invoke(dir.path(), &config, "run", 1));
    fs::write(
        dir.path().join("children.yml"),
        "- name: code\n  length: 0\n",
    )
    .unwrap();
    failed(invoke(dir.path(), &config, "check", 1), 2, "children.yml");
}

#[test]
fn conditional_arrays_skip_inactive_values_and_counts() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"n","type":"uint-be","length":1},
        {"name":"items","offset":2,"repeat":{"count_field":"n","max":2},
         "when":"kind = 'D'","type":"uint-be","length":1},
        {"name":"label","offset":2,"length":2,"when":"kind = 'H'"}
    ]);
    let bytes = b"H\xffOKD\x02\x03\x04";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let mut config = job("fixed", fields);
    config["output"].as_array_mut().unwrap().push(csv(
        "SELECT kind, items IS NULL AS missing, items[1] AS first_value, label FROM data",
    ));
    for threads in [1, 4] {
        ok(invoke(dir.path(), &config, "check", threads));
        ok(invoke(dir.path(), &config, "run", threads));
        assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
        assert_eq!(
            fs::read_to_string(dir.path().join("out.csv")).unwrap(),
            "kind,missing,first_value,label\nH,true,,OK\nD,false,3,\n"
        );
    }
    // Even an invalid array length is ignored when its condition is false.
    config["output"][0]["sql"] = json!(
        "SELECT kind, n, CASE WHEN kind = 'H' THEN [CAST(1 AS BIGINT UNSIGNED),CAST(2 AS BIGINT UNSIGNED),CAST(3 AS BIGINT UNSIGNED)] ELSE items END AS items, label FROM data"
    );
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn nested_conditions_resolve_later_common_fields_and_enclosing_fields() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"items","repeat":2,"fields":[
            {"name":"value","offset":0,"length":2,"type":"decimal","precision":2,"when":"tag = 'N' AND kind = 'D'"},
            {"name":"label","offset":0,"length":2,"when":"tag <> 'N' OR kind <> 'D'"},
            {"name":"tag","offset":2,"length":1}
        ]}
    ]);
    let bytes = b"D12NabTHxxNyzN";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let mut config = job("fixed", fields);
    config["output"].as_array_mut().unwrap().push(csv("SELECT kind, items[1]['value'] AS value, items[1]['label'] AS label, items[2]['label'] AS second_label FROM data"));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "kind,value,label,second_label\nD,12,,ab\nH,,xx,yz\n"
    );
}

#[test]
fn variable_repeat_and_conditional_rest_roundtrip_empty_and_nonempty_tails() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"n","type":"uint-be","length":1},
        {"name":"items","when":"kind = 'D'","repeat":{"count_field":"n","max":3},"type":"uint-be","length":1},
        {"name":"tail","length":"rest","max_length":10,"when":"kind IN ('D','H')"}
    ]);
    let bytes = [
        record(b"D\x02\x07\x08memo "),
        record(b"D\0"),
        record(b"H\xffheader"),
    ]
    .concat();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", fields);
    config["output"].as_array_mut().unwrap().push(csv("SELECT kind, array_length(items) AS n, items IS NULL AS missing, length(tail) AS tail_length FROM data"));
    ok(invoke(dir.path(), &config, "check", 4));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "kind,n,missing,tail_length\nD,2,false,5\nD,0,false,0\nH,,true,6\n"
    );
    fs::write(dir.path().join("in.dat"), record(b"D\0this is too long")).unwrap();
    failed(invoke(dir.path(), &config, "check", 1), 3, "max_length");
}

#[test]
fn bounded_repeated_groups_delimit_each_conditional_rest() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"n","type":"uint-be","length":1},
        {"name":"items","length":5,"repeat":{"count_field":"n","max":3},"fields":[
            {"name":"kind","length":1},
            {"name":"text","offset":1,"length":"rest","max_length":4,"when":"kind = 'T'"},
            {"name":"raw","offset":1,"length":"rest","type":"binary","when":"kind = 'B'"}
        ]},
        {"name":"tail","length":"rest"}
    ]);
    let bytes = [record(b"\x02Tabc B\xff\0\x01\x02end"), record(b"\0")].concat();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", fields);
    config["output"].as_array_mut().unwrap().push(csv(
        "SELECT items[1]['text'] AS text, items[2]['raw'] IS NOT NULL AS has_raw, tail FROM data",
    ));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "text,has_raw,tail\nabc ,true,end\n,false,\"\"\n"
    );
    config["output"][0]["sql"] =
        json!("SELECT n, array_slice(items, 1, 1) AS items, tail FROM data");
    failed(invoke(dir.path(), &config, "run", 4), 5, "array length");
    fs::write(dir.path().join("in.dat"), record(b"\x01Txx")).unwrap();
    failed(
        invoke(dir.path(), &config, "check", 1),
        3,
        "group lies outside record body",
    );
}

#[test]
fn conditional_groups_and_single_rest_elements_are_supported() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"group","offset":1,"when":"kind = 'D'","fields":[
            {"name":"n","type":"uint-be","length":1},
            {"name":"values","repeat":{"count_field":"n","max":2},"length":1},
            {"name":"tail","length":"rest"}
        ]},
        {"name":"labels","offset":1,"when":"kind = 'H'","repeat":1,"length":"rest"}
    ]);
    let bytes = [record(b"D\x02abtail"), record(b"Hheader"), record(b"H")].concat();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let mut config = job("rdw", fields);
    config["output"].as_array_mut().unwrap().push(csv(
        "SELECT kind, \"group\"['tail'] AS tail, labels[1] AS label FROM data",
    ));
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "kind,tail,label\nD,tail,\nH,,header\nH,,\"\"\n"
    );
}

#[test]
fn alternate_arrays_can_share_names_and_offsets_but_not_be_active_together() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"values","offset":1,"repeat":2,"length":1,"type":"uint-be","when":"kind = 'A'"},
        {"name":"values","offset":1,"repeat":2,"length":1,"type":"uint-le","when":"kind = 'B'"}
    ]);
    let bytes = b"A\x01\x02B\x03\x04";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let mut config = job("fixed", fields);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
    config["input"][0]["fields"][2]["when"] = json!("kind IN ('A','B')");
    failed(
        invoke(dir.path(), &config, "check", 1),
        3,
        "duplicate active field",
    );
}

#[test]
fn invalid_conditional_repeat_definitions_and_dependencies_are_reported() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), record(b"x")).unwrap();
    for fields in [
        json!([{"name":"values","repeat":2,"length":"rest"}]),
        json!([{"name":"values","repeat":2,"fields":[{"name":"tail","length":"rest"}]}]),
        json!([{"name":"values","repeat":2,"length":1,"fields":[{"name":"text","length":2}]}]),
        json!([{"name":"values","repeat":1,"length":1,"when":"missing = 'x'"}]),
        json!([{"name":"values","repeat":1,"length":1,"when":"random() > 0.5"}]),
        json!([{"name":"a","length":1,"when":"TRUE"},{"name":"values","repeat":1,"offset":u64::MAX,"length":1,"when":"TRUE"}]),
        json!([{"name":"a","length":1,"when":"TRUE"},{"name":"values","repeat":1,"length":1,"when":"a = 'x'"}]),
    ] {
        failed(invoke(dir.path(), &job("rdw", fields), "check", 1), 2, "");
    }
    let fields = json!([
        {"name":"values","repeat":1,"length":"rest","when":"tag = 'x'"},
        {"name":"tag","length":1}
    ]);
    failed(
        invoke(dir.path(), &job("rdw", fields), "check", 1),
        3,
        "cyclic dependency",
    );
}

#[test]
fn output_conditions_use_sql_values_and_failures_preserve_outputs() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.dat"), record(b"A\x01\x02")).unwrap();
    let fields = json!([
        {"name":"kind","length":1},
        {"name":"values","repeat":2,"length":1,"type":"uint-be"}
    ]);
    let mut config = job("rdw", fields);
    config["output"][0]["fields"][1]["when"] = json!("array_length(values) = 2 AND kind = 'A'");
    ok(invoke(dir.path(), &config, "run", 1));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        record(b"A\x01\x02")
    );
    config["output"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"tail","length":"rest"}));
    config["output"][0]["sql"] = json!("SELECT kind, \"values\", 'abc' AS tail FROM data");
    ok(invoke(dir.path(), &config, "run", 1));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        record(b"A\x01\x02abc")
    );
    // A field after an active rest is invalid, even when that rest is empty.
    config["output"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"suffix","length":1}));
    config["output"][0]["sql"] =
        json!("SELECT kind, \"values\", '' AS tail, '!' AS suffix FROM data");
    config["output"]
        .as_array_mut()
        .unwrap()
        .insert(0, csv("SELECT kind FROM data"));
    fs::write(dir.path().join("out.dat"), b"original").unwrap();
    fs::write(dir.path().join("out.csv"), b"original-csv").unwrap();
    failed(
        invoke(dir.path(), &config, "run", 4),
        5,
        "last active field",
    );
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"original");
    assert_eq!(
        fs::read(dir.path().join("out.csv")).unwrap(),
        b"original-csv"
    );
}

#[test]
fn fixed_unused_conditional_group_elements_are_zero_filled() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"n","length":1,"type":"uint-be"},
        {"name":"items","repeat":{"count_field":"n","max":2},"fields":[
            {"name":"kind","length":1},
            {"name":"number","offset":1,"length":1,"type":"uint-be","when":"kind = 'N'"},
            {"name":"text","offset":1,"length":1,"when":"kind = 'T'"}
        ]}
    ]);
    fs::write(dir.path().join("in.dat"), b"\x01N\x05\xff\xff").unwrap();
    let config = job("fixed", fields);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(
        fs::read(dir.path().join("out.dat")).unwrap(),
        b"\x01N\x05\0\0"
    );
}

#[test]
fn nested_when_parameters_are_bound() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([{"name":"items","repeat":2,"fields":[
        {"name":"kind","length":1},
        {"name":"text","offset":1,"length":1,"when":"kind = ${kind}"},
        {"name":"number","offset":1,"length":1,"type":"uint-be","when":"kind <> ${kind}"}
    ]}]);
    let mut config = job("fixed", fields);
    config["params"] = json!({"kind":{"default":"'T'"}});
    fs::write(dir.path().join("in.dat"), b"TxN\x05").unwrap();
    ok(invoke(dir.path(), &config, "run", 1));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"TxN\x05");
}

#[test]
fn rdw_group_elements_can_have_different_lengths_selected_by_when() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"items","repeat":2,"fields":[
            {"name":"kind","length":1},
            {"name":"number","offset":1,"length":2,"type":"uint-be","when":"kind = 'N'"},
            {"name":"text","offset":1,"length":1,"when":"kind = 'T'"}
        ]},
        {"name":"tail","length":"rest","type":"binary"}
    ]);
    let bytes = record(b"N\x01\x02Ta\xff\x01");
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let config = job("rdw", fields);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn nested_input_conditions_can_read_a_later_sibling_group() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"items","repeat":2,"fields":[
            {"name":"n","length":1,"type":"uint-be","when":"header['kind'] = 'N'"},
            {"name":"text","offset":0,"length":1,"when":"header['kind'] <> 'N'"}
        ]},
        {"name":"header","offset":2,"fields":[{"name":"kind","length":1}]}
    ]);
    let bytes = b"\x01\x02NabT";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let config = job("fixed", fields);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}

#[test]
fn null_conditions_skip_arrays_and_output_uses_actual_sql_types() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([
        {"name":"flag","type":"decimal","length":1,"precision":1,"nullable":true,"null_if":["?"],"null_output":"?"},
        {"name":"items","offset":1,"length":1,"repeat":1,"when":"flag > 0"},
        {"offset":1,"length":1,"fill_byte":32,"when":"flag IS NULL OR flag <= 0"}
    ]);
    let bytes = b"? 1a";
    fs::write(dir.path().join("in.dat"), bytes).unwrap();
    let mut config = job("fixed", fields);
    config["output"][0]["sql"] =
        json!("SELECT CAST(flag AS DECIMAL(5,2)) AS flag, items FROM data");
    ok(invoke(dir.path(), &config, "run", 4));
    // Fixed numeric NULL uses the existing zero-fill rule; the NULL condition
    // must still skip the NULL array before that physical conversion.
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), b"0 1a");
}

#[test]
fn conditional_group_boundaries_reject_gaps_overlaps_and_short_output() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([{"name":"items","repeat":1,"length":4,"fields":[
        {"name":"kind","length":1},
        {"name":"text","length":"rest","max_length":3,"when":"kind = 'T'"}
    ]}]);
    fs::write(dir.path().join("in.dat"), record(b"Tabc")).unwrap();
    let mut config = job("rdw", fields);
    config["output"][0]["sql"] =
        json!("SELECT [named_struct('kind','T','text','x')] AS items FROM data");
    failed(invoke(dir.path(), &config, "run", 1), 5, "declared group");
    config["output"][0]["sql"] = json!("SELECT * FROM data");
    config["input"][0]["fields"][0]["fields"][1]["offset"] = json!(2);
    failed(
        invoke(dir.path(), &config, "check", 1),
        3,
        "overlaps or gaps",
    );
    config["input"][0]["fields"][0]["fields"][1]["offset"] = json!(0);
    failed(invoke(dir.path(), &config, "check", 1), 3, "max_length");
}

#[test]
fn a_single_rest_array_element_can_occupy_an_empty_rdw_body() {
    let dir = tempfile::tempdir().unwrap();
    let fields = json!([{"name":"items","repeat":1,"length":"rest"}]);
    let bytes = [record(b""), record(b"data")].concat();
    fs::write(dir.path().join("in.dat"), &bytes).unwrap();
    let config = job("rdw", fields);
    ok(invoke(dir.path(), &config, "run", 4));
    assert_eq!(fs::read(dir.path().join("out.dat")).unwrap(), bytes);
}
