use serde_json::json;
use std::{fs, process::Command};

#[tokio::test]
async fn process_catalog_preserves_named_references_instead_of_inlining_the_chain() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "n\n1\n").unwrap();
    let processes: Vec<_> = (0..64).map(|i| json!({"id":format!("p{i}"),
        "sql":format!("SELECT * FROM {}", if i == 0 { "data".into() } else { format!("p{}", i-1) })})).collect();
    let path = dir.path().join("job.json");
    fs::write(&path, json!({
        "input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8","fields":[{"name":"n"}]}],
        "process":processes,
        "output":[{"id":"result","path":"out.csv","format":"csv","encoding":"utf-8","sql":"SELECT * FROM p63"}]
    }).to_string()).unwrap();
    let job = dxtran::config::load(&path).unwrap();
    let context = datafusion::prelude::SessionContext::new();
    dxtran::planner::plan(&context, &job).await.unwrap();
    let plan = context
        .sql("SELECT * FROM p63")
        .await
        .unwrap()
        .into_unoptimized_plan();
    let display = plan.display_indent().to_string();
    assert!(display.contains("TableScan: p63"), "{display}");
    assert!(!display.contains("TableScan: data"), "{display}");
}

#[test]
fn deep_process_views_keep_order_aliases_and_validate_unused_processes() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("in.csv"), "n\na\nc\nb\n").unwrap();
    let mut processes = vec![json!({"id":"p0","sql":"SELECT n FROM data ORDER BY n DESC LIMIT 2"})];
    for i in 1..1000 {
        processes.push(json!({"id":format!("p{i}"),"sql":format!("SELECT * FROM p{}", i-1)}));
    }
    let mut job = json!({
        "input":[{"id":"data","path":"in.csv","format":"csv","encoding":"utf-8","fields":[{"name":"n"}]}],
        "process":processes,
        "output":[{"id":"result","path":"out.csv","format":"csv","encoding":"utf-8","mode":"overwrite",
            "sql":"SELECT l.n AS left_n, r.n AS right_n FROM p999 l JOIN p999 r ON l.n=r.n"}]
    });
    let path = dir.path().join("job.json");
    for (sql, expected) in [
        ("SELECT * FROM p998", "left_n,right_n\nc,c\nb,b\n"),
        (
            "SELECT n FROM p998 EXCEPT ALL SELECT 'b' AS n",
            "left_n,right_n\nc,c\n",
        ),
    ] {
        job["process"][999]["sql"] = json!(sql);
        fs::write(&path, job.to_string()).unwrap();
        for threads in ["1", "4"] {
            let output = Command::new(env!("CARGO_BIN_EXE_dxt"))
                .arg("run")
                .arg(&path)
                .args(["--threads", threads, "--temp-dir"])
                .arg(dir.path())
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                fs::read_to_string(dir.path().join("out.csv")).unwrap(),
                expected
            );
        }
    }
    job["process"].as_array_mut().unwrap().push(json!({"id":"unused",
        "sql":"SELECT EXISTS (SELECT n FROM data d WHERE d.n=p999.n LIMIT 1 OFFSET 1) AS yes FROM p999"}));
    fs::write(&path, job.to_string()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("check")
        .arg(path)
        .arg("--temp-dir")
        .arg(dir.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(4),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("unused"));
    assert_eq!(
        fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "left_n,right_n\nc,c\n"
    );
}
