use serde_json::json;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn setup() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    fs::write(
        d.path().join("a.csv"),
        "id,k,v\nr1,b,20\nr2,a,10\nr3,b,20\nr4,a,30\nr5,c,\nr6,,15\n",
    )
    .unwrap();
    fs::write(d.path().join("b.csv"), "k,v\nb,2\na,1\nb,3\n,9\n").unwrap();
    d
}
fn run(dir: &Path, sql: &str, threads: usize) -> Output {
    let job = json!({"input":[
        {"id":"a","path":"a.csv","format":"csv","encoding":"utf-8","fields":[{"name":"id"},{"name":"k","nullable":true},{"name":"v","type":"decimal","precision":4,"nullable":true}]},
        {"id":"b","path":"b.csv","format":"csv","encoding":"utf-8","fields":[{"name":"k","nullable":true},{"name":"v","type":"decimal","precision":4}]}
    ],"process":[],"output":[{"id":"result","sql":sql,"format":"csv","encoding":"utf-8","path":"out.csv","mode":"overwrite"}]});
    fs::write(dir.join("job.yml"), job.to_string()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_dxt"))
        .arg("run")
        .arg(dir.join("job.yml"))
        .args(["--threads", &threads.to_string()])
        .output()
        .unwrap()
}
fn check(queries: &[(&str, &str)]) {
    let d = setup();
    for (sql, expected) in queries {
        for threads in [1, 4] {
            let result = run(d.path(), sql, threads);
            assert!(
                result.status.success(),
                "{sql}\n{}",
                String::from_utf8_lossy(&result.stderr)
            );
            assert_eq!(
                fs::read_to_string(d.path().join("out.csv")).unwrap(),
                *expected,
                "{sql}; threads {threads}"
            );
        }
    }
}

#[test]
fn expression_subqueries_keep_outer_order_and_null_semantics() {
    check(&[
        (
            "SELECT id, (SELECT MAX(v) FROM b) AS n FROM a",
            "id,n\nr1,9\nr2,9\nr3,9\nr4,9\nr5,9\nr6,9\n",
        ),
        (
            "SELECT id, (SELECT MAX(v) FROM b WHERE b.k=a.k) AS n FROM a",
            "id,n\nr1,3\nr2,1\nr3,3\nr4,1\nr5,\nr6,\n",
        ),
        (
            "SELECT id, (SELECT COUNT(*) FROM b WHERE b.k=a.k) AS n FROM a",
            "id,n\nr1,2\nr2,1\nr3,2\nr4,1\nr5,0\nr6,0\n",
        ),
        (
            "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
            "id\nr1\nr2\nr3\nr4\n",
        ),
        (
            "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k=a.k)",
            "id\nr5\nr6\n",
        ),
        (
            "SELECT id, k IN (SELECT k FROM b) AS yes, k NOT IN (SELECT k FROM b) AS no FROM a",
            "id,yes,no\nr1,true,false\nr2,true,false\nr3,true,false\nr4,true,false\nr5,,\nr6,,\n",
        ),
        (
            "SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k=a.k) AS yes FROM a",
            "id,yes\nr1,true\nr2,true\nr3,true\nr4,true\nr5,false\nr6,false\n",
        ),
        ("SELECT id FROM a WHERE k NOT IN (SELECT k FROM b)", "id\n"),
        (
            "SELECT (SELECT v FROM b ORDER BY v DESC LIMIT 1) AS n FROM a LIMIT 1",
            "n\n9\n",
        ),
        (
            "SELECT (SELECT v FROM b LIMIT 1 OFFSET 1) AS n FROM a LIMIT 1",
            "n\n1\n",
        ),
        (
            "SELECT (SELECT v FROM b WHERE k='missing') AS n FROM a LIMIT 1",
            "n\n\n",
        ),
        (
            "SELECT (SELECT 42 FROM b WHERE k='missing') AS n FROM a LIMIT 1",
            "n\n\n",
        ),
        (
            "SELECT id, k IN (SELECT k FROM b WHERE b.k=a.k) AS yes FROM a",
            "id,yes\nr1,true\nr2,true\nr3,true\nr4,true\nr5,false\nr6,false\n",
        ),
        (
            "SELECT id, k IN (SELECT k FROM b WHERE v>2) AS yes FROM a",
            "id,yes\nr1,true\nr2,\nr3,true\nr4,\nr5,\nr6,\n",
        ),
        (
            "SELECT id FROM a WHERE (k IN (SELECT k FROM b)) IS NULL",
            "id\nr5\nr6\n",
        ),
        (
            "SELECT id, k IN (SELECT k FROM b WHERE v>100) AS yes FROM a",
            "id,yes\nr1,false\nr2,false\nr3,false\nr4,false\nr5,false\nr6,false\n",
        ),
        (
            "SELECT id, CASE WHEN EXISTS (SELECT 1 FROM b WHERE b.k=a.k) THEN 'yes' ELSE 'no' END AS s FROM a",
            "id,s\nr1,yes\nr2,yes\nr3,yes\nr4,yes\nr5,no\nr6,no\n",
        ),
        ("SELECT (SELECT (SELECT MAX(v) FROM b)) AS n", "n\n9\n"),
        (
            "SELECT id FROM a WHERE v>(SELECT AVG(v) FROM b)",
            "id\nr1\nr2\nr3\nr4\nr6\n",
        ),
    ]);
}

#[test]
fn subqueries_work_in_aggregate_sort_window_and_distinct_on() {
    check(&[
        (
            "SELECT SUM(CASE WHEN k IN (SELECT k FROM b) THEN 1 ELSE 0 END) AS n FROM a",
            "n\n4\n",
        ),
        (
            "SELECT id FROM a ORDER BY EXISTS (SELECT 1 FROM b WHERE b.k=a.k) DESC",
            "id\nr1\nr2\nr3\nr4\nr5\nr6\n",
        ),
        (
            "SELECT id, SUM(CASE WHEN k IN (SELECT k FROM b) THEN 1 ELSE 0 END) OVER () AS n FROM a",
            "id,n\nr1,4\nr2,4\nr3,4\nr4,4\nr5,4\nr6,4\n",
        ),
        (
            "SELECT DISTINCT ON(k) k, EXISTS (SELECT 1 FROM b WHERE b.k=a.k) AS yes FROM a ORDER BY k ASC NULLS LAST",
            "k,yes\na,true\nb,true\nc,false\n,false\n",
        ),
        (
            "SELECT DISTINCT ON(EXISTS (SELECT 1 FROM b WHERE b.k=a.k)) id FROM a",
            "id\nr1\nr5\n",
        ),
    ]);
}

#[test]
fn correlated_scalar_aggregates_preserve_the_empty_group() {
    check(&[
        (
            "SELECT id, EXISTS (SELECT COUNT(*) FROM b WHERE b.k=a.k) AS yes FROM a",
            "id,yes\nr1,true\nr2,true\nr3,true\nr4,true\nr5,true\nr6,true\n",
        ),
        (
            "SELECT id, v IN (SELECT COUNT(*) FROM b WHERE b.k=a.k) AS yes FROM a",
            "id,yes\nr1,false\nr2,false\nr3,false\nr4,false\nr5,\nr6,false\n",
        ),
    ]);
}

#[test]
fn unnest_preserves_parent_and_element_order() {
    check(&[
        (
            "SELECT id, UNNEST([v, v+1, v]) AS n FROM a WHERE id IN ('r1','r2')",
            "id,n\nr1,20\nr1,21\nr1,20\nr2,10\nr2,11\nr2,10\n",
        ),
        ("SELECT UNNEST([3,1,2]) AS n", "n\n3\n1\n2\n"),
        ("SELECT x FROM UNNEST([3,1,2]) AS t(x)", "x\n3\n1\n2\n"),
        ("SELECT UNNEST([NULL,2,NULL,1]) AS n", "n\n\n2\n\n1\n"),
        ("SELECT UNNEST(CAST([] AS BIGINT[])) AS n", "n\n"),
        ("SELECT UNNEST([3,1,2]) AS n LIMIT 1 OFFSET 1", "n\n1\n"),
        ("SELECT UNNEST([3,1,2]) AS n ORDER BY n", "n\n1\n2\n3\n"),
        (
            "SELECT UNNEST([3,1,2]) AS n, UNNEST([7,8]) AS m",
            "n,m\n3,7\n1,8\n2,\n",
        ),
        ("SELECT UNNEST(CAST(NULL AS BIGINT[])) AS n", "n\n"),
        ("SELECT UNNEST(UNNEST([[3,1],[2]])) AS n", "n\n3\n1\n2\n"),
        (
            "SELECT id, UNNEST([a.v,b.v]) AS n FROM a JOIN b ON a.k=b.k WHERE id IN ('r1','r2')",
            "id,n\nr1,20\nr1,2\nr1,20\nr1,3\nr2,10\nr2,1\n",
        ),
        (
            "SELECT UNNEST([3,1,2]) AS n UNION ALL SELECT UNNEST([7,8]) AS n",
            "n\n3\n1\n2\n7\n8\n",
        ),
        (
            "SELECT DISTINCT ON(n) n FROM (SELECT UNNEST([3,1,3,2]) AS n) x",
            "n\n3\n1\n2\n",
        ),
    ]);
}

#[test]
fn unnest_element_order_survives_batch_boundaries() {
    let mut expected = String::from("id,n\n");
    for id in ["r1", "r2"] {
        for n in (1..=10000).rev() {
            use std::fmt::Write;
            writeln!(&mut expected, "{id},{n}").unwrap();
        }
    }
    check(&[(
        "SELECT id, UNNEST(range(10000,0,-1)) AS n FROM a WHERE id IN ('r1','r2')",
        &expected,
    )]);
}

#[test]
fn scalar_subquery_multiple_rows_is_an_error() {
    let d = setup();
    fs::write(d.path().join("out.csv"), "previous").unwrap();
    let result = run(d.path(), "SELECT (SELECT v FROM b) AS n FROM a", 4);
    assert!(!result.status.success());
    assert_eq!(
        fs::read_to_string(d.path().join("out.csv")).unwrap(),
        "previous"
    );
}

#[test]
fn unsupported_correlations_fail_before_replacing_output() {
    let d = setup();
    for sql in [
        "SELECT id, (SELECT v FROM b WHERE b.k=a.k ORDER BY v DESC LIMIT 1) AS n FROM a",
        "SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k=a.k LIMIT 1 OFFSET 1) AS yes FROM a",
        "SELECT id, EXISTS (SELECT COUNT(*) FROM b WHERE b.k=a.k HAVING COUNT(*)=0) AS yes FROM a",
    ] {
        fs::write(d.path().join("out.csv"), "previous").unwrap();
        let result = run(d.path(), sql, 4);
        assert_eq!(
            result.status.code(),
            Some(4),
            "{sql}\n{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            fs::read_to_string(d.path().join("out.csv")).unwrap(),
            "previous"
        );
    }
}

#[tokio::test]
async fn empty_scalar_has_nullable_output() {
    let d = setup();
    let result = run(
        d.path(),
        "SELECT (SELECT v FROM b WHERE k='missing') AS n FROM a LIMIT 1",
        1,
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let job = dxtran::config::load(&d.path().join("job.yml")).unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    let job = dxtran::planner::plan(&ctx, &job).await.unwrap();
    let frame = ctx
        .execute_logical_plan(job.outputs[0].plan.clone())
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(batches[0].schema().field(0).is_nullable());
}

#[test]
fn distinct_on_picks_stable_rows_and_preserves_explicit_sort() {
    check(&[
        (
            "SELECT DISTINCT ON (k) k,id,v FROM a",
            "k,id,v\nb,r1,20\na,r2,10\nc,r5,\n,r6,15\n",
        ),
        (
            "SELECT DISTINCT ON (k) k,id,v FROM a ORDER BY k ASC NULLS LAST, v DESC",
            "k,id,v\na,r4,30\nb,r1,20\nc,r5,\n,r6,15\n",
        ),
        (
            "SELECT DISTINCT ON (k) id FROM a ORDER BY k DESC NULLS FIRST, v DESC LIMIT 2 OFFSET 1",
            "id\nr5\nr1\n",
        ),
        (
            "SELECT DISTINCT ON (k) k,id FROM (SELECT * FROM a ORDER BY id DESC) x",
            "k,id\n,r6\nc,r5\na,r4\nb,r3\n",
        ),
    ]);
}
