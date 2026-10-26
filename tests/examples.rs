use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    directory: String,
    jobs: Vec<Job>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Job {
    file: String,
    #[serde(default)]
    outputs: BTreeMap<String, String>,
    #[serde(default)]
    produced: Vec<String>,
    #[serde(default)]
    exit_code: i32,
    #[serde(default)]
    scan_exit_code: i32,
    #[serde(default)]
    diagnostic: String,
    #[serde(default)]
    absent: Vec<String>,
}

fn copy_examples(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        // 実行済みの出力を持ち込まない。期待値は元の examples から直接読む。
        if name == "out" || name == "expected" || name == "__pycache__" {
            continue;
        }
        let target = destination.join(name);
        if entry.file_type().unwrap().is_dir() {
            copy_examples(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn invoke(job: &Path, command: &str, scan: bool, threads: usize, work: &Path) -> Output {
    let mut process = Command::new(env!("CARGO_BIN_EXE_dxt"));
    process
        .arg(command)
        .arg(job)
        .args(["--threads", &threads.to_string(), "--memory-limit", "64MiB"])
        .arg("--temp-dir")
        .arg(work.join("spill"))
        // YAML の相対パスがカレントディレクトリに依存しないことも検証する。
        .current_dir(work.join("cwd"));
    if scan {
        process.arg("--scan");
    }
    process.output().unwrap()
}

fn assert_status(output: &Output, code: i32, context: &str) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "{context}\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_bytes(actual: &Path, expected: &Path, context: &str) {
    let actual_bytes =
        fs::read(actual).unwrap_or_else(|error| panic!("{context}: {actual:?}: {error}"));
    let expected_bytes = fs::read(expected).unwrap();
    let difference = actual_bytes
        .iter()
        .zip(&expected_bytes)
        .position(|(a, b)| a != b)
        .unwrap_or(actual_bytes.len().min(expected_bytes.len()));
    assert!(
        actual_bytes == expected_bytes,
        "{context}: {actual:?}: first difference at byte {difference}, lengths actual={} expected={}\nactual={}\nexpected={}",
        actual_bytes.len(),
        expected_bytes.len(),
        hex::encode(&actual_bytes),
        hex::encode(&expected_bytes)
    );
}

#[test]
fn examples_match_independent_expected_bytes() {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples");
    let cases: Vec<Case> =
        serde_json::from_slice(&fs::read(source.join("manifest.json")).unwrap()).unwrap();
    assert!(!cases.is_empty());
    for threads in [1, 4] {
        let temporary = tempfile::tempdir().unwrap();
        let work = temporary.path();
        let examples = work.join("examples");
        copy_examples(&source, &examples);
        fs::create_dir(work.join("cwd")).unwrap();
        fs::create_dir(work.join("spill")).unwrap();
        for case in &cases {
            fs::create_dir_all(examples.join(&case.directory).join("out")).unwrap();
            for job in &case.jobs {
                let context = format!("{}/{} (threads={threads})", case.directory, job.file);
                let directory = examples.join(&case.directory);
                let path = directory.join(&job.file);
                // 設定検証と計画表示は、入力の不正データを走査しない。
                let checked = invoke(&path, "check", false, threads, work);
                assert_status(&checked, 0, &context);
                let scanned = invoke(&path, "check", true, threads, work);
                assert_status(&scanned, job.scan_exit_code, &context);
                let explained = invoke(&path, "explain", false, threads, work);
                assert_status(&explained, 0, &context);
                assert!(!explained.stdout.is_empty(), "{context}: no plan");
                for output in job.outputs.keys().chain(&job.absent).chain(&job.produced) {
                    assert!(
                        !directory.join(output).exists(),
                        "{context}: check wrote {output}"
                    );
                }
                // overwrite を指定したジョブは、同じ場所への再実行でも結果が一致する。
                for _ in 0..2 {
                    let executed = invoke(&path, "run", false, threads, work);
                    assert_status(&executed, job.exit_code, &context);
                    if !job.diagnostic.is_empty() {
                        assert!(
                            String::from_utf8_lossy(&executed.stderr).contains(&job.diagnostic),
                            "{context}: missing diagnostic {:?}: {}",
                            job.diagnostic,
                            String::from_utf8_lossy(&executed.stderr)
                        );
                    }
                    for (output, expected) in &job.outputs {
                        assert_bytes(
                            &directory.join(output),
                            &source.join(&case.directory).join(expected),
                            &context,
                        );
                    }
                    for output in &job.absent {
                        assert!(
                            !directory.join(output).exists(),
                            "{context}: published {output}"
                        );
                    }
                    for output in &job.produced {
                        assert!(
                            directory.join(output).is_file(),
                            "{context}: missing {output}"
                        );
                    }
                    assert_eq!(
                        fs::read_dir(work.join("spill")).unwrap().count(),
                        0,
                        "{context}: temporary files were not cleaned"
                    );
                }
                println!("PASS {context}");
            }
        }
    }
}
