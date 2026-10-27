#![cfg(target_os = "linux")]
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    process::Command,
};

#[test]
fn private_staging_and_output_permissions_under_each_umask() {
    const MASK: &str = "DXT_TEST_UMASK";
    if let Ok(mask) = std::env::var(MASK) {
        // This branch runs alone in a child test process: umask is process-wide.
        rustix::process::umask(rustix::fs::Mode::from_raw_mode(
            u32::from_str_radix(&mask, 8).unwrap(),
        ));
        let root = tempfile::tempdir().unwrap();
        let temp = root.path().join("new-temp").join("nested");
        fs::write(root.path().join("in.csv"), b"value\nnew\n").unwrap();
        let job = serde_json::json!({"input":[{"id":"data","format":"csv","encoding":"utf-8","path":"in.csv","fields":[{"name":"value"}]}],"output":[{"id":"result","format":"csv","encoding":"utf-8","sql":"SELECT * FROM data","path":"out.csv","mode":"overwrite"}]});
        let jobfile = root.path().join("job.yml");
        fs::write(&jobfile, job.to_string()).unwrap();
        let dest = root.path().join("out.csv");
        for mode in [None, Some(0o600), Some(0o640), Some(0o444)] {
            let before = mode.map(|mode| {
                fs::set_permissions(&dest, fs::Permissions::from_mode(mode)).unwrap();
                let meta = fs::metadata(&dest).unwrap();
                (meta.uid(), meta.gid())
            });
            let output = Command::new(env!("CARGO_BIN_EXE_dxt"))
                .arg("run")
                .arg(&jobfile)
                .arg("--temp-dir")
                .arg(&temp)
                .args(["--threads", "1"])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(fs::read(&dest).unwrap(), b"value\nnew\n");
            assert_eq!(
                fs::metadata(&dest).unwrap().mode() & 0o777,
                mode.unwrap_or(0o600)
            );
            if let Some(before) = before {
                let meta = fs::metadata(&dest).unwrap();
                assert_eq!((meta.uid(), meta.gid()), before);
            }
            assert_eq!(fs::read_dir(&temp).unwrap().count(), 0);
            for directory in [temp.parent().unwrap(), &temp] {
                assert_eq!(fs::metadata(directory).unwrap().mode() & 0o777, 0o700);
            }
        }
        return;
    }
    for mask in ["000", "022", "077"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "private_staging_and_output_permissions_under_each_umask",
                "--test-threads=1",
            ])
            .env(MASK, mask)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "umask {mask}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
