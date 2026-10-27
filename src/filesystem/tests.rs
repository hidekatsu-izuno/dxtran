#[cfg(target_os = "linux")]
use super::create_staging_dir;
use super::{Security, create_staging_file, create_workspace, prepare, publish};
use std::{fs, io::Write};

#[test]
fn existing_permissions_survive_replacement_and_no_replace_is_exclusive() {
    let root = tempfile::tempdir().unwrap();
    let work = create_workspace(root.path(), "dxtran-test-").unwrap();
    let dest = root.path().join("out");
    let source = work.path().join("out");
    create_staging_file(&dest)
        .unwrap()
        .write_all(b"old")
        .unwrap();
    create_staging_file(&source)
        .unwrap()
        .write_all(b"new")
        .unwrap();
    let security = Security::read(&dest).unwrap();
    let file = prepare(&source, Some(&security)).unwrap();
    assert!(publish(&source, &dest, false, &file).is_err());
    assert_eq!(fs::read(&dest).unwrap(), b"old");
    publish(&source, &dest, true, &file).unwrap();
    drop(file);
    assert_eq!(Security::read(&dest).unwrap(), security);
    assert_eq!(fs::read(&dest).unwrap(), b"new");
}

#[test]
fn workspace_cleanup_never_removes_siblings() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("keep"), b"keep").unwrap();
    let path;
    {
        let work = create_workspace(root.path(), "dxtran-test-").unwrap();
        path = work.path().to_owned();
        create_staging_file(&path.join("data")).unwrap();
    }
    assert!(!path.exists());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}

#[cfg(target_os = "linux")]
#[test]
fn unix_modes_and_named_acl_are_preserved() {
    use rustix::fs::{XattrFlags, fgetxattr, fsetxattr};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = tempfile::tempdir().unwrap();
    let work = create_workspace(root.path(), "dxtran-test-").unwrap();
    assert_eq!(fs::metadata(work.path()).unwrap().mode() & 0o077, 0);
    for mode in [0o600, 0o640, 0o444] {
        let dest = root.path().join(format!("out-{mode}"));
        let source = work.path().join(format!("out-{mode}"));
        let dest_file = create_staging_file(&dest).unwrap();
        dest_file
            .set_permissions(fs::Permissions::from_mode(mode))
            .unwrap();
        let file = create_staging_file(&source).unwrap();
        assert_eq!(file.metadata().unwrap().mode() & 0o077, 0);
        drop(file);
        let security = Security::read(&dest).unwrap();
        let file = prepare(&source, Some(&security)).unwrap();
        publish(&source, &dest, true, &file).unwrap();
        assert_eq!(fs::metadata(&dest).unwrap().mode() & 0o777, mode);
        assert_eq!(Security::read(&dest).unwrap(), security);
    }
    // Linux ACL xattr: version 2; owner rw, named user r, group none, mask r, other none.
    let mut acl = 2u32.to_le_bytes().to_vec();
    for (tag, perms, id) in [
        (1u16, 6u16, u32::MAX),
        (2, 4, fs::metadata(root.path()).unwrap().uid()),
        (4, 0, u32::MAX),
        (16, 4, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        acl.extend(tag.to_le_bytes());
        acl.extend(perms.to_le_bytes());
        acl.extend(id.to_le_bytes());
    }
    let dest = root.path().join("acl");
    let source = work.path().join("acl");
    let original = create_staging_file(&dest).unwrap();
    fsetxattr(
        &original,
        "system.posix_acl_access",
        &acl,
        XattrFlags::empty(),
    )
    .unwrap();
    create_staging_file(&source).unwrap();
    let security = Security::read(&dest).unwrap();
    let file = prepare(&source, Some(&security)).unwrap();
    publish(&source, &dest, true, &file).unwrap();
    assert_eq!(Security::read(&dest).unwrap(), security);
    let mut actual_acl = vec![0; acl.len()];
    assert_eq!(
        fgetxattr(&file, "system.posix_acl_access", actual_acl.as_mut_slice()).unwrap(),
        acl.len()
    );
    assert_eq!(actual_acl, acl);

    // Copying a plain mode must not activate a named ACL inherited in staging.
    let inherited = work.path().join("inherited");
    create_staging_dir(&inherited).unwrap();
    let directory = fs::File::open(&inherited).unwrap();
    fsetxattr(
        &directory,
        "system.posix_acl_default",
        &acl,
        XattrFlags::empty(),
    )
    .unwrap();
    let source = inherited.join("staged");
    let dest = root.path().join("plain");
    drop(create_staging_file(&source).unwrap());
    let original = create_staging_file(&dest).unwrap();
    original
        .set_permissions(fs::Permissions::from_mode(0o640))
        .unwrap();
    let security = Security::read(&dest).unwrap();
    let file = prepare(&source, Some(&security)).unwrap();
    assert_eq!(
        fgetxattr(&file, "system.posix_acl_access", actual_acl.as_mut_slice()),
        Err(rustix::io::Errno::NODATA)
    );
    publish(&source, &dest, true, &file).unwrap();
    assert_eq!(Security::read(&dest).unwrap(), security);
}

#[cfg(windows)]
#[test]
fn prepared_output_is_unreadable_until_it_is_published() {
    let root = tempfile::tempdir().unwrap();
    let work = create_workspace(root.path(), "dxtran-test-").unwrap();
    let source = work.path().join("staged");
    let dest = root.path().join("output");
    fs::write(&dest, b"old").unwrap();
    create_staging_file(&source)
        .unwrap()
        .write_all(b"new")
        .unwrap();
    let original = Security::read(&dest).unwrap();
    let file = prepare(&source, Some(&original)).unwrap();
    assert!(fs::read(&source).is_err());
    publish(&source, &dest, true, &file).unwrap();
    drop(file);
    assert_eq!(fs::read(&dest).unwrap(), b"new");
    assert_eq!(Security::read(&dest).unwrap(), original);
}

#[cfg(target_os = "linux")]
#[test]
fn private_workspace_permissions_under_each_umask() {
    use std::{os::unix::fs::MetadataExt, process::Command};
    const MASK: &str = "DXT_WORKSPACE_TEST_UMASK";
    if let Ok(mask) = std::env::var(MASK) {
        // Run alone in a child process because umask is process-wide.
        rustix::process::umask(rustix::fs::Mode::from_raw_mode(
            u32::from_str_radix(&mask, 8).unwrap(),
        ));
        let root = tempfile::tempdir().unwrap();
        let temp = root.path().join("new-temp").join("nested");
        let work = create_workspace(&temp, "dxtran-test-").unwrap();
        let spill = work.path().join("spill");
        create_staging_dir(&spill).unwrap();
        let staged = work.path().join("staged");
        create_staging_file(&staged).unwrap();
        for directory in [temp.parent().unwrap(), &temp, work.path(), &spill] {
            assert_eq!(fs::metadata(directory).unwrap().mode() & 0o777, 0o700);
        }
        assert_eq!(fs::metadata(&staged).unwrap().mode() & 0o777, 0o600);
        work.close().unwrap();
        return;
    }
    for mask in ["000", "022", "077"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "filesystem::tests::private_workspace_permissions_under_each_umask",
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
