//! OS access control and same-filesystem publication behind a safe interface.
//! Callers must control the output parent directories during publication.
use std::{fs, io, path::Path};
use tempfile::{Builder, TempDir};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(windows)]
// Windows access control and handle-based rename require FFI. Keep unsafe here.
#[allow(unsafe_code)]
mod windows;
#[cfg(windows)]
use windows as platform;
#[cfg(not(any(target_os = "linux", windows)))]
compile_error!("dxtran supports Linux and Windows only");

pub(crate) use platform::{
    Security, create_staging_dir, create_staging_file, filesystem, identity, prepare, publish,
    sync_directory,
};

pub(crate) fn create_workspace(parent: &Path, prefix: &str) -> io::Result<TempDir> {
    if prefix.contains(['/', '\\']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid workspace prefix",
        ));
    }
    let mut parents = fs::DirBuilder::new();
    parents.recursive(true);
    let mut workspace = Builder::new();
    workspace.prefix(prefix).rand_bytes(32);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        parents.mode(0o700);
        workspace.permissions(fs::Permissions::from_mode(0o700));
    }
    // Windows inherits the chosen parent's ACL. Callers manage that directory.
    parents.create(parent)?;
    workspace.tempdir_in(parent)
}

#[cfg(test)]
mod tests;
