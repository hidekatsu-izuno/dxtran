#![forbid(unsafe_code)]
use rustix::fs::{self as sys, AtFlags, CWD, Gid, RenameFlags, StatxFlags, Uid, XattrFlags};
use std::{
    fs::{self, File},
    io,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

pub fn create_staging_dir(path: &Path) -> io::Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path)
}
pub fn create_staging_file(path: &Path) -> io::Result<File> {
    File::options()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}
pub fn identity(path: &Path) -> io::Result<(u64, u64)> {
    let meta = fs::metadata(path)?;
    Ok((meta.dev(), meta.ino()))
}
pub fn filesystem(path: &Path) -> io::Result<(u64, u64)> {
    let meta = fs::metadata(path)?;
    let stat = sys::statx(CWD, path, AtFlags::empty(), StatxFlags::MNT_ID)?;
    Ok((meta.dev(), stat.stx_mnt_id))
}

const ACL: &str = "system.posix_acl_access";
fn acl(file: &File) -> io::Result<Option<Vec<u8>>> {
    let mut bytes = vec![0; 65536];
    match sys::fgetxattr(file, ACL, bytes.as_mut_slice()) {
        Ok(n) => {
            bytes.truncate(n);
            Ok(Some(bytes))
        }
        Err(rustix::io::Errno::NODATA | rustix::io::Errno::OPNOTSUPP) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn regular_file(path: &Path, write: bool) -> io::Result<File> {
    let file = File::options()
        .read(true)
        .write(write)
        .custom_flags((sys::OFlags::NOFOLLOW | sys::OFlags::NONBLOCK).bits() as i32)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "output must be a regular file, not a link",
        ));
    }
    Ok(file)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Security {
    uid: u32,
    gid: u32,
    mode: u32,
    acl: Option<Vec<u8>>,
}
impl Security {
    pub fn read(path: &Path) -> io::Result<Self> {
        Self::from_file(&regular_file(path, false)?)
    }
    fn from_file(file: &File) -> io::Result<Self> {
        let meta = file.metadata()?;
        if meta.mode() & 0o7000 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "special permission bits on output are unsupported",
            ));
        }
        Ok(Self {
            uid: meta.uid(),
            gid: meta.gid(),
            mode: meta.mode() & 0o777,
            acl: acl(file)?,
        })
    }
    /// Apply through the descriptor before publication, then sync metadata too.
    fn apply(&self, file: &File) -> io::Result<()> {
        let meta = file.metadata()?;
        if meta.uid() != self.uid || meta.gid() != self.gid {
            sys::fchown(
                file,
                Some(Uid::from_raw(self.uid)),
                Some(Gid::from_raw(self.gid)),
            )?;
        }
        if let Some(acl) = &self.acl {
            sys::fsetxattr(file, ACL, acl, XattrFlags::empty())?;
        } else {
            // Do not activate named entries inherited from the temporary parent.
            match sys::fremovexattr(file, ACL) {
                Ok(()) | Err(rustix::io::Errno::NODATA | rustix::io::Errno::OPNOTSUPP) => (),
                Err(e) => return Err(e.into()),
            }
        }
        file.set_permissions(fs::Permissions::from_mode(self.mode))?;
        if &Self::from_file(file)? != self {
            return Err(io::Error::other("output access rights were not preserved"));
        }
        Ok(())
    }
}
pub fn prepare(path: &Path, security: Option<&Security>) -> io::Result<File> {
    let file = regular_file(path, true)?;
    if let Some(security) = security {
        security.apply(&file)?;
    }
    file.sync_all()?;
    Ok(file)
}
pub fn publish(source: &Path, dest: &Path, overwrite: bool, _: &File) -> io::Result<()> {
    let flags = if overwrite {
        RenameFlags::empty()
    } else {
        RenameFlags::NOREPLACE
    };
    sys::renameat_with(CWD, source, CWD, dest, flags).map_err(Into::into)
}
pub fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
