//! Local NTFS access control. FFI allocations and handles never escape this module.
use std::{
    ffi::{OsStr, c_void},
    fs::{self, File},
    io,
    mem::{offset_of, size_of},
    os::windows::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawHandle,
    },
    path::Path,
    ptr::null_mut,
};
use windows_sys::Win32::{
    Foundation::*,
    Security::{Authorization::*, *},
    Storage::FileSystem::*,
};

const SECURITY: u32 = OWNER_SECURITY_INFORMATION
    | GROUP_SECURITY_INFORMATION
    | DACL_SECURITY_INFORMATION
    | LABEL_SECURITY_INFORMATION;
fn checked(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
fn status(code: u32) -> io::Result<()> {
    if code == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(code as i32))
    }
}
fn wide(s: &OsStr) -> io::Result<Vec<u16>> {
    let mut value: Vec<_> = s.encode_wide().collect();
    if value.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in path or security descriptor",
        ));
    }
    value.push(0);
    Ok(value)
}
struct Local(*mut c_void);
impl Drop for Local {
    fn drop(&mut self) {
        // SAFETY: All Local values originate from APIs documented to use LocalAlloc.
        unsafe {
            LocalFree(self.0);
        }
    }
}
impl Local {
    fn descriptor(sddl: &str) -> io::Result<Self> {
        let text = wide(OsStr::new(sddl))?;
        let mut ptr = null_mut();
        // SAFETY: text is terminated and live; ptr is an initialized output slot.
        checked(unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                text.as_ptr(),
                1,
                &mut ptr,
                null_mut(),
            )
        })?;
        Ok(Self(ptr))
    }
}
pub fn create_staging_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}
pub fn create_staging_file(path: &Path) -> io::Result<File> {
    File::options().write(true).create_new(true).open(path)
}
fn open(path: &Path, access: u32, sharing: u32) -> io::Result<File> {
    File::options()
        .access_mode(access)
        .share_mode(sharing)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}
fn regular(file: &File) -> io::Result<()> {
    let meta = file.metadata()?;
    if !meta.is_file() || meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "output must be a regular file, not a reparse point",
        ));
    }
    // Replacing encrypted contents with an ordinary staged file would remove EFS.
    if meta.file_attributes() & FILE_ATTRIBUTE_ENCRYPTED != 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "encrypted output is unsupported",
        ));
    }
    Ok(())
}
pub fn identity(path: &Path) -> io::Result<(u64, u64)> {
    let file = open(
        path,
        FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
    )?;
    // SAFETY: The output struct is plain C data and the handle is live.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: info is writable and correctly sized.
    checked(unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) })?;
    Ok((
        u64::from(info.dwVolumeSerialNumber),
        (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    ))
}
pub fn filesystem(path: &Path) -> io::Result<(u64, u64)> {
    let path = fs::canonicalize(path)?;
    // Canonical local paths start with \\?\C:\; UNC and device namespaces are excluded.
    let text = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if text.len() < 7 || text[..4] != [92, 92, 63, 92] || text[5] != 58 || text[6] != 92 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "publication requires local NTFS",
        ));
    }
    let file = open(
        &path,
        FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
    )?;
    let (mut serial, mut flags) = (0, 0);
    let mut name = [0u16; 32];
    // SAFETY: All output pointers reference live storage with the supplied lengths.
    checked(unsafe {
        GetVolumeInformationByHandleW(
            file.as_raw_handle(),
            null_mut(),
            0,
            &mut serial,
            null_mut(),
            &mut flags,
            name.as_mut_ptr(),
            name.len() as u32,
        )
    })?;
    let name =
        String::from_utf16_lossy(&name[..name.iter().position(|c| *c == 0).unwrap_or(name.len())]);
    if name != "NTFS" || flags & 8 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "publication requires NTFS with persistent ACLs",
        ));
    }
    Ok((u64::from(serial), 0))
}

/// Effective owner/group/DACL and integrity label. The DACL is protected against inheritance
/// from the staging directory or destination; the original access rules are retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Security {
    sddl: String,
}
impl Security {
    pub fn read(path: &Path) -> io::Result<Self> {
        let file = open(
            path,
            READ_CONTROL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )?;
        regular(&file)?;
        Self::from_file(&file)
    }
    fn from_file(file: &File) -> io::Result<Self> {
        let mut descriptor = null_mut();
        // SAFETY: handle is valid and descriptor receives owned LocalAlloc storage.
        status(unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                SECURITY,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                &mut descriptor,
            )
        })?;
        let descriptor = Local(descriptor);
        let mut dacl = null_mut();
        let mut sacl = null_mut();
        let (mut present, mut defaulted) = (0, 0);
        // SAFETY: GetSecurityInfo allocated a valid, writable descriptor. Mutations
        // affect only this local copy, not the file. GetAce bounds-checks each index.
        unsafe {
            checked(GetSecurityDescriptorDacl(
                descriptor.0,
                &mut present,
                &mut dacl,
                &mut defaulted,
            ))?;
            if present == 0 || dacl.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "NULL output DACL is unsupported",
                ));
            }
            for index in 0..u32::from((*dacl).AceCount) {
                let mut ace = null_mut();
                checked(GetAce(dacl, index, &mut ace))?;
                (*(ace.cast::<ACE_HEADER>())).AceFlags &= !(INHERITED_ACE as u8);
            }
            checked(SetSecurityDescriptorControl(
                descriptor.0,
                SE_DACL_PROTECTED
                    | SE_DACL_AUTO_INHERITED
                    | SE_DACL_AUTO_INHERIT_REQ
                    | SE_SACL_AUTO_INHERITED
                    | SE_SACL_AUTO_INHERIT_REQ,
                SE_DACL_PROTECTED,
            ))?;
            checked(GetSecurityDescriptorSacl(
                descriptor.0,
                &mut present,
                &mut sacl,
                &mut defaulted,
            ))?;
            if present != 0 && !sacl.is_null() {
                for index in 0..u32::from((*sacl).AceCount) {
                    let mut ace = null_mut();
                    checked(GetAce(sacl, index, &mut ace))?;
                    (*(ace.cast::<ACE_HEADER>())).AceFlags &= !(INHERITED_ACE as u8);
                }
            }
        }
        let mut text = null_mut();
        let mut length = 0;
        // SAFETY: descriptor stays live; output length includes its terminating NUL.
        checked(unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor.0,
                1,
                SECURITY,
                &mut text,
                &mut length,
            )
        })?;
        let allocation = Local(text.cast());
        // SAFETY: The successful API initialized length UTF-16 code units.
        let units = unsafe { std::slice::from_raw_parts(text, length as usize) };
        let end = units
            .iter()
            .position(|c| *c == 0)
            .ok_or_else(|| io::Error::other("unterminated security descriptor"))?;
        let sddl = String::from_utf16_lossy(&units[..end]);
        drop(allocation);
        Ok(Self { sddl })
    }
    fn apply(&self, file: &File) -> io::Result<()> {
        let descriptor = Local::descriptor(&self.sddl)?;
        let (mut owner, mut group, mut dacl, mut sacl) =
            (null_mut(), null_mut(), null_mut(), null_mut());
        let (mut present, mut defaulted) = (0, 0);
        // SAFETY: descriptor owns all returned SID/ACL pointers until SetSecurityInfo returns.
        unsafe {
            checked(GetSecurityDescriptorOwner(
                descriptor.0,
                &mut owner,
                &mut defaulted,
            ))?;
            checked(GetSecurityDescriptorGroup(
                descriptor.0,
                &mut group,
                &mut defaulted,
            ))?;
            checked(GetSecurityDescriptorDacl(
                descriptor.0,
                &mut present,
                &mut dacl,
                &mut defaulted,
            ))?;
            checked(GetSecurityDescriptorSacl(
                descriptor.0,
                &mut present,
                &mut sacl,
                &mut defaulted,
            ))?;
            // No explicit label normally means both files use the default label.
            // Do not request a NULL label update; verification rejects a staged
            // label that differs from the original in this case.
            let security = if sacl.is_null() {
                SECURITY & !LABEL_SECURITY_INFORMATION
            } else {
                SECURITY
            };
            status(SetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                security | PROTECTED_DACL_SECURITY_INFORMATION,
                owner,
                group,
                dacl,
                sacl,
            ))?;
        }
        if Self::from_file(file)? != *self {
            return Err(io::Error::other("output access rights were not preserved"));
        }
        Ok(())
    }
}
pub fn prepare(path: &Path, security: Option<&Security>) -> io::Result<File> {
    // Windows permits bypassing directory traversal checks. Keep an exclusive
    // handle until rename, so granting final ACLs cannot expose the staged data.
    let file = open(
        path,
        GENERIC_WRITE | READ_CONTROL | WRITE_DAC | WRITE_OWNER | DELETE,
        0,
    )?;
    regular(&file)?;
    if let Some(security) = security {
        security.apply(&file)?;
    }
    file.sync_all()?;
    Ok(file)
}
pub fn publish(_: &Path, dest: &Path, overwrite: bool, file: &File) -> io::Result<()> {
    let name = wide(dest.as_os_str())?;
    let bytes = offset_of!(FILE_RENAME_INFO, FileName) + name.len() * 2;
    let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
    let info = storage.as_mut_ptr().cast::<FILE_RENAME_INFO>();
    // SAFETY: storage is aligned for FILE_RENAME_INFO and includes its variable
    // UTF-16 tail. The live exclusive handle owns DELETE access for rename.
    unsafe {
        (*info).Anonymous.ReplaceIfExists = overwrite;
        (*info).RootDirectory = null_mut();
        (*info).FileNameLength = ((name.len() - 1) * 2) as u32;
        std::ptr::copy_nonoverlapping(name.as_ptr(), (*info).FileName.as_mut_ptr(), name.len());
        checked(SetFileInformationByHandle(
            file.as_raw_handle(),
            FileRenameInfo,
            info.cast(),
            bytes as u32,
        ))
    }
}
pub fn sync_directory(_: &Path) -> io::Result<()> {
    // Windows has no unprivileged equivalent of Linux directory fsync. File
    // contents/security are flushed before rename; power-loss durability differs.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_inherits_parent_dacl() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("shared");
        // A shared parent makes inheritance observable instead of relying on the
        // test user's temporary directory already having restrictive permissions.
        let descriptor = Local::descriptor("D:P(A;OICI;FA;;;WD)").unwrap();
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        let name = wide(parent.as_os_str()).unwrap();
        // SAFETY: The terminated name and security descriptor outlive the call.
        checked(unsafe { CreateDirectoryW(name.as_ptr(), &attributes) }).unwrap();
        let workspace = super::super::create_workspace(&parent, "dxtran-test-").unwrap();
        let directory = workspace.path().join("spill");
        create_staging_dir(&directory).unwrap();
        let path = directory.join("data");
        drop(create_staging_file(&path).unwrap());
        for path in [workspace.path(), &directory, &path] {
            let file = open(
                path,
                READ_CONTROL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            )
            .unwrap();
            let mut raw = null_mut();
            let mut acl = null_mut();
            // SAFETY: The live handle is readable; each output pointer is initialized.
            status(unsafe {
                GetSecurityInfo(
                    file.as_raw_handle(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    null_mut(),
                    null_mut(),
                    &mut acl,
                    null_mut(),
                    &mut raw,
                )
            })
            .unwrap();
            let descriptor = Local(raw);
            let (mut control, mut revision) = (0, 0);
            let mut ace = null_mut();
            // SAFETY: GetSecurityInfo returned a live descriptor owning these ACL pointers.
            unsafe {
                checked(GetSecurityDescriptorControl(
                    descriptor.0,
                    &mut control,
                    &mut revision,
                ))
                .unwrap();
                assert_eq!(control & SE_DACL_PROTECTED, 0);
                assert!(!acl.is_null());
                assert_eq!((*acl).AceCount, 1);
                checked(GetAce(acl, 0, &mut ace)).unwrap();
                let header = &*ace.cast::<ACE_HEADER>();
                assert_eq!(header.AceType, 0); // ACCESS_ALLOWED_ACE_TYPE
                assert_ne!(header.AceFlags & INHERITED_ACE as u8, 0);
                let ace = &*ace.cast::<ACCESS_ALLOWED_ACE>();
                assert_eq!(ace.Mask, FILE_ALL_ACCESS);
            }
        }
    }

    #[test]
    fn explicit_integrity_label_survives_publication() {
        let root = tempfile::tempdir().unwrap();
        let dest = root.path().join("out");
        let source = root.path().join("staged");
        drop(create_staging_file(&dest).unwrap());
        drop(create_staging_file(&source).unwrap());
        // SetSecurityInfo also queries the current security descriptor.
        let file = open(
            &dest,
            READ_CONTROL | WRITE_OWNER,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        )
        .unwrap();
        let descriptor = Local::descriptor("S:(ML;;NW;;;LW)").unwrap();
        let mut sacl = null_mut();
        let (mut present, mut defaulted) = (0, 0);
        // SAFETY: The descriptor owns the SACL until the synchronous setter returns.
        unsafe {
            checked(GetSecurityDescriptorSacl(
                descriptor.0,
                &mut present,
                &mut sacl,
                &mut defaulted,
            ))
            .unwrap();
            status(SetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                LABEL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                null_mut(),
                sacl,
            ))
            .unwrap();
        }
        drop(file);
        let original = Security::read(&dest).unwrap();
        assert!(original.sddl.contains("(ML;;NW;;;LW)"));
        let file = prepare(&source, Some(&original)).unwrap();
        let dest = fs::canonicalize(&dest).unwrap();
        publish(&source, &dest, true, &file).unwrap();
        drop(file);
        assert_eq!(Security::read(&dest).unwrap(), original);
    }
}
