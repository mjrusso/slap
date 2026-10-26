use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Debug, PartialEq)]
enum Filesystem {
    Local,
    Remote(&'static str),
    Uncertain(String),
}

pub(crate) fn inspect(directory: &File, root: &Path) -> io::Result<()> {
    check(filesystem(directory)?, root)
}

fn check(filesystem: Filesystem, root: &Path) -> io::Result<()> {
    match filesystem {
        Filesystem::Local => Ok(()),
        Filesystem::Remote(name) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("local store {} uses {name}: network filesystems do not support Slap's local mutation-lock assumptions", root.display()),
        )),
        Filesystem::Uncertain(name) => {
            static WARNED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
            let mut roots = WARNED.get_or_init(Default::default).lock().unwrap();
            if roots.insert(root.to_owned()) {
                log::warn!("local store {} uses {name}; filesystem type does not establish support for cross-process mutation locks or synchronization", root.display());
            }
            Ok(())
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn filesystem(directory: &File) -> io::Result<Filesystem> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // fstatfs writes this buffer on success; directory owns a valid descriptor.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // The successful fstatfs call initialized every field read below.
    let stat = unsafe { stat.assume_init() };
    #[cfg(target_os = "linux")]
    return Ok(linux_filesystem(stat.f_type as u64));
    #[cfg(target_os = "macos")]
    {
        // f_fstypename is a NUL-terminated kernel-provided filesystem name.
        let name = unsafe { std::ffi::CStr::from_ptr(stat.f_fstypename.as_ptr()) };
        Ok(macos_filesystem(
            &name.to_string_lossy(),
            stat.f_flags & libc::MNT_LOCAL as u32 != 0,
        ))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn filesystem(_directory: &File) -> io::Result<Filesystem> {
    Ok(Filesystem::Uncertain(std::env::consts::OS.into()))
}

#[cfg(any(target_os = "linux", test))]
fn linux_filesystem(kind: u64) -> Filesystem {
    match kind {
        0x6969 => Filesystem::Remote("NFS"),
        0xff534d42 => Filesystem::Remote("CIFS/SMB"),
        0xfe534d42 => Filesystem::Remote("SMB2"),
        0x517b => Filesystem::Remote("SMB"),
        0x5346414f => Filesystem::Remote("AFS"),
        0x00c36400 => Filesystem::Remote("Ceph"),
        // ext2/ext3/ext4, XFS, Btrfs, tmpfs and ZFS.
        0xef53 | 0x58465342 | 0x9123683e | 0x01021994 | 0x2fc12fc1 => Filesystem::Local,
        0x65735546 => Filesystem::Uncertain("FUSE".into()),
        0x794c7630 => Filesystem::Uncertain("overlayfs".into()),
        other => Filesystem::Uncertain(format!("filesystem type {other:#x}")),
    }
}

#[cfg(any(target_os = "macos", test))]
fn macos_filesystem(name: &str, local: bool) -> Filesystem {
    match name {
        "nfs" => Filesystem::Remote("NFS"),
        "smbfs" | "cifs" => Filesystem::Remote("SMB/CIFS"),
        "afpfs" => Filesystem::Remote("AFP"),
        "webdav" => Filesystem::Remote("WebDAV"),
        "apfs" | "hfs" if local => Filesystem::Local,
        _ => Filesystem::Uncertain(format!("{name} (MNT_LOCAL={local})")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_network_filesystems_and_leave_ambiguous_types_unconfirmed() {
        for filesystem in [
            linux_filesystem(0x6969),
            linux_filesystem(0xff534d42),
            macos_filesystem("smbfs", true),
            macos_filesystem("nfs", false),
        ] {
            let error = check(filesystem, Path::new("/store/root")).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(error.to_string().contains("/store/root"));
            assert!(error.to_string().contains("mutation-lock assumptions"));
        }
        for filesystem in [
            linux_filesystem(0x65735546),
            linux_filesystem(0x794c7630),
            linux_filesystem(123),
            macos_filesystem("apfs", false),
            macos_filesystem("macfuse", true),
        ] {
            assert!(matches!(filesystem, Filesystem::Uncertain(_)));
            check(filesystem, Path::new("/ambiguous")).unwrap();
        }
        assert_eq!(linux_filesystem(0xef53), Filesystem::Local);
        assert_eq!(macos_filesystem("apfs", true), Filesystem::Local);
    }
}
