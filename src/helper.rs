//! Local helpers only. On macOS a 70-entry PATH cost 54 native spawn attempts
//! per SSH check; resolving once keeps failed PATH probes out of the spawn path.

use std::ffi::{CString, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{LazyLock, OnceLock};

pub static SSH: Helper = Helper::new("ssh");
pub static SSH_ADD: Helper = Helper::new("ssh-add");
pub static PS: Helper = Helper::new("ps");
pub static MULE: Helper = Helper::new("mule");
static MU: Helper = Helper::new("mu");

struct SearchPath {
    explicit: bool,
    dirs: Vec<PathBuf>,
}

static SEARCH: LazyLock<SearchPath> = LazyLock::new(|| {
    let path = std::env::var_os("PATH");
    let cwd = std::env::current_dir().ok();
    SearchPath {
        explicit: path.is_some(),
        // The exec search default on macOS and Linux when PATH is unset.
        dirs: std::env::split_paths(path.as_deref().unwrap_or(OsStr::new("/usr/bin:/bin")))
            .filter_map(|dir| {
                if dir.is_absolute() {
                    Some(dir)
                } else {
                    Some(cwd.as_ref()?.join(dir))
                }
            })
            .collect(),
    }
});

/// Capture PATH (including the meaning of relative entries) before dispatch.
/// Resolve individual helpers only if used, including in library callers.
pub fn snapshot_path() {
    LazyLock::force(&SEARCH);
}

pub(crate) fn mu_on_path() -> Option<&'static Path> {
    // `mule init` skips registry discovery when PATH is unset.
    SEARCH.explicit.then(|| MU.path().ok()).flatten()
}

pub struct Helper {
    name: &'static str,
    resolved: OnceLock<Result<PathBuf, i32>>,
}

impl Helper {
    const fn new(name: &'static str) -> Self {
        Self {
            name,
            resolved: OnceLock::new(),
        }
    }

    pub fn path(&self) -> io::Result<&Path> {
        self.resolved
            .get_or_init(|| resolve(&SEARCH.dirs, self.name))
            .as_deref()
            .map_err(|&errno| io::Error::from_raw_os_error(errno))
    }

    pub fn command(&self) -> io::Result<Command> {
        self.path().map(Command::new)
    }
}

fn resolve(dirs: &[PathBuf], name: &str) -> Result<PathBuf, i32> {
    let mut missing = libc::ENOENT;
    for dir in dirs {
        let candidate = dir.join(name);
        match check_executable(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(libc::ENOENT | libc::ENOTDIR) => (),
            Err(libc::EACCES) => missing = libc::EACCES,
            Err(errno) => return Err(errno),
        }
    }
    Err(missing)
}

fn check_executable(path: &Path) -> Result<(), i32> {
    let errno = |err: io::Error| err.raw_os_error().unwrap_or(libc::EINVAL);
    if !std::fs::metadata(path).map_err(errno)?.is_file() {
        return Err(libc::EACCES);
    }
    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)?;
    // Unlike a mode-bit test, this checks execute permission for this process.
    // SAFETY: path is a live NUL-terminated C string.
    if unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), libc::X_OK, libc::AT_EACCESS) } == 0
    {
        Ok(())
    } else {
        Err(errno(io::Error::last_os_error()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn resolver_picks_first_executable_regular_file() -> io::Result<()> {
        let root = std::env::temp_dir().join(format!("mule-resolver-{}", std::process::id()));
        fs::create_dir(&root)?;
        struct Remove(PathBuf);
        impl Drop for Remove {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _remove = Remove(root.clone());
        let dirs: Vec<_> = ["missing", "dir", "noexec", "shim", "later"]
            .map(|name| root.join(name))
            .into();
        for dir in &dirs[1..] {
            fs::create_dir(dir)?;
        }
        fs::create_dir(dirs[1].join("helper"))?;
        fs::write(dirs[2].join("helper"), b"not executable")?;
        fs::set_permissions(dirs[2].join("helper"), fs::Permissions::from_mode(0o644))?;
        fs::write(dirs[4].join("helper"), b"#!/bin/sh\nexit 0\n")?;
        fs::set_permissions(dirs[4].join("helper"), fs::Permissions::from_mode(0o755))?;
        let shim = dirs[3].join("helper");
        symlink("../later/helper", &shim)?;

        assert_eq!(resolve(&dirs, "helper"), Ok(shim));
        assert_eq!(resolve(&dirs[..3], "helper"), Err(libc::EACCES));
        assert_eq!(resolve(&dirs, "absent"), Err(libc::ENOENT));
        Ok(())
    }
}
