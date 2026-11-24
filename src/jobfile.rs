//! Job files: the one-line files mu polls for a job's handle (`--job-file`)
//! and its stop-failure record, published atomically and never through a
//! planted name.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::wrapper::JobId;

/// How many fresh temp names `write_job_file` tries before giving up. Each is
/// random, so a collision means somebody is planting names in the directory;
/// a bound turns that into an error instead of a spin.
const TEMP_ATTEMPTS: u32 = 16;

/// Write `id` to `path` as the job file: [`write_line_file`] with the id.
pub fn write_job_file(path: &Path, id: &JobId) -> Result<()> {
    write_line_file(path, id.as_str())
        .with_context(|| format!("cannot write job file {}", path.display()))
}

/// Write one `line` to `path` atomically: a fresh temp file in the same
/// directory (the line plus `\n`, fsynced), then a rename, then an fsync of
/// the directory. The job file and the stop-failure record share it.
///
/// Atomic because mu polls for these files and parses their one line: a
/// reader must never see a half-written handle. Durable because the job file
/// is the only pointer to the job if the lease is SIGKILLed a moment later.
///
/// The temp is created exclusively (`O_CREAT|O_EXCL`) and never through a
/// symlink (`O_NOFOLLOW`), under a random name, then `fchmod`ed to 0600. The
/// documented `--job-file /tmp/w1.job` sits in a world-writable directory, and
/// the old predictable `F.tmp<pid>` opened with create+truncate let a planted
/// symlink overwrite its target with the id and then become F itself, and let
/// a planted 0644 file lend its mode to F (both reproduced).
/// An existing path is never opened, truncated or followed: a name that is
/// taken is skipped for another.
pub fn write_line_file(path: &Path, line: &str) -> Result<()> {
    // Callers add the context (which file, and what it is for); one layer
    // here would print the path twice.
    let (dir, tmp) = write_temp(path, line)?;
    let renamed = std::fs::rename(&tmp, path)
        // The rename is only durable once the directory entry is.
        .and_then(|()| std::fs::File::open(&dir)?.sync_all());
    if renamed.is_err() {
        // Ours: created exclusively, so removing it removes nobody else's file.
        let _ = std::fs::remove_file(&tmp);
    }
    Ok(renamed?)
}

/// `line` plus `\n` in a fresh, fsynced, 0600 temp file beside `path`.
/// Returns the directory and the temp's path; the caller renames or links it.
fn write_temp(path: &Path, line: &str) -> Result<(PathBuf, PathBuf)> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path
        .file_name()
        .with_context(|| format!("{} has no file name", path.display()))?;
    let (tmp, mut file) = create_temp(&dir, name)?;
    let written = (|| -> std::io::Result<()> {
        // `mode` applies only at creation and is masked by the umask; the
        // explicit fchmod makes 0600 the mode whatever the umask.
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(format!("{line}\n").as_bytes())?;
        file.sync_all()
    })();
    if let Err(error) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(error.into());
    }
    Ok((dir, tmp))
}

/// What [`publish_job_file`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// There was no job file; this call created it.
    Written,
    /// The job file already named this job, and was left alone.
    Kept,
}

/// How many times [`publish_job_file`] goes round read, then link. Each lap
/// means another process created the file and someone removed it again before
/// the re-read; a bound turns a fight over the path into an error.
const PUBLISH_ATTEMPTS: u32 = 4;

/// Publish `id` as the job file for an attach, never replacing anything.
///
/// The file is mu's handle to the job, and a reattach must not quietly retarget
/// it (SPEC § Leases):
///
/// - missing: write a temp, then `link(temp, path)`, which fails with EEXIST
///   rather than replace a file that appeared since the read. On EEXIST the
///   file is read again and that read decides;
/// - naming this id: kept as it is;
/// - naming another id, empty, invalid or unreadable: an error, file untouched.
///
/// `rename` would be the wrong primitive: it replaces whatever won the race.
pub fn publish_job_file(path: &Path, id: &JobId) -> Result<Published> {
    publish_job_file_with(path, id, &mut || {})
}

/// [`publish_job_file`] with a hook between the read that found no file and
/// the link, so a test can plant a racing writer exactly there.
#[doc(hidden)]
pub fn publish_job_file_with(
    path: &Path,
    id: &JobId,
    before_link: &mut dyn FnMut(),
) -> Result<Published> {
    let refuse = |why: String| {
        anyhow::anyhow!(
            "job file {} {why}; refusing to attach job {id} over it",
            path.display()
        )
    };
    for _ in 0..PUBLISH_ATTEMPTS {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let named = text.trim();
                if named.is_empty() {
                    return Err(refuse("is empty".to_string()));
                }
                return match named.parse::<JobId>() {
                    Ok(other) if other == *id => Ok(Published::Kept),
                    Ok(other) => Err(refuse(format!("names another job, {other}"))),
                    Err(_) => Err(refuse(format!("holds no job id: {named:?}"))),
                };
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "cannot read job file {}; refusing to attach job {id}",
                        path.display()
                    )
                });
            }
        }
        before_link();
        let (dir, tmp) = write_temp(path, id.as_str())
            .with_context(|| format!("cannot write job file {}", path.display()))?;
        let linked = std::fs::hard_link(&tmp, path);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                std::fs::File::open(&dir)
                    .and_then(|dir| dir.sync_all())
                    .with_context(|| format!("cannot write job file {}", path.display()))?;
                return Ok(Published::Written);
            }
            // Somebody else published first: their file decides, on the
            // next read.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("cannot write job file {}", path.display()));
            }
        }
    }
    Err(refuse(
        "keeps appearing and disappearing under another writer".to_string(),
    ))
}

/// The stop-failure record beside `job_file`: `<job_file>.<id>.stop-failed`.
///
/// The path names the job, so a late write by an old stop child can never
/// overwrite -- or a late success delete -- a newer job's record under the
/// same name (cross-repo contract § 2).
pub fn stop_failed_path(job_file: &Path, id: &JobId) -> PathBuf {
    let mut path = job_file.as_os_str().to_os_string();
    path.push(format!(".{id}.stop-failed"));
    PathBuf::from(path)
}

/// Create a new, empty, private temp file beside the job file, under a name
/// nobody else holds.
fn create_temp(dir: &Path, name: &std::ffi::OsStr) -> std::io::Result<(PathBuf, std::fs::File)> {
    let mut last = None;
    for _ in 0..TEMP_ATTEMPTS {
        let mut tmp_name = std::ffi::OsString::from(".");
        tmp_name.push(name);
        tmp_name.push(format!(".tmp{}-{:016x}", std::process::id(), random_u64()));
        let tmp = dir.join(tmp_name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW)
            .mode(0o600)
            .open(&tmp)
        {
            Ok(file) => return Ok((tmp, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => last = Some(error),
            Err(error) => return Err(error),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("no temp name tried")))
}

/// Unpredictable enough that a temp name cannot be planted ahead of time:
/// `RandomState` is seeded from the OS per process and per instance. Not a
/// secret -- `O_EXCL|O_NOFOLLOW` are what make a planted name harmless; the
/// randomness only keeps a planter from forcing every retry to collide.
fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    hasher.finish()
}
