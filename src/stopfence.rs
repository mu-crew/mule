//! The stop fence: one generation number per job id, in mule's LOCAL state.
//!
//! A lease's signal path hands the stop to a detached `mule rm --retry-for`,
//! which may outlive the pane by minutes while it waits for a master. If mu
//! reattaches a new carrier to the same job in that time (`run --lease
//! --attach`), the old child must not stop it. A cancel marker cannot say
//! that: removed by attach, it re-arms a sleeping old child; left in place, it
//! cancels the next carrier's stop of the same id (2026-11-04 lease-adopt
//! spec, review item mule 2). A generation can.
//!
//! - The stop child is told the generation its carrier saw (`--fence-gen`) and
//!   holds the fence's `flock` for each whole `rm` attempt. A different
//!   generation under the lock means the job was reattached: it stops nothing.
//! - Attach takes the same lock (waiting out an attempt in flight) and bumps
//!   the generation only once it has proven the job running. A later stop by
//!   the attached carrier reads the bumped number, so the fence never disarms
//!   a future stop.
//! - The file is unlinked only after an `rm` proved the job gone. Unlinking a
//!   live flock file lets two holders in (the trap `forward.rs` notes), so it
//!   is never done while the job may run.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::wrapper::JobId;

/// Where one job's fence lives.
#[derive(Debug, Clone)]
pub struct StopFence {
    path: PathBuf,
}

impl StopFence {
    /// `<state_dir>/stopping/<id>`.
    pub fn for_job(id: &JobId) -> Self {
        Self::at(crate::lock::state_dir().join("stopping").join(id.as_str()))
    }

    /// A fence at an explicit path, so a test can hold or bump the fence of a
    /// child it drives without mutating the environment of a threaded binary.
    #[doc(hidden)]
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Block until this process holds the fence. Never a timeout: the holder
    /// is an `rm` attempt or an attach, and giving up would just run the race.
    pub fn lock(&self) -> Result<FenceLock> {
        self.flock(true)
            .map(|lock| lock.expect("a blocking lock is always taken"))
    }

    /// [`StopFence::lock`] without waiting: `None` while someone else holds it.
    /// For tests, which observe whether the fence is held.
    #[doc(hidden)]
    pub fn try_lock(&self) -> Result<Option<FenceLock>> {
        self.flock(false)
    }

    /// The generation without the lock, for a signal path that must not wait
    /// on a stop attempt in flight. Missing is 0.
    ///
    /// Safe to read unlocked because a bump only ever lengthens or rewrites
    /// the digits in place (see [`FenceLock::bump`]), and the one writer that
    /// matters -- attach -- bumps before the carrier can be signalled into a
    /// stop at all.
    pub fn generation_unlocked(&self) -> Result<u64> {
        match File::open(&self.path) {
            Ok(mut file) => read_generation(&mut file, &self.path),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error)
                .with_context(|| format!("cannot read stop fence {}", self.path.display())),
        }
    }

    fn flock(&self, wait: bool) -> Result<Option<FenceLock>> {
        unsafe extern "C" {
            fn flock(fd: i32, operation: i32) -> i32;
        }
        // LOCK_EX and LOCK_NB; the same values on Linux and macOS, as in
        // `lock.rs` and `forward.rs`.
        const LOCK_EX: i32 = 2;
        const LOCK_NB: i32 = 4;

        let dir = self.path.parent().expect("fence paths have a parent");
        std::fs::create_dir_all(dir)
            .with_context(|| format!("cannot create stop fence directory {}", dir.display()))?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
            .with_context(|| format!("cannot open stop fence {}", self.path.display()))?;
        let operation = if wait { LOCK_EX } else { LOCK_EX | LOCK_NB };
        loop {
            // SAFETY: `file` owns a valid fd for the duration of the call.
            if unsafe { flock(file.as_raw_fd(), operation) } == 0 {
                return Ok(Some(FenceLock {
                    file,
                    path: self.path.clone(),
                }));
            }
            let error = std::io::Error::last_os_error();
            match error.kind() {
                ErrorKind::Interrupted => {}
                ErrorKind::WouldBlock if !wait => return Ok(None),
                _ => {
                    return Err(error).with_context(|| {
                        format!("cannot lock stop fence {}", self.path.display())
                    });
                }
            }
        }
    }
}

/// The fence, held; released on drop (or by the kernel when the holder dies).
#[derive(Debug)]
pub struct FenceLock {
    file: File,
    path: PathBuf,
}

impl FenceLock {
    pub fn generation(&mut self) -> Result<u64> {
        read_generation(&mut self.file, &self.path)
    }

    /// Advance the generation and return the new one.
    ///
    /// Rewritten in place, never by rename: a rename puts a new inode at the
    /// path, and a waiter blocked on the old one would then lock a file nobody
    /// else sees. The number only grows, so the new digits are never shorter
    /// than the old and an unlocked reader cannot see an empty file.
    pub fn bump(&mut self) -> Result<u64> {
        let next = self.generation()? + 1;
        let text = next.to_string();
        (|| -> std::io::Result<()> {
            self.file.write_all_at(text.as_bytes(), 0)?;
            self.file.set_len(text.len() as u64)?;
            self.file.sync_data()
        })()
        .with_context(|| format!("cannot write stop fence {}", self.path.display()))?;
        Ok(next)
    }

    /// Remove the fence file while still holding it. Only after an `rm` proved
    /// the job gone: a waiter on the old inode then probes Missing, which is
    /// harmless.
    pub fn unlink(self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Err(error) if error.kind() != ErrorKind::NotFound => Err(error)
                .with_context(|| format!("cannot remove stop fence {}", self.path.display())),
            _ => Ok(()),
        }
    }
}

fn read_generation(file: &mut File, path: &Path) -> Result<u64> {
    let mut text = String::new();
    // Read from the start whatever the cursor: a held lock reads, bumps
    // (positioned writes), and may read again.
    let mut reader = &*file;
    std::io::Seek::rewind(&mut reader)
        .and_then(|_| reader.read_to_string(&mut text))
        .with_context(|| format!("cannot read stop fence {}", path.display()))?;
    let text = text.trim();
    if text.is_empty() {
        return Ok(0);
    }
    text.parse()
        .with_context(|| format!("invalid stop fence {}: {text:?}", path.display()))
}
