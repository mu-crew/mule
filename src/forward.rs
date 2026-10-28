//! `mule forward` / `mule unforward`: unix-socket forwards over mule's master.
//!
//! A forward is a `direct-streamlocal` channel, not a session, so it rides the
//! master without touching the `MaxSessions` slot (measured during a held
//! session, three concurrent forwarded connections included). That is why
//! nothing here takes the ticket lock: every call is a [`MuxOp`], and
//! `Transport::mux_op` is the lock-free path.
//!
//! The reason this file is longer than "run `ssh -O forward`" is that OpenSSH's
//! answers cannot be trusted (SPEC § Leases, measured on a loopback capped
//! sshd):
//!
//! - an exit code from `-O forward` or `-O cancel` proves nothing (`cancel` of a
//!   forward that is gone prints `port not found` and exits 0);
//! - file existence proves nothing (`cancel` leaves the file; a dead master
//!   leaves it too);
//! - `connect(LOCAL)` succeeding proves a live listener, even before REMOTE
//!   exists, because REMOTE is resolved per connection;
//! - one LOCAL forwarded to two REMOTEs **wedges** the master's forward table:
//!   both listeners close, the table still lists one, and from then on `cancel`
//!   says `port not found` and `forward` exits 0 and binds nothing. Only a
//!   master restart recovers.
//!
//! So `forward` verifies by `connect`, and keeps a small record per LOCAL so a
//! changed REMOTE cancels the *old* spec first. OpenSSH has no `-O list`; the
//! record is forward state for the master mule owns, not a job index.

use std::io::Write;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};

use crate::config::Host;
use crate::errors::{MuleError, no_master};
use crate::transport::{MuxOp, Transport};

/// One `LOCAL:REMOTE` unix-socket pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub local: PathBuf,
    pub remote: String,
}

impl Spec {
    /// Parse `LOCAL:REMOTE`.
    ///
    /// `:` is ssh's own separator in `-L`, and ssh has no escape for it, so a
    /// path containing one cannot be forwarded and is refused here rather than
    /// being silently split into a different pair. A newline would break the
    /// line-oriented record. LOCAL is made absolute: the master binds it, and
    /// resolves a relative path against *its* working directory, not ours.
    pub fn parse(spec: &str) -> Result<Self> {
        let (local, remote) = spec
            .split_once(':')
            .ok_or_else(|| anyhow!("expected LOCAL:REMOTE, got {spec:?}"))?;
        if local.is_empty() || remote.is_empty() {
            bail!("expected LOCAL:REMOTE, got {spec:?}");
        }
        if remote.contains(':') {
            bail!("a socket path cannot contain ':' (ssh splits -L on it): {spec:?}");
        }
        if spec.contains('\n') || spec.contains('\0') {
            bail!("a socket path cannot contain a newline or NUL: {spec:?}");
        }
        let local = Path::new(local);
        let local = if local.is_absolute() {
            local.to_path_buf()
        } else {
            std::env::current_dir()
                .context("cannot resolve a relative LOCAL")?
                .join(local)
        };
        Ok(Self {
            local,
            remote: remote.to_string(),
        })
    }

    /// Parse every spec of one invocation, refusing one LOCAL twice: that is
    /// the wedge, made by a single command line.
    pub fn parse_all(specs: &[String]) -> Result<Vec<Self>> {
        let mut out: Vec<Self> = Vec::new();
        for spec in specs {
            let spec = Self::parse(spec)?;
            if out.iter().any(|seen| seen.local == spec.local) {
                bail!(
                    "{} is forwarded twice; one LOCAL must never go to two REMOTEs \
                     (it wedges the master's forward table)",
                    spec.local.display()
                );
            }
            out.push(spec);
        }
        Ok(out)
    }

    fn mux(&self) -> (PathBuf, String) {
        (self.local.clone(), self.remote.clone())
    }
}

/// What `forward` remembers about a LOCAL, to make the wedge avoidable and
/// drift detectable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub local: PathBuf,
    pub remote: String,
    /// The master that holds the forward. A replaced master has none of the
    /// old forwards, so a different pid means the record describes nothing.
    pub master_pid: Option<u32>,
    /// LOCAL's identity after binding: a changed inode means someone replaced
    /// the file and the listener is not ours.
    pub dev: u64,
    pub ino: u64,
}

impl Record {
    fn render(&self) -> String {
        format!(
            "local={}\nremote={}\npid={}\ndev={}\nino={}\n",
            self.local.display(),
            self.remote,
            self.master_pid.map(|p| p.to_string()).unwrap_or_default(),
            self.dev,
            self.ino
        )
    }

    fn parse(text: &str) -> Option<Self> {
        let field = |key: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(key)?.strip_prefix('='))
        };
        Some(Self {
            local: PathBuf::from(field("local")?),
            remote: field("remote")?.to_string(),
            master_pid: field("pid")?.parse().ok(),
            dev: field("dev")?.parse().ok()?,
            ino: field("ino")?.parse().ok()?,
        })
    }
}

/// FNV-1a, 64 bit. Hand-rolled because the record name must be stable across
/// toolchains: `DefaultHasher` makes no such promise, and an unstable name
/// would orphan every record on a compiler upgrade.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// `~/.local/state/mule/forwards/<host>/<hash of LOCAL>`.
pub fn record_path(host: &Host, local: &Path) -> PathBuf {
    crate::lock::state_dir()
        .join("forwards")
        .join(&host.name)
        .join(format!(
            "{:016x}",
            fnv1a(local.as_os_str().as_encoded_bytes())
        ))
}

pub fn read_record(host: &Host, local: &Path) -> Option<Record> {
    let text = std::fs::read_to_string(record_path(host, local)).ok()?;
    Record::parse(&text).filter(|record| record.local == local)
}

fn write_record(host: &Host, record: &Record) -> Result<()> {
    let path = record_path(host, &record.local);
    let dir = path.parent().expect("record paths have a parent");
    std::fs::create_dir_all(dir)
        .with_context(|| format!("cannot create forward state {}", dir.display()))?;
    // Temp + rename, so a concurrent reader (the lease's drift check) never
    // sees half a record and mistakes it for "no record".
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    let mut file = std::fs::File::create(&tmp)
        .with_context(|| format!("cannot write forward record {}", tmp.display()))?;
    file.write_all(record.render().as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("cannot write forward record {}", path.display()))
}

fn delete_record(host: &Host, local: &Path) {
    let _ = std::fs::remove_file(record_path(host, local));
}

/// The master pid from `-O check`'s `Master running (pid=N)`. ssh prints it on
/// stderr; reading both streams keeps this independent of that detail.
pub fn master_pid(stdout: &str, stderr: &str) -> Option<u32> {
    [stderr, stdout].iter().find_map(|text| {
        let rest = &text[text.find("pid=")? + 4..];
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    })
}

/// Ask the master for its pid. `None` when there is no master.
pub fn check_master(t: &dyn Transport, host: &Host) -> Option<Option<u32>> {
    let out = t.mux_op(host, MuxOp::Check).ok()?;
    (out.code == 0).then(|| master_pid(&out.text(), &out.stderr))
}

/// Does `connect(local)` succeed? The only proof of a live listener.
pub fn listening(local: &Path) -> bool {
    UnixStream::connect(local).is_ok()
}

/// Unlink LOCAL only if it is a socket; refuse anything else.
///
/// The master leaves a stale socket file behind after `cancel`, and a stale
/// file makes a master without `StreamLocalBindUnlink` fail the next forward
/// (measured: 255). A regular file or a directory at that path is somebody's
/// data, and deleting it would be mule's worst possible surprise.
fn unlink_socket(local: &Path) -> Result<()> {
    match std::fs::symlink_metadata(local) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(local)
            .with_context(|| format!("cannot remove stale socket {}", local.display())),
        Ok(_) => bail!(
            "{} exists and is not a socket; refusing to remove it",
            local.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("cannot stat {}", local.display())),
    }
}

/// LOCAL's dev+inode, for the record and the drift check.
pub fn identity(local: &Path) -> Option<(u64, u64)> {
    let meta = std::fs::symlink_metadata(local).ok()?;
    Some((meta.dev(), meta.ino()))
}

fn cancel(t: &dyn Transport, host: &Host, local: PathBuf, remote: String) {
    // The exit code proves nothing, so it is not read: a cancel of a forward
    // that is gone exits 0, and one that fails leaves nothing to act on.
    let _ = t.mux_op(host, MuxOp::CancelUnix(local, remote));
}

/// Forward each spec. See the module header for why each step exists.
///
/// Exit mapping (via the error type): no master and a wedged table are exit 3,
/// the hand-back code; anything else is exit 1, after cancelling the forwards
/// this call added.
pub fn forward(t: &dyn Transport, host: &Host, specs: &[Spec]) -> Result<()> {
    let pid = check_master(t, host).ok_or_else(|| no_master(host))?;

    let mut added: Vec<&Spec> = Vec::new();
    for spec in specs {
        match forward_one(t, host, spec, pid) {
            Ok(()) => added.push(spec),
            Err(error)
                if matches!(
                    error.downcast_ref::<MuleError>(),
                    Some(MuleError::ForwardWedged { .. })
                ) =>
            {
                return Err(error);
            }
            Err(error) => {
                // Only what this call added: the caller asked for the set, and
                // a half-applied set is harder to reason about than none.
                for spec in added {
                    undo(t, host, spec);
                }
                return Err(error.context(format!(
                    "forwarding {}:{} failed",
                    spec.local.display(),
                    spec.remote
                )));
            }
        }
    }
    Ok(())
}

fn forward_one(t: &dyn Transport, host: &Host, spec: &Spec, pid: Option<u32>) -> Result<()> {
    // 1. Same LOCAL, different REMOTE, same master: the recorded forward is
    //    still in that master's table. Cancel it first or the new one wedges it.
    if let Some(old) = read_record(host, &spec.local)
        && old.remote != spec.remote
        && old.master_pid.is_some()
        && old.master_pid == pid
    {
        cancel(t, host, old.local, old.remote);
    }
    // 2. Clear an identical stale forward, so step 4 never doubles it.
    let (local, remote) = spec.mux();
    cancel(t, host, local.clone(), remote.clone());
    // 3. `cancel` leaves the file behind.
    unlink_socket(&spec.local)?;
    // 4.
    let out = t
        .mux_op(host, MuxOp::ForwardUnix(local, remote))
        .with_context(|| format!("asking the master to forward {}", spec.local.display()))?;
    if out.code != 0 {
        bail!(
            "the master refused the forward (exit {}): {}",
            out.code,
            out.stderr.trim()
        );
    }
    // 5. Exit 0 proved nothing; only a listener does.
    if !listening(&spec.local) {
        return Err(MuleError::ForwardWedged {
            host: host.name.clone(),
            local: spec.local.display().to_string(),
            exit: crate::errors::master_exit_command(host),
            open: crate::errors::master_command(host)
                .trim_start_matches("run: ")
                .to_string(),
        }
        .into());
    }
    // 6.
    let (dev, ino) = identity(&spec.local).context("the forward's socket vanished after bind")?;
    let record = Record {
        local: spec.local.clone(),
        remote: spec.remote.clone(),
        master_pid: pid,
        dev,
        ino,
    };
    write_record(host, &record).inspect_err(|_| undo(t, host, spec))
}

/// Cancel, unlink and forget one spec. Never fails: callers are cleanup paths.
fn undo(t: &dyn Transport, host: &Host, spec: &Spec) {
    cancel(t, host, spec.local.clone(), spec.remote.clone());
    // Not `unlink_socket`: a non-socket here is not ours, and is left alone.
    if std::fs::symlink_metadata(&spec.local).is_ok_and(|m| m.file_type().is_socket()) {
        let _ = std::fs::remove_file(&spec.local);
    }
    delete_record(host, &spec.local);
}

/// Undo each spec. Idempotent, and needs no master: `cancel` of a master that
/// is gone fails harmlessly, and the file and record are still ours to remove.
pub fn unforward(t: &dyn Transport, host: &Host, specs: &[Spec]) {
    for spec in specs {
        undo(t, host, spec);
    }
}
