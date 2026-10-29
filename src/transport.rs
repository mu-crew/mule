//! The seam between mule's logic and the ssh channel.
//!
//! Every invariant that matters was measured against a real capped host and is
//! unreachable from a plain unit test. This trait is what makes test layer 1
//! possible at all: `Fake` records the exact script mule would have run, so
//! wrapper construction and state mapping are tested as pure functions.
//!
//! One rule shapes the whole file: **`run` takes mule's ticket lock, and
//! `mux_op` does not.** `ssh -O check`, and `-O forward`/`-O cancel` of a local
//! unix forward, talk only to the mux socket and are handled in the master's
//! own loop. None opens a session channel -- measured during a held session,
//! and `check` at 0s -- so exempting them is safe, and having them be a
//! *different method* makes the exemption structural instead of a rule someone
//! has to remember.
//!
//! The exemption is exactly as wide as [`MuxOp`], a closed enum. It has no
//! free-form arguments and no remote (`-R`) forward, because a remote forward
//! needs a server round trip and so belongs inside the lock. Widening the
//! exemption means editing the enum, in review, not passing a different string.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::config::Host;
use crate::errors::MuleError;

/// How long one caller keeps re-queueing after proven pre-exec refusals.
///
/// A scope limit on the retry loop only -- backoff plus re-queued attempts --
/// not a bound on the call: the ticket wait before each attempt is
/// deliberately unbounded (SPEC § Contention), so a caller needing a hard
/// wall-clock limit imposes it itself, as mu does. 12s because mu's stop
/// template runs `mule rm` under a 20s budget, and recovery must leave room
/// inside it for the queue, the ssh and the removal. The refusal it absorbs
/// is a session held by a dead caller's still-running ssh: a lease TERMed
/// mid-dispatch, whose dispatch outlived its 3s grace, held it for seconds
/// under three parallel layer-3 suites and failed the next `mule rm`.
pub const REFUSAL_RECOVERY: Duration = Duration::from_secs(12);

/// First pause after a refusal; doubles to [`MAX_BACKOFF`]. A held session
/// frees when a remote script ends, so the first retries come quickly.
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Hard bound on one `ssh -O check`.
///
/// A stopped master (SIGSTOP, a wedged mux loop) answers nothing, and a plain
/// `-O check` then blocks with it: measured still blocked at 12.31s. Every
/// verb checks the master before it queues, and the recovery loop checks it
/// between attempts, so one wedged master would hang both. A live master
/// answers in ~0s (measured); 2s also covers a loaded machine, where spawning
/// an ssh for a mux op took up to 0.45s.
pub const CHECK_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Output {
    /// Raw bytes, never a lossy `String`.
    ///
    /// A job may emit a tarball, or simply invalid UTF-8, and the probe reply
    /// carries log bytes inside this field. `String::from_utf8_lossy` would
    /// silently substitute replacement characters — corrupting the one artifact
    /// the entire design treats as the source of truth. Text-only would have
    /// been a defensible scope cut; silent corruption is not.
    pub stdout: Vec<u8>,
    /// Text, because this is ssh's own diagnostics and `errors` pattern-matches
    /// them. A non-UTF-8 ssh error message is not a case worth carrying bytes
    /// for.
    pub stderr: String,
    pub code: i32,
}

impl Output {
    pub fn ok(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            stdout: stdout.into(),
            stderr: String::new(),
            code: 0,
        }
    }

    /// The stdout as text, for the many call sites parsing a known-ASCII reply
    /// (`rc=0`, a session count). Lossy on purpose and only here: these fields
    /// are mule's own output, not the user's.
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }

    pub fn fail(code: i32, stderr: impl Into<String>) -> Self {
        Self {
            stdout: Vec::new(),
            stderr: stderr.into(),
            code,
        }
    }
}

/// The mux operations that bypass the ticket lock.
///
/// Closed on purpose: see the module header. Each is answered by the master
/// itself without opening a session channel, so it cannot contend with a job
/// verb for the `MaxSessions` slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MuxOp {
    /// `-O check`: is there a master, and which pid owns it.
    Check,
    /// `-O forward -L LOCAL:REMOTE` for unix sockets. LOCAL is bound by the
    /// master, so it must be absolute: a relative path would resolve against
    /// the master's working directory, not the caller's.
    ForwardUnix(PathBuf, String),
    /// `-O cancel -L LOCAL:REMOTE`.
    CancelUnix(PathBuf, String),
}

pub trait Transport {
    /// Run a shell script on the host, WITHOUT taking the ticket lock.
    ///
    /// Callers outside this module want [`Transport::run`], which is the same
    /// thing with the lock held. This is the unlocked primitive an
    /// implementation provides; calling it directly opens a session channel
    /// that mule's own fairness gate cannot see, which on a `MaxSessions 1`
    /// host recreates the contention the whole tool exists to remove.
    fn run_unlocked(&self, host: &Host, script: &str) -> Result<Output>;

    /// Run one mux operation over the master, WITHOUT the ticket lock.
    ///
    /// This is the only lock-free path to the host, and [`MuxOp`] is what keeps
    /// it honest: every variant is handled in the master's mux loop and opens
    /// no session channel (measured during a held session). The exit code of a
    /// forward or cancel proves nothing -- callers verify by effect.
    fn mux_op(&self, host: &Host, op: MuxOp) -> Result<Output>;

    /// Is there a usable multiplexing socket? `-O check` only, so this probe
    /// never competes with anything.
    fn master_alive(&self, host: &Host) -> bool {
        self.mux_op(host, MuxOp::Check)
            .is_ok_and(|output| output.code == 0)
    }

    /// Run a shell script on the host, holding the host's ticket lock.
    ///
    /// **This is the only way callers should reach a host.** The lock covers
    /// every ssh mule issues except the [`MuxOp`]s, because two concurrent
    /// reads hit exactly the cap that motivated the tool -- but until now that
    /// was prose, enforced by six call sites each remembering to wrap
    /// `run_unlocked` in `with_lock`. A seventh that forgot would compile,
    /// pass every test, and quietly reintroduce the contention.
    ///
    /// Provided rather than required, so no implementation can weaken it: the
    /// lock is applied here, once, and an implementor supplies only the
    /// unlocked primitive.
    ///
    /// One exception to "one ticket, one attempt": a session refused before
    /// the script started (see [`run_within`]) is retried on a fresh ticket.
    fn run(&self, host: &Host, script: &str) -> Result<Output> {
        run_within(self, host, script, REFUSAL_RECOVERY)
    }
}

/// [`Transport::run`] with the refusal-recovery budget as a parameter, so a
/// test can pin the bound without spending 12s.
///
/// Each attempt takes its own ticket and holds it only for the attempt. On
/// [`MuleError::MuxRefusedPreExec`] -- proven from ssh's own log, nothing on
/// stdout, so the script never ran -- and only while `-O check` still finds
/// the master, the ticket is released, the caller backs off, and re-queues
/// at the BACK. Holding the ticket through the backoff would stall every
/// caller behind a session none of them can get; re-queueing first means
/// later callers may complete first, so FIFO holds per attempt, not per call
/// (SPEC § Contention, recovery exception).
///
/// The budget bounds only this loop: once the time left is shorter than the
/// next backoff it stops and reports [`MuleError::SessionChannelBusy`], which
/// keeps `rm`'s exit 1 (the job is not proven stopped) for its caller.
#[doc(hidden)]
pub fn run_within<T: Transport + ?Sized>(
    t: &T,
    host: &Host,
    script: &str,
    budget: Duration,
) -> Result<Output> {
    let deadline = Instant::now() + budget;
    let mut backoff = FIRST_BACKOFF;
    loop {
        let result = crate::lock::with_lock(&host.name, || t.run_unlocked(host, script))?;
        let refused = matches!(
            result
                .as_ref()
                .err()
                .and_then(|e| e.downcast_ref::<MuleError>()),
            Some(MuleError::MuxRefusedPreExec)
        );
        if !refused {
            return result;
        }
        // Lock-free: `-O check` opens no session. A master that is gone, or
        // too wedged to answer within CHECK_TIMEOUT, will not hand out a
        // session however long we wait.
        if !t.master_alive(host) || deadline.saturating_duration_since(Instant::now()) < backoff {
            return Err(MuleError::SessionChannelBusy.into());
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Every ssh mule runs, configured so it can never prompt.
///
/// `BatchMode=yes` alone is not enough. It gags *ssh's* own prompts, but a
/// `ProxyCommand` is a separate program with its own terminal: a site wrapper
/// doing 2FA (`ProxyCommand x2ssh ...`) prompts regardless, so every mule call
/// on a host with a dead master spawned a Duo passcode prompt into the user's
/// terminal -- repeatedly, since mule is expected to be called often, and with
/// no indication of which invocation was asking.
///
/// Three settings close it:
///   BatchMode=yes                - ssh itself never asks
///   ControlMaster=no             - never create a master as a side effect;
///                                  mule requires one and refuses otherwise
///   ProxyCommand=/usr/bin/false  - never reach the host except through the
///                                  master
///
/// The proxy setting is what makes every mule ssh FAIL CLOSED. When the mux
/// client cannot get a session -- the master refused one, or is gone -- ssh
/// falls back to a fresh connection of its own. `ProxyCommand=none` (mule's
/// earlier setting) let that fallback dial the host directly, and where keys
/// alone authenticate it ran the script outside the master: measured on a
/// capped loopback sshd, a refused call executed its script anyway, while
/// mule reported the refusal. That is a session the ticket lock cannot see,
/// and a refusal mule cannot safely retry. `/usr/bin/false` kills the
/// fallback at once (no prompt, no connect, no execution: measured, a site
/// `ProxyCommand` never started), and a command-line ProxyCommand also wins
/// over a configured `ProxyJump` (`ssh -G`). Multiplexing over the master is
/// unaffected: the socket is already connected, so no proxy is consulted. The
/// master the user opens by hand keeps its own ProxyCommand, which is where
/// 2FA belongs -- once per ControlPersist window, with the user watching.
fn base_args(host: &Host) -> Vec<String> {
    vec![
        "-S".into(),
        host.socket.display().to_string(),
        "-o".into(),
        "BatchMode=yes".into(),
        "-o".into(),
        "ControlMaster=no".into(),
        "-o".into(),
        "ProxyCommand=/usr/bin/false".into(),
        host.target.clone(),
    ]
}

/// Arguments for a lock-exempt mux op. Exposed for tests, which assert that no
/// mule invocation can prompt and that paths reach ssh as argv.
///
/// Paths are single argv elements: `-O` runs no remote shell, so nothing here
/// is ever re-parsed. (`:` in a path is rejected earlier, where the spec is
/// parsed, because ssh itself splits `-L` on it.)
pub fn mux_args(host: &Host, op: &MuxOp) -> Vec<String> {
    let mut args = base_args(host);
    match op {
        MuxOp::Check => args.extend(["-O".into(), "check".into()]),
        MuxOp::ForwardUnix(local, remote) => args.extend([
            "-O".into(),
            "forward".into(),
            "-L".into(),
            format!("{}:{remote}", local.display()),
        ]),
        MuxOp::CancelUnix(local, remote) => args.extend([
            "-O".into(),
            "cancel".into(),
            "-L".into(),
            format!("{}:{remote}", local.display()),
        ]),
    }
    args
}

/// Arguments for the lock-exempt master probe.
pub fn probe_args(host: &Host) -> Vec<String> {
    mux_args(host, &MuxOp::Check)
}

/// Arguments for running a script over the master.
pub fn run_args(host: &Host, script: &str) -> Vec<String> {
    let mut args = base_args(host);
    args.push(script.to_string());
    args
}

fn ssh_agent_state() -> crate::errors::AgentState {
    match Command::new("ssh-add").arg("-l").output() {
        Ok(output) if output.status.success() => crate::errors::AgentState::Keys,
        Ok(output) if output.status.code() == Some(1) => crate::errors::AgentState::NoKeys,
        Ok(output) if output.status.code() == Some(2) => crate::errors::AgentState::Unreachable,
        Ok(_) | Err(_) => crate::errors::AgentState::Unknown,
    }
}

/// The real thing.
#[derive(Debug, Default, Clone, Copy)]
pub struct Ssh;

impl Transport for Ssh {
    fn run_unlocked(&self, host: &Host, script: &str) -> Result<Output> {
        // ssh's own diagnostics go to a private file (`-E`), apart from the
        // remote script's stderr: only they may drive a retry. See
        // `errors::classify_attempt` for the spoof that made this necessary.
        let log = SshLog::create()?;
        let mut cmd = Command::new("ssh");
        cmd.arg("-E").arg(&log.0).args(run_args(host, script));
        let out = cmd
            .output()
            .with_context(|| format!("spawning ssh for host {}", host.name))?;
        let ssh_log = std::fs::read_to_string(&log.0).unwrap_or_default();
        if let Some(error) = crate::errors::classify_attempt(&ssh_log, &out.stdout, ssh_agent_state)
        {
            return Err(error.into());
        }
        // Callers quote stderr in their failures, and before `-E` it carried
        // ssh's diagnostics too (a lost master, a dropped connection): keep
        // them, after the remote's own text.
        let mut stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        stderr.push_str(&ssh_log);
        Ok(Output {
            stdout: out.stdout,
            stderr,
            code: out.status.code().unwrap_or(-1),
        })
    }

    fn mux_op(&self, host: &Host, op: MuxOp) -> Result<Output> {
        let mut cmd = Command::new("ssh");
        cmd.args(mux_args(host, &op));
        let out = if op == MuxOp::Check {
            match output_within(&mut cmd, CHECK_TIMEOUT)
                .with_context(|| format!("spawning ssh for host {}", host.name))?
            {
                Some(out) => out,
                None => {
                    return Ok(Output::fail(
                        255,
                        format!(
                            "ssh -O check did not answer within {}s",
                            CHECK_TIMEOUT.as_secs()
                        ),
                    ));
                }
            }
        } else {
            cmd.output()
                .with_context(|| format!("spawning ssh for host {}", host.name))?
        };
        Ok(Output {
            stdout: out.stdout,
            // Not classified: a mux op opens no session, so the refused-channel
            // messages `classify` hunts for cannot come from it. ssh prints
            // `-O check`'s `Master running (pid=N)` here, and callers read it.
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        })
    }
}

/// Run `cmd` to completion, or kill it at `limit` and return `None`.
///
/// For replies of a few bytes only: the pipes are drained after exit, so an
/// output larger than a pipe buffer would stall the child until the limit.
fn output_within(
    cmd: &mut Command,
    limit: Duration,
) -> std::io::Result<Option<std::process::Output>> {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + limit;
    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output().map(Some);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(None);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// One call's private `ssh -E` file, removed on drop.
///
/// Created exclusively (`create_new`, so a planted file or symlink fails the
/// call rather than being written through) and 0600, under a name no other
/// call can choose.
struct SshLog(PathBuf);

impl SshLog {
    fn create() -> Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let path = std::env::temp_dir().join(format!(
            "mule-ssh-{}-{}-{nanos}.log",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&path)
            .with_context(|| format!("cannot create ssh log {}", path.display()))?;
        Ok(Self(path))
    }
}

impl Drop for SshLog {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A transport whose locked calls a signal path can stop and wait out.
///
/// For a process that exits on a signal mid-call: killing a local ssh does
/// not free the session it holds -- the remote command keeps the channel
/// until it ends (measured: a SIGKILLed `sleep 3` holder left the channel
/// refused for 2.99s). The ticket, though, is released by the caller's death.
/// So a caller that exits mid-call hands the next ticket holder a channel
/// that is still taken, and that holder is refused.
///
/// [`Stopper::stop`] closes the gate -- no new call starts -- and waits for the
/// one in flight. Both halves matter: a signal path that only waited could see
/// nothing in flight, then lose to a main thread starting its next probe while
/// the path was busy elsewhere, and exit with that probe holding the session.
///
/// It lives here, not with its user, because it is a `Transport`: delegating
/// `run_unlocked` is safe only inside the provided `run`, which holds the lock.
pub struct InFlight<T> {
    inner: T,
    state: std::sync::Arc<std::sync::atomic::AtomicU8>,
}

const GATE_IDLE: u8 = 0;
const GATE_BUSY: u8 = 1;
const GATE_STOPPED: u8 = 2;

impl<T: Transport> InFlight<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            state: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(GATE_IDLE)),
        }
    }

    pub fn stopper(&self) -> Stopper {
        Stopper(std::sync::Arc::clone(&self.state))
    }
}

impl<T: Transport> Transport for InFlight<T> {
    fn run_unlocked(&self, host: &Host, script: &str) -> Result<Output> {
        use std::sync::atomic::Ordering;
        if self
            .state
            .compare_exchange(GATE_IDLE, GATE_BUSY, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            anyhow::bail!("not started: mule is stopping");
        }
        let out = self.inner.run_unlocked(host, script);
        // Fails only if `stop` gave up waiting and closed the gate anyway.
        let _ =
            self.state
                .compare_exchange(GATE_BUSY, GATE_IDLE, Ordering::SeqCst, Ordering::SeqCst);
        out
    }

    fn mux_op(&self, host: &Host, op: MuxOp) -> Result<Output> {
        self.inner.mux_op(host, op)
    }
}

/// The signal path's handle on an [`InFlight`].
#[derive(Clone)]
pub struct Stopper(std::sync::Arc<std::sync::atomic::AtomicU8>);

impl Stopper {
    /// Refuse every new locked call, and wait up to `grace` for the one in
    /// flight. True when nothing is left running. On a timeout the gate is
    /// closed anyway: the caller is about to exit and must not start more.
    pub fn stop(&self, grace: std::time::Duration) -> bool {
        use std::sync::atomic::Ordering;
        let deadline = std::time::Instant::now() + grace;
        loop {
            match self.0.compare_exchange(
                GATE_IDLE,
                GATE_STOPPED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(GATE_STOPPED) => return true,
                Err(_) if std::time::Instant::now() >= deadline => {
                    self.0.store(GATE_STOPPED, Ordering::SeqCst);
                    return false;
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(10)),
            }
        }
    }
}

/// A recording transport for test layer 1.
///
/// Deliberately **not** `#[cfg(test)]`: integration tests live in their own
/// crate and could not see it otherwise, and layer 1 is where most of mule's
/// logic is actually verified.
pub struct Fake {
    scripts: std::sync::Mutex<Vec<String>>,
    outputs: std::sync::Mutex<std::collections::VecDeque<Output>>,
    mux_ops: std::sync::Mutex<Vec<MuxOp>>,
    mux_outputs: std::sync::Mutex<std::collections::VecDeque<Output>>,
    mux_hook: std::sync::Mutex<Option<MuxHook>>,
    // Atomics so a test can take the master away and bring it back under a new
    // pid mid-run, as `ssh -O exit` and a human's `ssh -MNf` would.
    master: std::sync::atomic::AtomicBool,
    master_pid: std::sync::atomic::AtomicU32,
}

/// Runs on every `mux_op`, so a test can play the master: bind the LOCAL a
/// forward names, or refuse to.
type MuxHook = Box<dyn Fn(&MuxOp) + Send + Sync>;

impl std::fmt::Debug for Fake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fake")
            .field("scripts", &self.scripts)
            .field("mux_ops", &self.mux_ops)
            .field("master", &self.master)
            .finish_non_exhaustive()
    }
}

impl Default for Fake {
    fn default() -> Self {
        Self {
            scripts: Default::default(),
            outputs: Default::default(),
            mux_ops: Default::default(),
            mux_outputs: Default::default(),
            mux_hook: Default::default(),
            master: false.into(),
            master_pid: 4242.into(),
        }
    }
}

impl Fake {
    /// A fake with a live master and no queued output (every `run` yields an
    /// empty success).
    pub fn new() -> Self {
        Self {
            master: true.into(),
            ..Default::default()
        }
    }

    /// A fake whose master is down, for the exit-3 path.
    pub fn no_master() -> Self {
        Self::default()
    }

    /// Queue one reply. Replies are consumed in order.
    pub fn push(&self, out: Output) -> &Self {
        self.outputs.lock().unwrap().push_back(out);
        self
    }

    /// Queue one reply for a non-`Check` mux op. Consumed in order; once the
    /// queue is empty every op succeeds.
    pub fn push_mux(&self, out: Output) -> &Self {
        self.mux_outputs.lock().unwrap().push_back(out);
        self
    }

    /// Install the master's side effects for forward/cancel.
    pub fn on_mux(&self, hook: impl Fn(&MuxOp) + Send + Sync + 'static) -> &Self {
        *self.mux_hook.lock().unwrap() = Some(Box::new(hook));
        self
    }

    /// The pid `-O check` reports.
    pub fn with_master_pid(self, pid: u32) -> Self {
        self.set_master(Some(pid));
        self
    }

    /// Take the master away (`None`) or bring one up with `pid`, mid-test.
    pub fn set_master(&self, pid: Option<u32>) {
        use std::sync::atomic::Ordering::SeqCst;
        if let Some(pid) = pid {
            self.master_pid.store(pid, SeqCst);
        }
        self.master.store(pid.is_some(), SeqCst);
    }

    /// Every script handed to `run`, in order. This is the assertion surface
    /// for wrapper construction.
    pub fn scripts(&self) -> Vec<String> {
        self.scripts.lock().unwrap().clone()
    }

    /// Every mux op, in order, `Check` included. The assertion surface for
    /// forward sequencing.
    pub fn mux_ops(&self) -> Vec<MuxOp> {
        self.mux_ops.lock().unwrap().clone()
    }
}

impl Transport for Fake {
    fn run_unlocked(&self, _host: &Host, script: &str) -> Result<Output> {
        self.scripts.lock().unwrap().push(script.to_string());
        Ok(self
            .outputs
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Output::ok("")))
    }

    fn mux_op(&self, _host: &Host, op: MuxOp) -> Result<Output> {
        self.mux_ops.lock().unwrap().push(op.clone());
        if op == MuxOp::Check {
            use std::sync::atomic::Ordering::SeqCst;
            return Ok(if self.master.load(SeqCst) {
                Output {
                    stdout: Vec::new(),
                    stderr: format!("Master running (pid={})\r\n", self.master_pid.load(SeqCst)),
                    code: 0,
                }
            } else {
                Output::fail(255, "Control socket connect: No such file or directory")
            });
        }
        if let Some(hook) = self.mux_hook.lock().unwrap().as_ref() {
            hook(&op);
        }
        Ok(self
            .mux_outputs
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Output::ok("")))
    }
}
