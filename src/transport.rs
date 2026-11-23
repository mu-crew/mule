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

use std::path::{Path, PathBuf};
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

    /// Run a script with a session stdin, WITHOUT the ticket lock: the
    /// unlocked primitive behind [`Transport::run_with_stdin`].
    ///
    /// Same rule as [`Transport::run_unlocked`]: implement it, never call it
    /// from outside this module. Provided as a refusal so the many
    /// transports that never carry stdin (test fakes, [`InFlight`]) need no
    /// change; [`Ssh`] and [`Fake`] implement it.
    fn run_with_stdin_unlocked(&self, host: &Host, script: &str, feed: &Feed) -> Result<Output> {
        let _ = (host, script, feed);
        anyhow::bail!("this transport cannot carry a session's stdin")
    }

    /// [`Transport::run`] for a session that also carries stdin: a payload,
    /// then a control stream (`mule serve`'s cancels), bounded locally by
    /// [`Feed`]. Takes the ticket lock and retries a proven pre-exec refusal
    /// exactly as `run` does, so the lock stays the transport's, not the
    /// caller's (SPEC § Contention and fairness).
    ///
    /// A retried attempt replays every control line sent so far: a cancel
    /// written into the refused attempt's stdin would otherwise be lost and
    /// the op run anyway.
    fn run_with_stdin(&self, host: &Host, script: &str, feed: &Feed) -> Result<Output> {
        locked_with_recovery(self, host, REFUSAL_RECOVERY, || {
            self.run_with_stdin_unlocked(host, script, feed)
        })
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
    locked_with_recovery(t, host, budget, || t.run_unlocked(host, script))
}

/// The lock and the refusal-recovery loop, shared by every locked method so
/// the stdin path cannot drift from `run`'s.
fn locked_with_recovery<T: Transport + ?Sized>(
    t: &T,
    host: &Host,
    budget: Duration,
    mut attempt: impl FnMut() -> Result<Output>,
) -> Result<Output> {
    let deadline = Instant::now() + budget;
    let mut backoff = FIRST_BACKOFF;
    loop {
        let result = crate::lock::with_lock(&host.name, &mut attempt)?;
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
/// doing 2FA (`ProxyCommand 2fa-proxy ...`) prompts regardless, so every mule call
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
    match crate::helper::SSH_ADD
        .command()
        .and_then(|mut cmd| cmd.arg("-l").output())
    {
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
        let mut cmd = crate::helper::SSH
            .command()
            .with_context(|| format!("spawning ssh for host {}", host.name))?;
        // This handoff is local even if the operator has a broad SendEnv rule.
        cmd.env_remove(crate::config::LEASE_CONFIG_ENV)
            .arg("-E")
            .arg(&log.0)
            .args(run_args(host, script));
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

    fn run_with_stdin_unlocked(&self, host: &Host, script: &str, feed: &Feed) -> Result<Output> {
        use std::os::unix::process::CommandExt;
        let log = SshLog::create()?;
        // Under a watchdog that outlives this process: see [`WATCHDOG`].
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(WATCHDOG)
            .arg("mule-watchdog")
            .arg((feed.kill_after + WATCHDOG_SLACK).as_secs().to_string())
            .arg("ssh")
            .arg("-E")
            .arg(&log.0)
            .args(run_args(host, script))
            .process_group(0);
        let out = spawn_fed(&mut cmd, feed)
            .with_context(|| format!("ssh with stdin for host {}", host.name))?;
        let ssh_log = std::fs::read_to_string(&log.0).unwrap_or_default();
        if let Some(error) = crate::errors::classify_attempt(&ssh_log, &out.stdout, ssh_agent_state)
        {
            return Err(error.into());
        }
        let mut stderr = out.stderr;
        stderr.push_str(&ssh_log);
        Ok(Output { stderr, ..out })
    }

    fn mux_op(&self, host: &Host, op: MuxOp) -> Result<Output> {
        let mut cmd = crate::helper::SSH
            .command()
            .with_context(|| format!("spawning ssh for host {}", host.name))?;
        cmd.env_remove(crate::config::LEASE_CONFIG_ENV)
            .args(mux_args(host, &op));
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

/// Runs `$2...` (ssh) and KILLs it `$1` seconds after the start, whatever
/// becomes of the process that spawned it.
///
/// `spawn_fed`'s own kill is a loop in the caller's thread: it died with
/// `mule serve`, and a session whose caller hung up (stdin EOF, then serve's
/// exit) or was SIGKILLed lived on past 16s with the only session slot taken
/// (review, 2026-11-20, local CLI repro). This shell is the session's parent
/// and in its own process group, so neither serve's death nor a hangup sent
/// to the caller's group reaches it, and it ignores SIGHUP besides (as ssh
/// then does). Its timer's stdio is `/dev/null`, so it holds none of the
/// session's pipes; it is TERMed, and kills its `sleep`, once ssh ends.
pub const WATCHDOG: &str = "trap '' HUP; n=$1; shift; exec 3<&0\n\
\"$@\" <&3 3<&- & c=$!\n\
exec 3<&- < /dev/null\n\
( sleep \"$n\" & s=$!; trap 'kill $s; exit 0' TERM; wait $s; kill -KILL $c ) \
< /dev/null > /dev/null 2>&1 & k=$!\n\
wait $c; r=$?; kill $k 2>/dev/null; exit $r";

/// The watchdog fires this long after `spawn_fed`'s own kill, so in a live
/// serve the in-process kill, which reports why, always comes first.
const WATCHDOG_SLACK: Duration = Duration::from_secs(1);

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

/// What a session with stdin carries, and the local bounds on it.
///
/// For `mule serve`'s one round trip per batch (SPEC § Bounded synchronous
/// ops). The remote script bounds the session itself; these are the local
/// backstops for when the host or the link cannot be trusted to answer.
pub struct Feed<'a> {
    /// Written to the session's stdin first.
    pub payload: &'a [u8],
    /// Where the session's stdin goes once the payload is written, for
    /// control lines, until the session ends.
    pub control: &'a Control,
    /// Hard local kill, counted from the spawn -- before the payload write.
    pub kill_after: Duration,
    /// The session is killed, and the call fails, past this much stdout.
    pub max_stdout: usize,
}

/// Bytes of a session's stderr kept; the rest is read and dropped, so a
/// chatty remote can neither stall the session nor grow this process.
const MAX_FED_STDERR: usize = 64 << 10;

/// After the session process exits, how long its pipes may take to close.
/// With ssh multiplexing the master holds the pipe fds, not the client, and
/// closes them as it tears the channel down; a master that never does must
/// not hang the caller.
const FED_DRAIN: Duration = Duration::from_secs(2);

/// A session's stdin after its payload: the control stream of one round trip.
///
/// Writes are non-blocking: a control line is a few bytes, below `PIPE_BUF`,
/// so each write is atomic -- whole or `EAGAIN`, never a torn line that names
/// the wrong op. One a wedged reader cannot take is dropped rather than
/// blocking the caller, whose next act on a wedge is the hard kill anyway.
#[derive(Default)]
pub struct ControlStream {
    stdin: Option<std::process::ChildStdin>,
    /// Bumped by each spawn and each close, so a writer thread that parks
    /// late (its attempt already over) parks nothing.
    attempt: u64,
    /// Every line of this round trip, replayed into a retried attempt.
    sent: Vec<Vec<u8>>,
    abandoned: bool,
}

pub type Control = std::sync::Arc<std::sync::Mutex<ControlStream>>;

impl ControlStream {
    /// Send one control line to the session in flight, or hold it for the
    /// session about to start.
    pub fn send(&mut self, line: &[u8]) {
        self.sent.push(line.to_vec());
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = std::io::Write::write(stdin, line);
        }
    }

    /// Start a new round trip: forget the last one's lines.
    pub fn reset(&mut self) {
        self.sent.clear();
    }

    /// The caller is gone: close the session's stdin now (the remote reads
    /// EOF as cancel-all) and refuse to start another session.
    pub fn abandon(&mut self) {
        self.abandoned = true;
        self.stdin = None;
    }

    pub fn is_abandoned(&self) -> bool {
        self.abandoned
    }

    fn open(&mut self) -> Option<u64> {
        if self.abandoned {
            return None;
        }
        self.attempt += 1;
        Some(self.attempt)
    }

    fn park(&mut self, attempt: u64, stdin: std::process::ChildStdin) {
        if attempt != self.attempt || self.abandoned {
            return;
        }
        use std::os::fd::AsRawFd;
        // SAFETY: fcntl on an fd this ChildStdin owns, with no pointers.
        unsafe {
            let fd = stdin.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
        }
        let mut stdin = stdin;
        for line in &self.sent {
            let _ = std::io::Write::write(&mut stdin, line);
        }
        self.stdin = Some(stdin);
    }

    fn close(&mut self, attempt: u64) {
        if attempt == self.attempt {
            self.stdin = None;
            self.attempt += 1;
        }
    }
}

/// Spawn `cmd` as a session with stdin, bounded by `feed`.
///
/// The hard kill is armed at the spawn, before a byte of payload: a payload
/// write into a session nobody reads blocks, and the prototype started its
/// clock only after the write, so a wedged link hung it unbounded (course
/// check #3). The write therefore runs on its own thread, as do both reads;
/// the calling thread only watches the clock and the stdout bound.
///
/// A child that leads its own process group is killed as a group: ssh's
/// [`WATCHDOG`] parent with its timer, or a local `sh` standing in for ssh
/// (tests) with its background jobs.
#[doc(hidden)]
pub fn spawn_fed(cmd: &mut Command, feed: &Feed) -> Result<Output> {
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    let attempt = feed
        .control
        .lock()
        .unwrap()
        .open()
        .ok_or_else(|| anyhow::anyhow!("not started: the caller is gone"))?;
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning a session with stdin")?;
    let deadline = Instant::now() + feed.kill_after;
    let mut stdin = child.stdin.take().expect("piped");
    let mut so = child.stdout.take().expect("piped");
    let mut se = child.stderr.take().expect("piped");
    let overflow = Arc::new(AtomicBool::new(false));
    let (out_tx, out_rx) = mpsc::channel();
    let (err_tx, err_rx) = mpsc::channel();
    let (wrote_tx, wrote_rx) = mpsc::channel();
    {
        let (overflow, max) = (overflow.clone(), feed.max_stdout);
        std::thread::spawn(move || {
            let (mut buf, mut chunk) = (Vec::new(), vec![0u8; 64 << 10]);
            loop {
                match so.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) if buf.len() + n > max => {
                        overflow.store(true, Ordering::SeqCst);
                        break;
                    }
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
            let _ = out_tx.send(buf);
        });
    }
    std::thread::spawn(move || {
        let (mut buf, mut chunk) = (Vec::new(), vec![0u8; 16 << 10]);
        while let Ok(n) = se.read(&mut chunk) {
            if n == 0 {
                break;
            }
            let keep = n.min(MAX_FED_STDERR.saturating_sub(buf.len()));
            buf.extend_from_slice(&chunk[..keep]);
        }
        let _ = err_tx.send(buf);
    });
    {
        // Owned copies: the writer may outlive this call, blocked on a pipe a
        // wedged master still holds, and must borrow nothing from it.
        let payload: Arc<[u8]> = feed.payload.into();
        let control = feed.control.clone();
        std::thread::spawn(move || {
            let _ = stdin.write_all(&payload);
            let _ = stdin.flush();
            control.lock().unwrap().park(attempt, stdin);
            let _ = wrote_tx.send(());
        });
    }
    let pid = child.id() as libc::pid_t;
    let kill = |child: &mut std::process::Child| {
        // SAFETY: getpgid/kill take plain integers.
        if unsafe { libc::getpgid(pid) } == pid {
            unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
        let _ = child.kill();
    };
    let (mut killed, mut too_big) = (false, false);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if overflow.load(Ordering::SeqCst) {
            too_big = true;
            kill(&mut child);
            break child.wait()?;
        }
        if Instant::now() >= deadline {
            killed = true;
            kill(&mut child);
            break child.wait()?;
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    let drain = Instant::now() + FED_DRAIN;
    let left = || drain.saturating_duration_since(Instant::now());
    // The writer first, then the close: a park that lands after the close
    // would leave the control stream open on a finished session.
    let _ = wrote_rx.recv_timeout(left());
    feed.control.lock().unwrap().close(attempt);
    let stdout = out_rx.recv_timeout(left());
    let stderr = err_rx.recv_timeout(left()).unwrap_or_default();
    if too_big || overflow.load(Ordering::SeqCst) {
        anyhow::bail!("session output passed {} bytes; killed it", feed.max_stdout);
    }
    if killed {
        anyhow::bail!(
            "session killed after {}s: the host or the link is wedged",
            feed.kill_after.as_secs()
        );
    }
    let Ok(stdout) = stdout else {
        anyhow::bail!(
            "session output did not close {}s after exit",
            FED_DRAIN.as_secs()
        );
    };
    Ok(Output {
        stdout,
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        code: status.code().unwrap_or(-1),
    })
}

/// One call's private `ssh -E` file, removed on drop.
///
/// Created exclusively (`create_new`, so a planted file or symlink fails the
/// call rather than being written through) and 0600, under a name no other
/// call can choose, inside `<state_dir>/ssh`: a directory checked to be ours
/// and 0700 before every use. The refusal proof in `run` reads this file, so
/// it must not sit in a shared temp dir, where another user can see, plant or
/// race names beside it.
#[doc(hidden)]
pub struct SshLog(PathBuf);

impl SshLog {
    fn create() -> Result<Self> {
        Self::create_in(&crate::lock::state_dir().join("ssh"))
    }

    /// Create the log in `dir`, making `dir` 0700 if it is missing.
    ///
    /// Fails closed: a `dir` that is a symlink, not a directory, owned by
    /// someone else, or open to group or other is refused, not repaired. A
    /// directory we did not make that way is one we cannot vouch for.
    #[doc(hidden)]
    pub fn create_in(dir: &Path) -> Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        match std::fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("cannot create ssh log dir {}", dir.display()));
            }
        }
        // Resolve once and use only the result from here on: the checks, the
        // creation, ssh's `-E` open and the read-back all name this canonical
        // path, so an alias (symlinked state dir or ancestor) swapped after
        // this point cannot redirect the log.
        // The leaf itself must not be a link (an ancestor alias is fine, it
        // is resolved below); canonicalize would otherwise follow it silently.
        if std::fs::symlink_metadata(dir).is_ok_and(|m| m.file_type().is_symlink()) {
            anyhow::bail!("refusing ssh log dir {}: it is a symlink", dir.display());
        }
        let real = std::fs::canonicalize(dir)
            .with_context(|| format!("cannot resolve ssh log dir {}", dir.display()))?;
        // lstat, so a symlink is judged as itself and never followed.
        let meta = std::fs::symlink_metadata(&real)
            .with_context(|| format!("cannot inspect ssh log dir {}", real.display()))?;
        // SAFETY: geteuid has no preconditions and cannot fail.
        let uid = unsafe { libc::geteuid() };
        if !meta.file_type().is_dir() || meta.uid() != uid || meta.mode() & 0o077 != 0 {
            anyhow::bail!(
                "refusing ssh log dir {}: it must be a directory owned by uid {uid} \
                 with mode 0700 (found uid {}, mode {:o})",
                real.display(),
                meta.uid(),
                meta.mode() & 0o7777
            );
        }
        // Like ssh's StrictModes: whoever can write an ancestor can swap the
        // verified dir for another between this check and ssh's open. The one
        // exception is a root-owned sticky dir (/tmp): the sticky bit stops
        // others renaming or removing our entries, so state dirs may live there.
        for anc in real.ancestors().skip(1) {
            let m = std::fs::metadata(anc)
                .with_context(|| format!("cannot inspect {}", anc.display()))?;
            let sticky_root = m.uid() == 0 && m.mode() & 0o1000 != 0;
            if (m.uid() != uid && m.uid() != 0) || (m.mode() & 0o022 != 0 && !sticky_root) {
                anyhow::bail!(
                    "refusing ssh log dir {}: ancestor {} is unsafe (uid {}, mode {:o}); \
                     it must be owned by you or root and not group/other writable",
                    dir.display(),
                    anc.display(),
                    m.uid(),
                    m.mode() & 0o7777
                );
            }
        }
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let path = real.join(format!(
            "mule-ssh-{}-{}-{nanos}.log",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&path)
            .with_context(|| format!("cannot create ssh log {}", path.display()))?;
        Ok(Self(path))
    }

    #[doc(hidden)]
    pub fn path(&self) -> &Path {
        &self.0
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

// A call is admitted only from IDLE, so STOPPING and STOPPED both refuse.
// STOPPING is BUSY with admission already closed: a stop that waited in BUSY
// instead left the gate open, and a call queued on the ticket behind the
// one in flight took the freed gate before the stop's next look (a racing
// test admitted 3-9 calls in 10 rounds).
const GATE_IDLE: u8 = 0;
const GATE_BUSY: u8 = 1;
const GATE_STOPPING: u8 = 2;
const GATE_STOPPED: u8 = 3;

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
        // BUSY goes back to IDLE; a stop that began meanwhile left STOPPING,
        // which this call's end turns into STOPPED.
        let _ = self
            .state
            .compare_exchange(GATE_BUSY, GATE_IDLE, Ordering::SeqCst, Ordering::SeqCst)
            .or_else(|_| {
                self.state.compare_exchange(
                    GATE_STOPPING,
                    GATE_STOPPED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
            });
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
    /// flight.
    ///
    /// The contract, which callers do rely on: admission closes before any
    /// waiting and stays closed, timeout or not, so a call queued on the
    /// ticket is refused once it gets here and never launches after the
    /// signal; the one call already admitted gets a bounded, best-effort wait
    /// of at most `grace`.
    ///
    /// The bool is an observation, not a proof: true means no call was seen in
    /// flight at the last look before the deadline. It is best effort -- a
    /// call that ends close to the deadline can be reported either way -- and
    /// no caller relies on it (the lease and exec signal paths discard it).
    pub fn stop(&self, grace: std::time::Duration) -> bool {
        use std::sync::atomic::Ordering;
        let deadline = std::time::Instant::now() + grace;
        // Close admission first, whatever is in flight: IDLE closes outright,
        // BUSY becomes STOPPING. Retried, because a call can start or end
        // between the load and the swap.
        let mut current = self.0.load(Ordering::SeqCst);
        loop {
            let next = match current {
                GATE_IDLE => GATE_STOPPED,
                GATE_BUSY => GATE_STOPPING,
                _ => break,
            };
            match self
                .0
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(seen) => current = seen,
            }
        }
        if self.0.load(Ordering::SeqCst) == GATE_STOPPED {
            return true;
        }
        // Then wait for the call in flight, which turns STOPPING into STOPPED
        // as it ends. On a timeout STOPPING stays: it refuses as STOPPED does.
        //
        // The state is read before the clock, so STOPPED is reported only when
        // it was seen before the deadline was noticed. Read the other way
        // round, a call still busy at the deadline that ended before the next
        // poll was reported clean (1ms grace, call held 4ms: true after 11ms).
        // For the same reason no sleep runs past the deadline. This only
        // sharpens the observation; the gate does not depend on it.
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            std::thread::sleep(remaining.min(std::time::Duration::from_millis(10)));
            let stopped = self.0.load(Ordering::SeqCst) == GATE_STOPPED;
            if std::time::Instant::now() >= deadline {
                return false;
            }
            if stopped {
                return true;
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
    payloads: std::sync::Mutex<Vec<Vec<u8>>>,
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
            payloads: Default::default(),
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

    /// Every payload handed to `run_with_stdin`, in order.
    pub fn payloads(&self) -> Vec<Vec<u8>> {
        self.payloads.lock().unwrap().clone()
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

    /// Records the script and the payload; the control stream is never
    /// opened, since nothing reads it.
    fn run_with_stdin_unlocked(&self, host: &Host, script: &str, feed: &Feed) -> Result<Output> {
        self.payloads.lock().unwrap().push(feed.payload.to_vec());
        self.run_unlocked(host, script)
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
