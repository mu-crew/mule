//! `mule serve --stdio`: bounded synchronous ops for one caller.
//!
//! The third class of ssh use (SPEC § Bounded synchronous ops). `exec` costs
//! about four round trips per command (dispatch, probe, collect, removal proof:
//! 1.3-1.7s measured on two real hosts); serve answers a read, stat or write in
//! one, at 0.32-0.38s p50 on the same hosts. Those three only: no arbitrary
//! command runs here; a command is a detached job (`run`, `exec`).
//!
//! - **A child of its caller, not a daemon.** JSON lines on stdin, JSON lines
//!   on stdout. stdin EOF, or a stdout the caller closed, means the caller is
//!   gone: the in-flight ops are cancelled, the queue is dropped, and the
//!   process exits. Nothing is shared between callers and nothing outlives
//!   one, so SPEC's "no process between invocations" holds per caller.
//! - **One ticket-locked round trip per batch**, through
//!   [`Transport::run_with_stdin`]: the lock is the transport's, as for every
//!   other verb. No session is held between batches.
//! - **Bounded by the host, not by this process.** The remote script bounds
//!   every phase -- payload read [`PAYLOAD_SECS`] and delivery
//!   [`DELIVER_SECS`] under a timer of the script's own ([`BOUND_FN`]), each op [`MAX_OP_SECS`] in its own
//!   session (or process group), KILLed whole 1s later -- and caps every op's output, and
//!   the batch's, on disk and on the wire. So a batch frees the session even
//!   if this process is SIGKILLed or the link wedges. Work longer than
//!   [`MAX_OP_SECS`] comes back rc 124 and belongs in a detached job. A host
//!   without the required tools, process group support,
//!   or a way to find an op's processes fails every op closed.
//! - **Payloads on stdin**, never argv: a 200KB write is one round trip.
//! - **Coalescing.** Requests queued while a batch is in flight go together
//!   in the next one and run in parallel on the host.
//! - **Cancel without a second session.** After the payload, the session's
//!   stdin is a control stream: `<k>\n` kills op `k`, EOF
//!   kills them all; an op the host saw complete is not killed, and reports
//!   its real result. A second ssh could not: on a `MaxSessions 1` host it
//!   would be refused until the op it means to stop had ended.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufRead, Write};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::Result;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde::{Deserialize, Serialize};

use crate::config::Host;
use crate::transport::{Control, Feed, Transport};

mod script;
use script::write_body;
pub use script::{BOUND_FN, batch_body, batch_script};

/// Hard per-op limit on the host. 1s: the remote workspace wants a single op at <=0.5s, so a
/// bounded op needing twice that is a job.
pub const MAX_OP_SECS: u32 = 1;
/// Test seam: a longer op deadline, for a test whose subject is not the
/// deadline (a cancel) and which must not race it. A delayed test thread lost
/// a cancel to a correct 1s timeout. Never shorter than [`MAX_OP_SECS`], and
/// at most [`TEST_OP_SECS_MAX`]: the remote bounds together must stay under
/// [`LOCAL_KILL`]. Anything else is ignored.
#[doc(hidden)]
pub const TEST_OP_SECS_ENV: &str = "MULE_SERVE_TEST_OP_SECS";
/// 5 payload + 3 op + 1 kill grace + 5 delivery = 14s, under the 15s
/// [`LOCAL_KILL`].
#[doc(hidden)]
pub const TEST_OP_SECS_MAX: u32 = 3;
const _: () = assert!(
    PAYLOAD_SECS + TEST_OP_SECS_MAX + KILL_GRACE_SECS + DELIVER_SECS < LOCAL_KILL.as_secs() as u32
);

/// The op deadline this process uses: [`MAX_OP_SECS`] unless a test asked
/// for longer through [`TEST_OP_SECS_ENV`].
fn op_secs() -> u32 {
    std::env::var(TEST_OP_SECS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|secs| (MAX_OP_SECS..=TEST_OP_SECS_MAX).contains(secs))
        .unwrap_or(MAX_OP_SECS)
}
/// Bound on reading the batch's payloads off stdin on the host. A 200KB write
/// took 0.54-1.05s whole on two real hosts; 5s leaves room for 8MB on a slow
/// link.
pub const PAYLOAD_SECS: u32 = 5;
/// Bound on writing the frames back. The output budget is
/// [`MAX_BATCH_OUTPUT`], so this is a slow-link allowance, not a size limit.
pub const DELIVER_SECS: u32 = 5;
/// Local hard kill, from the spawn. Above every remote bound together
/// (payload + op + kill grace + delivery = 12s) so it fires only when the host
/// or the link is wedged. Killing the ssh does not free the session (SPEC §
/// Detached cleanup); the remote bounds do, and they run without us.
pub const LOCAL_KILL: Duration = Duration::from_secs(15);
/// Per-op stdout cap for `read`, enforced on the host: control-sized, like
/// `tail`'s one-shot read is bounded.
pub const MAX_OUT: u64 = 1 << 20;
/// Both caps of `stat` and `write`, and `read`'s stderr: a mode line, a path
/// or an error message. 8KB holds a 4096-byte `PATH_MAX` path twice.
pub const MAX_SMALL: u64 = 8 << 10;
/// All ops' output in one batch, on the host's disk and on the wire. Each op
/// reserves its caps out of it when it joins a batch, so the spool can never
/// pass it however the ops behave: 7 reads a batch, or dozens of stats and
/// writes. A shared cap the ops drew on as they wrote would let a batch hold
/// more, but `sh` has no atomic counter, and the per-op caps alone let 64 ops
/// spool 68MB for an 8MB reply.
pub const MAX_BATCH_OUTPUT: u64 = 8 << 20;
/// Payload cap per batch (and so per op), so a batch is a bounded transfer.
pub const MAX_PAYLOAD: usize = 8 << 20;
/// Script cap per batch, before [`batch_script`] base64s it for `/bin/sh`:
/// leaves room for 4/3 expansion and the fixed batch code within the
/// 112 KiB dispatch cap. Payloads ride stdin.
pub const MAX_SCRIPT: usize = 64 << 10;
/// Required on every host, in addition to the process-control alternatives.
const REQUIRED_TOOLS: &str = "base64 mktemp head tail wc cat sleep";
/// Ops per batch.
pub const MAX_BATCH: usize = 64;
/// How long the worker lets a burst gather before sending. Windows of 1-20ms
/// were invisible next to the round trip (codemode spike).
pub const WINDOW: Duration = Duration::from_millis(3);
/// After the caller goes, how long serve waits for the batch in flight to end
/// before exiting anyway. Exiting early is safe -- the remote bounds still
/// free the session -- so this only gives a clean end a chance.
const EXIT_GRACE: Duration = Duration::from_secs(2);

/// The host lacks one of the tools the batch script needs.
pub const RC_MISSING_TOOL: i32 = 97;
/// Grace between an op's TERM and the KILL of its whole process group.
const KILL_GRACE_SECS: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum Request {
    Read {
        id: u64,
        path: String,
    },
    Stat {
        id: u64,
        path: String,
    },
    Write {
        id: u64,
        path: String,
        #[serde(default)]
        data_b64: String,
    },
    Cancel {
        id: u64,
    },
}

/// One bounded op, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Read(String),
    Stat(String),
    Write(String, Vec<u8>),
}

impl Op {
    fn payload(&self) -> &[u8] {
        match self {
            Op::Write(_, data) => data,
            _ => &[],
        }
    }

    fn path(&self) -> &str {
        match self {
            Op::Read(p) | Op::Stat(p) | Op::Write(p, _) => p,
        }
    }

    /// `(stdout, stderr)` caps on the host, reserved from the batch's
    /// [`MAX_BATCH_OUTPUT`].
    pub fn caps(&self) -> (u64, u64) {
        match self {
            Op::Read(_) => (MAX_OUT, MAX_SMALL),
            Op::Stat(_) | Op::Write(..) => (MAX_SMALL, MAX_SMALL),
        }
    }

    /// What this op can put on the host's disk: each cap plus the one byte
    /// past it the runner keeps to see a cut.
    fn spool(&self) -> u64 {
        let (o, e) = self.caps();
        o + e + 2
    }

    /// The shell body run in the op's own session, with the op's path as
    /// `$1`. The path is never part of any shell code: it travels base64'd
    /// and is decoded into a variable ([`batch_body`]), so it is data
    /// byte-for-byte, trailing newlines included. [`accept`] refuses a NUL,
    /// which no shell string can hold.
    ///
    /// A path `-` becomes `./-`, the same file: `--` ends option parsing but
    /// leaves `-` meaning stdin to GNU `head` and `stat`, and a read of the
    /// file named `-` replied empty, a stat size 0, both rc 0 (on Linux).
    fn body(&self) -> String {
        const DASH: &str = "case $1 in -) set -- ./-;; esac; ";
        match self {
            // One byte past the cap, so the runner sees and flags a cut.
            Op::Read(_) => format!("{DASH}exec head -c {} -- \"$1\"", MAX_OUT + 1),
            // uname, not try-and-fall-back (SPEC § Remote snippets fail closed).
            Op::Stat(_) => format!(
                "{DASH}case $(uname -s) in Linux) exec stat -c '%s %Y %f' -- \"$1\";; \
                 *) exec stat -f '%z %m %Xp' -- \"$1\";; esac"
            ),
            Op::Write(_, data) => write_body(data.len()),
        }
    }

    /// This op's share of a batch script, before the script exists.
    fn script_cost(&self) -> usize {
        (self.body().len() + self.path().len()).div_ceil(3) * 4 + 600
    }
}

/// What the host knows of how an op ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ended {
    /// It ran to the end; `rc` is its own.
    Completed,
    /// A cancel killed it before it completed.
    Cancelled,
    /// The deadline killed it before it completed.
    TimedOut,
    /// It ended with no result, without cancellation or a deadline.
    Lost,
    /// It completed, but the host could not spool its output whole (a
    /// collector failed: a full disk, a quota): its bytes are not its own.
    Unspooled,
    /// The host could not prove that every process of the op was gone when
    /// it ended (its `pgrep` errored, or something kept answering a KILL
    /// that was not a zombie): something it started may still run.
    Uncleaned,
}

/// One op's result from the frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub rc: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// The host cut stdout or stderr at a cap.
    pub truncated: bool,
    pub ended: Ended,
}

/// Parse `n` frames and the end marker. `None` for anything malformed,
/// missing or trailing: the caller then fails the whole batch rather than
/// guess which op a byte belongs to.
pub fn parse_frames(mut buf: &[u8], n: usize) -> Option<Vec<Frame>> {
    let mut out = Vec::with_capacity(n);
    for k in 0..n {
        let nl = buf.iter().position(|&b| b == b'\n')?;
        let head = std::str::from_utf8(&buf[..nl]).ok()?;
        let f: Vec<&str> = head.split(' ').collect();
        if f.len() != 7 || f[0] != "@@" || f[1].parse::<usize>().ok()? != k {
            return None;
        }
        let rc: i32 = f[2].parse().ok()?;
        let (no, ne): (usize, usize) = (f[3].parse().ok()?, f[4].parse().ok()?);
        let truncated = match f[5] {
            "0" => false,
            "1" => true,
            _ => return None,
        };
        let ended = match f[6] {
            "d" => Ended::Completed,
            "c" => Ended::Cancelled,
            "t" => Ended::TimedOut,
            "k" => Ended::Lost,
            "e" => Ended::Unspooled,
            "u" => Ended::Uncleaned,
            _ => return None,
        };
        buf = &buf[nl + 1..];
        if buf.len() < no + ne {
            return None;
        }
        out.push(Frame {
            rc,
            stdout: buf[..no].to_vec(),
            stderr: buf[no..no + ne].to_vec(),
            truncated,
            ended,
        });
        buf = &buf[no + ne..];
    }
    (buf == b"@@ end\n").then_some(out)
}

/// The most stdout a batch can legitimately send: the host's budget plus a
/// header per op. Anything past it is not ours, and the session is killed.
fn max_batch_stdout(ops: usize) -> usize {
    MAX_BATCH_OUTPUT as usize + (ops + 1) * 64
}

/// One reply line. `error` and `kind` are set on failure; `kind` is stable for
/// programs:
///
/// - `bad_request`, `too_large` (over a batch cap, refused before any ssh),
///   `timeout`, `cancelled`, `output_limit`;
/// - `lost`: the op ended on the host with no result, and neither a cancel nor
///   the deadline killed it;
/// - `spool_failed`: rc -1 whatever the op's own rc (in `error`). The op ran,
///   so a write's effect may have happened, but its output is not whole, and a
///   caller reading rc 0 would trust it;
/// - `cleanup_failed`: rc -1 likewise. The op ended, but the host could not
///   prove every process of it gone, so something it started may still run;
/// - `host_tool_missing`, `batch_failed`, `master_gone`.
///
/// `cancelled` is a fact: the op was dropped before dispatch, or the host
/// killed it before it completed. `cancel_requested` only says a cancel was
/// asked for; an op that completed anyway reports its real result, since a
/// caller retrying a "cancelled" effect that happened would repeat it.
#[derive(Debug, Serialize)]
pub struct Reply {
    pub id: u64,
    pub rc: i32,
    pub stdout_b64: String,
    pub stderr: String,
    pub truncated: bool,
    pub cancelled: bool,
    pub cancel_requested: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<&'static str>,
    pub batch: u64,
    pub batch_size: usize,
}

fn error_reply(id: u64, kind: &'static str, error: String) -> Reply {
    Reply {
        id,
        rc: -1,
        stdout_b64: String::new(),
        stderr: String::new(),
        truncated: false,
        cancelled: false,
        cancel_requested: false,
        error: Some(error),
        kind: Some(kind),
        batch: 0,
        batch_size: 0,
    }
}

#[derive(Default)]
struct State {
    queue: VecDeque<(u64, Op)>,
    /// Op id -> index in the batch in flight.
    inflight: HashMap<u64, usize>,
    /// Ops in flight a cancel was sent for: asked, not confirmed.
    cancel_requested: HashSet<u64>,
    closed: bool,
}

type Shared = Arc<(Mutex<State>, Condvar)>;

enum Event {
    Line(String),
    /// The caller is gone: stdin EOF or error, or its stdout closed.
    CallerGone,
    /// The worker stopped, with serve's exit code.
    Worker(i32),
}

/// The serve loop over any transport, reader and writer, so tests drive it.
///
/// Returns the exit code: 0 when the caller goes, 3 when the master does.
/// Neither the caller's stdin nor its stdout can hold a session or the
/// ticket: requests are read on one thread, replies written on another, and
/// the worker never waits on either while it holds a session.
pub fn serve<T: Transport + Send + Sync + 'static>(
    transport: Arc<T>,
    host: Host,
    input: impl BufRead + Send + 'static,
    output: impl Write + Send + 'static,
) -> i32 {
    let state: Shared = Arc::default();
    let ctl: Control = Arc::default();
    let (events, inbox) = mpsc::channel();
    let (replies, outbox) = mpsc::channel::<Reply>();
    {
        let events = events.clone();
        std::thread::spawn(move || {
            for line in input.lines() {
                let Ok(line) = line else { break };
                if events.send(Event::Line(line)).is_err() {
                    return;
                }
            }
            let _ = events.send(Event::CallerGone);
        });
    }
    let writer = {
        let events = events.clone();
        std::thread::spawn(move || write_replies(output, outbox, events))
    };
    {
        let (state, ctl, replies, events) =
            (state.clone(), ctl.clone(), replies.clone(), events.clone());
        std::thread::spawn(move || {
            let code = worker(&*transport, &host, &state, &ctl, &replies);
            let _ = events.send(Event::Worker(code));
        });
    }
    drop(events);
    let code = loop {
        match inbox.recv() {
            Ok(Event::Line(line)) => accept(&line, &state, &ctl, &replies),
            Ok(Event::Worker(code)) => break code,
            Ok(Event::CallerGone) | Err(_) => {
                // Drop the queue, close the control stream (the remote kills
                // every op on EOF), refuse any further session, and give the
                // batch in flight a moment to end.
                {
                    let (lock, cv) = &*state;
                    let mut st = lock.lock().unwrap();
                    st.closed = true;
                    st.queue.clear();
                    cv.notify_all();
                }
                ctl.lock().unwrap().abandon();
                let deadline = std::time::Instant::now() + EXIT_GRACE;
                loop {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    match inbox.recv_timeout(left) {
                        Ok(Event::Worker(_)) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                break 0;
            }
        }
    };
    drop(replies);
    // The writer ends once every reply is out. A caller that stopped reading
    // can hold it forever; it holds no session, so serve exits without it.
    let _ = join_within(writer, EXIT_GRACE);
    code
}

fn join_within(handle: std::thread::JoinHandle<()>, limit: Duration) -> bool {
    let deadline = std::time::Instant::now() + limit;
    while !handle.is_finished() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let _ = handle.join();
    true
}

fn write_replies(mut output: impl Write, outbox: Receiver<Reply>, events: Sender<Event>) {
    for reply in outbox {
        let ok = serde_json::to_writer(&mut output, &reply).is_ok()
            && output.write_all(b"\n").is_ok()
            && output.flush().is_ok();
        if !ok {
            // EPIPE: the caller closed its end. Same as stdin EOF.
            let _ = events.send(Event::CallerGone);
            return;
        }
    }
}

/// Parse one request line and queue it, cancel it, or refuse it.
fn accept(line: &str, state: &Shared, ctl: &Control, replies: &Sender<Reply>) {
    if line.trim().is_empty() {
        return;
    }
    let req: Request = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => {
            let _ = replies.send(error_reply(0, "bad_request", format!("bad request: {e}")));
            return;
        }
    };
    let decode = |id, field: &str, b64: String| {
        B64.decode(b64)
            .map_err(|e| error_reply(id, "bad_request", format!("bad {field}: {e}")))
    };
    let op = match req {
        Request::Cancel { id } => {
            let (lock, _) = &**state;
            let mut st = lock.lock().unwrap();
            if let Some(pos) = st.queue.iter().position(|(q, _)| *q == id) {
                st.queue.remove(pos);
                let mut r = error_reply(id, "cancelled", "cancelled before dispatch".into());
                r.cancelled = true;
                r.cancel_requested = true;
                let _ = replies.send(r);
            } else if let Some(&k) = st.inflight.get(&id) {
                st.cancel_requested.insert(id);
                ctl.lock().unwrap().send(format!("{k}\n").as_bytes());
            }
            return;
        }
        Request::Read { id, path } => Ok((id, Op::Read(path))),
        Request::Stat { id, path } => Ok((id, Op::Stat(path))),
        Request::Write { id, path, .. } if !path.starts_with('/') => Err(error_reply(
            id,
            "bad_request",
            format!("write needs an absolute path, not {path:?}"),
        )),
        Request::Write { id, path, data_b64 } => {
            decode(id, "data_b64", data_b64).map(|d| (id, Op::Write(path, d)))
        }
    };
    let (id, op) = match op {
        Ok(op) => op,
        Err(reply) => {
            let _ = replies.send(reply);
            return;
        }
    };
    // No shell string holds a NUL: `a\0b` reached the host as `ab`.
    if op.path().contains('\0') {
        let _ = replies.send(error_reply(
            id,
            "bad_request",
            "a path cannot hold a NUL byte".into(),
        ));
        return;
    }
    // Refused here, before any ssh: one op past a batch's caps could never be
    // sent, and the caller should hear so rather than see a failed batch.
    if op.payload().len() > MAX_PAYLOAD || op.script_cost() > MAX_SCRIPT {
        let _ = replies.send(error_reply(
            id,
            "too_large",
            format!(
                "op too large for one bounded round trip (payload limit {MAX_PAYLOAD} bytes, \
                 script limit {MAX_SCRIPT}); put it in a file and run a detached job"
            ),
        ));
        return;
    }
    let (lock, cv) = &**state;
    lock.lock().unwrap().queue.push_back((id, op));
    cv.notify_all();
}

/// Take up to one batch's worth of queued ops.
fn take_batch(queue: &mut VecDeque<(u64, Op)>) -> Vec<(u64, Op)> {
    let mut batch = Vec::new();
    let (mut script, mut payload, mut spool) = (0usize, 0usize, 0u64);
    while let Some((_, op)) = queue.front() {
        let (cost, size, out) = (op.script_cost(), op.payload().len(), op.spool());
        if !batch.is_empty()
            && (batch.len() >= MAX_BATCH
                || script + cost > MAX_SCRIPT
                || payload + size > MAX_PAYLOAD
                || spool + out > MAX_BATCH_OUTPUT)
        {
            break;
        }
        script += cost;
        payload += size;
        spool += out;
        batch.push(queue.pop_front().unwrap());
    }
    batch
}

fn worker<T: Transport + ?Sized>(
    transport: &T,
    host: &Host,
    state: &Shared,
    ctl: &Control,
    replies: &Sender<Reply>,
) -> i32 {
    let (lock, cv) = &**state;
    let mut seq = 0u64;
    loop {
        {
            let mut st = lock.lock().unwrap();
            while st.queue.is_empty() && !st.closed {
                st = cv.wait(st).unwrap();
            }
            if st.closed {
                return 0;
            }
        }
        std::thread::sleep(WINDOW);
        let batch = {
            let mut st = lock.lock().unwrap();
            if st.closed {
                return 0;
            }
            let b = take_batch(&mut st.queue);
            st.inflight = b.iter().enumerate().map(|(k, (id, _))| (*id, k)).collect();
            ctl.lock().unwrap().reset();
            b
        };
        if batch.is_empty() {
            continue;
        }
        seq += 1;
        let ops: Vec<Op> = batch.iter().map(|(_, op)| op.clone()).collect();
        let payload: Vec<u8> = ops.iter().flat_map(|op| op.payload().to_vec()).collect();
        let script = batch_script(&ops);
        let feed = Feed {
            payload: &payload,
            control: ctl,
            kill_after: LOCAL_KILL,
            max_stdout: max_batch_stdout(ops.len()),
        };
        let result = transport.run_with_stdin(host, &script, &feed);
        let requested = {
            let mut st = lock.lock().unwrap();
            st.inflight.clear();
            std::mem::take(&mut st.cancel_requested)
        };
        let frames = match &result {
            Ok(out) => parse_frames(&out.stdout, ops.len()).ok_or_else(|| {
                let why = out.stderr.trim();
                if out.code == RC_MISSING_TOOL {
                    (
                        "host_tool_missing",
                        format!(
                            "{why}; serve needs {REQUIRED_TOOLS}, plus \
                             setsid (or a /bin/sh with job control) and pgrep -s (or ps and \
                             awk) on the host"
                        ),
                    )
                } else {
                    (
                        "batch_failed",
                        format!("batch failed (rc {}): {why}", out.code),
                    )
                }
            }),
            Err(e) => Err(("batch_failed", format!("{e:#}"))),
        };
        let master_gone = frames.is_err() && !transport.master_alive(host);
        for (k, (id, _)) in batch.iter().enumerate() {
            let reply = match &frames {
                Ok(f) => op_reply(&f[k], *id, requested.contains(id), seq, batch.len()),
                Err((kind, e)) => Reply {
                    cancel_requested: requested.contains(id),
                    batch: seq,
                    batch_size: batch.len(),
                    ..error_reply(
                        *id,
                        if master_gone { "master_gone" } else { kind },
                        e.clone(),
                    )
                },
            };
            let _ = replies.send(reply);
        }
        if master_gone {
            // Exit 3 is a request for a human (SPEC § SSH master): answer
            // everything still queued, then stop rather than retry.
            let mut st = lock.lock().unwrap();
            st.closed = true;
            for (id, _) in st.queue.drain(..) {
                let _ = replies.send(error_reply(
                    id,
                    "master_gone",
                    format!("no control master for {}; not sent", host.name),
                ));
            }
            return crate::errors::EXIT_NO_MASTER;
        }
    }
}

/// One op's reply from its frame: the host's account of how it ended wins
/// over what was asked of it.
fn op_reply(f: &Frame, id: u64, cancel_requested: bool, batch: u64, size: usize) -> Reply {
    let (kind, error) = match f.ended {
        Ended::Cancelled => (Some("cancelled"), None),
        Ended::TimedOut => (
            Some("timeout"),
            Some(format!(
                "exceeded the {}s bounded-op limit; \
                 run it as a detached job (mule run, mule exec)",
                op_secs()
            )),
        ),
        Ended::Lost => (
            Some("lost"),
            Some("the op ended on the host without a result".into()),
        ),
        Ended::Completed if f.truncated => (
            Some("output_limit"),
            Some(format!(
                "output passed the host's cap ({MAX_OUT} bytes of stdout, \
                 {MAX_SMALL} for a stat, a write or a read's stderr) and was \
                 cut; read it in parts"
            )),
        ),
        Ended::Unspooled => (
            Some("spool_failed"),
            Some(format!(
                "the op exited {} but the host could not spool its output whole \
                 (disk full?); a write may have landed",
                f.rc
            )),
        ),
        Ended::Uncleaned => (
            Some("cleanup_failed"),
            Some(format!(
                "the op ended (rc {}) but the host could not prove its processes \
                 gone; something it started may still run",
                f.rc
            )),
        ),
        Ended::Completed => (None, None),
    };
    let rc = if matches!(f.ended, Ended::Unspooled | Ended::Uncleaned) {
        -1
    } else {
        f.rc
    };
    Reply {
        id,
        rc,
        stdout_b64: B64.encode(&f.stdout),
        stderr: String::from_utf8_lossy(&f.stderr).into_owned(),
        truncated: f.truncated,
        cancelled: f.ended == Ended::Cancelled,
        cancel_requested,
        error,
        kind,
        batch,
        batch_size: size,
    }
}

/// CLI entry: `mule serve --stdio --host H`.
pub fn run_cli(host: &Host) -> Result<i32> {
    crate::lock::set_quiet(true);
    crate::errors::require_master(&crate::transport::Ssh, host)?;
    let code = serve(
        Arc::new(crate::transport::Ssh),
        host.clone(),
        std::io::BufReader::new(std::io::stdin()),
        std::io::stdout(),
    );
    if code == crate::errors::EXIT_NO_MASTER {
        eprintln!("mule: {}", crate::errors::no_master(host));
    }
    Ok(code)
}
