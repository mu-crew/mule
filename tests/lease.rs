//! Test layer 1 for `mule run --lease`: the logic, over `Fake`.
//!
//! `Fake` returns canned probe replies whatever script it is handed, so these
//! tests cannot see a wrong script -- `tests/local_sshd.rs` drives the real
//! lease binary against a capped sshd for that. What is pinned here is what
//! only logic decides: the job file before the dispatch, the exit for each
//! terminal state, holding on through a lost master, drift repair, the mirror
//! window, and the order of the signal path.

use std::collections::HashMap;
use std::io::Write;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mule::config::{Config, Host};
use mule::errors::{EXIT_ORPHAN, MuleError};
use mule::forward::{Spec, read_record};
use mule::lease::{Hold, Request, SignalSteps, hold, on_signal, start, write_job_file};
use mule::transport::{Fake, MuxOp, Output, Transport};
use mule::wrapper::{JobId, JobMetadata};

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-lease-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Safe: runs once, before any thread reads it.
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Config::parse("[hosts.lease]\ntarget = \"h\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn id() -> JobId {
    "a1b2c3".parse().unwrap()
}

/// A short directory: unix socket paths are capped near 104 bytes on macOS.
fn scratch() -> PathBuf {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let dir = PathBuf::from(format!(
        "/tmp/ml-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

type Listeners = Arc<Mutex<HashMap<PathBuf, UnixListener>>>;

/// A master that binds LOCAL on forward and drops the listener on cancel.
fn master(fake: &Fake) -> Listeners {
    let listeners: Listeners = Arc::default();
    let held = Arc::clone(&listeners);
    fake.on_mux(move |op| match op {
        MuxOp::ForwardUnix(local, _) => {
            if let Ok(listener) = UnixListener::bind(local) {
                held.lock().unwrap().insert(local.clone(), listener);
            }
        }
        MuxOp::CancelUnix(local, _) => {
            held.lock().unwrap().remove(local);
        }
        MuxOp::Check => {}
    });
    listeners
}

fn spec(local: &Path) -> Spec {
    Spec::parse(&format!("{}:/remote/ctl.sock", local.display())).unwrap()
}

fn running() -> Output {
    Output::ok("exists=1\nrc=\nalive=1\nsize=0\nbytes:\nscreen text\n")
}

fn done(code: i32) -> Output {
    Output::ok(format!(
        "exists=1\nrc={code}\nalive=0\nsize=0\nbytes:\nfinal screen\n"
    ))
}

fn forwards(fake: &Fake) -> usize {
    fake.mux_ops()
        .iter()
        .filter(|op| matches!(op, MuxOp::ForwardUnix(..)))
        .count()
}

fn lease<'a>(host: &'a Host, id: &'a JobId, specs: &'a [Spec]) -> Hold<'a> {
    Hold {
        host,
        id,
        specs,
        mirror_for: Duration::from_secs(3600),
    }
}

fn request() -> Request<'static> {
    Request {
        cmd: "pi",
        cwd: None,
        max_secs: None,
        metadata: JobMetadata::Human,
    }
}

// -- the job file ---------------------------------------------------------------

/// Reads the job file at the instant the dispatch script runs.
struct SeesJobFile {
    fake: Fake,
    file: PathBuf,
    seen: Mutex<Option<String>>,
}

impl Transport for SeesJobFile {
    fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
        *self.seen.lock().unwrap() = std::fs::read_to_string(&self.file).ok();
        self.fake.run_unlocked(host, script)
    }
    fn mux_op(&self, host: &Host, op: MuxOp) -> anyhow::Result<Output> {
        self.fake.mux_op(host, op)
    }
}

#[test]
fn the_job_file_names_the_job_before_the_dispatch_runs() {
    let dir = scratch();
    let file = dir.join("job");
    let t = SeesJobFile {
        fake: Fake::new(),
        file: file.clone(),
        seen: Mutex::default(),
    };
    t.fake.push(Output::ok("1\n"));

    let id = start(
        &t,
        &host(),
        &file,
        &request(),
        &|_| {},
        &mut std::io::sink(),
    )
    .unwrap();

    let seen = t.seen.lock().unwrap().clone();
    assert_eq!(
        seen.as_deref(),
        Some(format!("{id}\n").as_str()),
        "the id was on disk when the dispatch ran"
    );
    let scripts = t.fake.scripts();
    assert_eq!(scripts.len(), 1);
    assert!(scripts[0].contains(&format!("mule-{id}")));
    assert!(
        scripts[0].contains("echo tui >"),
        "dispatched exactly as run --tui"
    );
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let leftovers: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
    assert_eq!(leftovers.len(), 1, "no temp file left beside the job file");
}

#[test]
fn a_job_file_that_cannot_be_written_dispatches_nothing() {
    let fake = Fake::new();
    let file = scratch().join("no-such-dir").join("job");

    let error = start(
        &fake,
        &host(),
        &file,
        &request(),
        &|_| panic!("nothing may be dispatched"),
        &mut std::io::sink(),
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("job file"), "{error:#}");
    assert_eq!(mule::errors::exit_code(&error), 1);
    assert!(fake.scripts().is_empty(), "no dispatch");
    assert!(fake.mux_ops().is_empty(), "not even a master check");
}

#[test]
fn no_master_at_startup_is_exit_three_with_the_job_named_but_never_dispatched() {
    let fake = Fake::no_master();
    let file = scratch().join("job");

    let error = start(
        &fake,
        &host(),
        &file,
        &request(),
        &|_| {},
        &mut std::io::sink(),
    )
    .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<MuleError>(),
        Some(MuleError::NoMaster { .. })
    ));
    assert_eq!(mule::errors::exit_code(&error), 3);
    assert!(fake.scripts().is_empty());
    // Named anyway: `mule rm` of a never-dispatched id is a no-op, so the
    // stop template stays safe to run.
    let named = std::fs::read_to_string(&file).unwrap();
    assert!(named.trim().parse::<JobId>().is_ok(), "{named:?}");
}

#[test]
fn a_rewritten_job_file_is_replaced_whole() {
    let file = scratch().join("job");
    std::fs::write(&file, "stale contents that are longer\n").unwrap();
    write_job_file(&file, &id()).unwrap();
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "a1b2c3\n");
}

// -- terminal states --------------------------------------------------------------

fn run_to_end(fake: &Fake, specs: &[Spec]) -> (i32, String) {
    let host = host();
    let id = id();
    let mut out = Vec::new();
    let code = hold(fake, &lease(&host, &id, specs), &mut |_| {}, &mut out);
    (code, String::from_utf8_lossy(&out).into_owned())
}

#[test]
fn a_finished_job_prints_its_final_screen_unforwards_and_exits_with_its_code() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    let specs = [spec(&local)];
    fake.push(running()).push(done(42));

    let (code, out) = run_to_end(&fake, &specs);

    assert_eq!(code, 42);
    assert!(out.contains("final screen"), "{out}");
    assert!(!local.exists(), "unforwarded: LOCAL is gone");
    assert!(read_record(&host(), &local).is_none(), "and its record");
    assert!(matches!(fake.mux_ops().last(), Some(MuxOp::CancelUnix(..))));
}

#[test]
fn an_orphan_unforwards_and_exits_five() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    fake.push(Output::ok("exists=1\nrc=\nalive=0\nsize=0\nbytes:\n"));

    let (code, out) = run_to_end(&fake, &[spec(&local)]);

    assert_eq!(code, EXIT_ORPHAN);
    assert!(out.contains("orphan"), "{out}");
    assert!(!local.exists());
}

#[test]
fn a_missing_job_unforwards_and_exits_one() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    fake.push(Output::ok("exists=0\nrc=\nalive=0\nsize=0\nbytes:\n"));

    let (code, out) = run_to_end(&fake, &[spec(&local)]);

    assert_eq!(code, 1);
    assert!(out.contains("not found"), "{out}");
    assert!(!local.exists());
}

#[test]
fn forwards_are_made_before_the_first_probe_without_waiting_for_remote() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    fake.push(done(0));

    run_to_end(&fake, &[spec(&local)]);

    let ops = fake.mux_ops();
    let first_forward = ops
        .iter()
        .position(|op| matches!(op, MuxOp::ForwardUnix(..)))
        .expect("forwarded");
    // Every probe is a `run`; the forward came before the first one, and no
    // script checks whether REMOTE exists.
    assert!(first_forward < 4, "{ops:?}");
    assert!(
        fake.scripts().iter().all(|s| !s.contains("ctl.sock")),
        "REMOTE is not waited for"
    );
}

// -- holding on -------------------------------------------------------------------

#[test]
fn a_lost_master_neither_exits_nor_stops_the_job_and_re_forwards_on_the_new_pid() {
    let fake = Fake::new().with_master_pid(100);
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    let specs = [spec(&local)];
    let host = host();
    let id = id();
    fake.push(running());

    let mut ticks = 0;
    let mut intervals = Vec::new();
    let mut out = Vec::new();
    let code = hold(
        &fake,
        &lease(&host, &id, &specs),
        &mut |interval| {
            ticks += 1;
            intervals.push(interval);
            match ticks {
                // `ssh -O exit`: the old master and its forwards are gone.
                1 => {
                    fake.set_master(None);
                    let _ = std::fs::remove_file(&local);
                }
                // A human reopened it: a new pid, with none of the old forwards.
                6 => {
                    fake.set_master(Some(200));
                    fake.push(done(0));
                }
                _ => {}
            }
        },
        &mut out,
    );
    let out = String::from_utf8_lossy(&out);

    assert_eq!(code, 0, "it exited only when the job did: {out}");
    assert!(ticks >= 6, "it held through the outage");
    assert_eq!(
        out.matches("is gone; a human must").count(),
        1,
        "the hand-back line is printed once: {out}"
    );
    assert!(out.contains("ssh -MNf"), "{out}");
    // While the master was gone: 5s checks, and no locked call at all -- in
    // particular nothing that stops the job.
    assert!(
        intervals[1..5].iter().all(|i| *i == Duration::from_secs(5)),
        "{intervals:?}"
    );
    assert_eq!(fake.scripts().len(), 2, "one probe before, one after");
    assert!(fake.scripts().iter().all(|s| !s.contains("kill-session")));
    assert_eq!(forwards(&fake), 2, "re-forwarded once on the new master");
}

#[test]
fn a_dropped_probe_holds_on_and_says_so_once() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    // An unparseable reply is what a connection dropped mid-probe looks like.
    fake.push(running())
        .push(Output::fail(255, "Connection reset"))
        .push(Output::fail(255, "Connection reset"))
        .push(running())
        .push(done(3));

    let (code, out) = run_to_end(&fake, &[spec(&local)]);

    assert_eq!(code, 3);
    assert_eq!(out.matches("lost contact").count(), 1, "{out}");
}

#[test]
fn drift_re_forwards_a_missing_local_a_new_inode_and_a_new_master_pid() {
    let fake = Fake::new().with_master_pid(1);
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    let specs = [spec(&local)];
    let host = host();
    let id = id();
    for _ in 0..4 {
        fake.push(running());
    }
    fake.push(done(0));

    let mut ticks = 0;
    let mut after_tick = Vec::new();
    hold(
        &fake,
        &lease(&host, &id, &specs),
        &mut |_| {
            after_tick.push(forwards(&fake));
            ticks += 1;
            match ticks {
                // Somebody deleted LOCAL.
                1 => std::fs::remove_file(&local).unwrap(),
                // Somebody replaced LOCAL with a socket that is not ours.
                2 => {
                    std::fs::remove_file(&local).unwrap();
                    std::mem::forget(UnixListener::bind(&local).unwrap());
                }
                // The master was replaced between ticks.
                3 => fake.set_master(Some(2)),
                _ => {}
            }
        },
        &mut std::io::sink(),
    );

    assert_eq!(after_tick, [1, 2, 3, 4], "one re-forward per kind of drift");
    let record = read_record(&host, &local);
    assert!(record.is_none(), "unforwarded at the end");
}

#[test]
fn an_undrifted_forward_is_not_touched() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    for _ in 0..5 {
        fake.push(running());
    }
    fake.push(done(0));

    run_to_end(&fake, &[spec(&local)]);

    assert_eq!(forwards(&fake), 1);
}

#[test]
fn a_wedge_seen_while_holding_prints_the_hand_back_and_keeps_holding() {
    let fake = Fake::new();
    let local = scratch().join("c.sock");
    // A master whose table is wedged: forward exits 0 and binds nothing.
    fake.on_mux(|_| {});
    fake.push(running()).push(running()).push(done(0));

    let (code, out) = run_to_end(&fake, &[spec(&local)]);

    assert_eq!(code, 0, "held until the job ended: {out}");
    assert_eq!(out.matches("wedged").count(), 1, "said once: {out}");
    assert!(out.contains("-O exit"), "{out}");
    assert!(forwards(&fake) >= 2, "kept trying to repair");
}

// -- the screen -------------------------------------------------------------------

#[test]
fn the_status_line_names_the_job_host_forwards_attach_and_stop() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    fake.push(done(0));

    let (_, out) = run_to_end(&fake, &[spec(&local)]);

    assert!(out.contains("a1b2c3"), "{out}");
    assert!(out.contains("on lease"), "{out}");
    assert!(out.contains(&format!("{} -> /remote/ctl.sock", local.display())));
    assert!(
        out.contains("murmur pick --all  # select mule-a1b2c3"),
        "{out}"
    );
    assert!(out.contains("mule rm --host lease a1b2c3"), "{out}");
}

#[test]
fn the_screen_is_mirrored_in_the_window_and_after_it_only_on_change() {
    // Inside the window every probe carries the capture.
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    fake.push(running()).push(running()).push(done(0));
    run_to_end(&fake, &[spec(&local)]);
    assert!(
        fake.scripts().iter().all(|s| s.contains("capture-pane")),
        "mirrored while the window is open"
    );

    // Past it, a quiet tick probes state only; the final screen still prints.
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    let specs = [spec(&local)];
    let host = host();
    let id = id();
    fake.push(running())
        .push(running())
        .push(running())
        .push(Output::ok("exists=1\nrc=0\nalive=0\nsize=0\nbytes:\n"))
        .push(done(0));
    let mut out = Vec::new();
    let code = hold(
        &fake,
        &Hold {
            host: &host,
            id: &id,
            specs: &specs,
            mirror_for: Duration::ZERO,
        },
        &mut |_| {},
        &mut out,
    );
    assert_eq!(code, 0);
    let scripts = fake.scripts();
    let captures: Vec<bool> = scripts.iter().map(|s| s.contains("capture-pane")).collect();
    // starting -> running is a change of state, so the next tick carries one
    // screen; a quiet tick after it does not; the final screen is fetched at
    // exit with one extra probe.
    assert_eq!(captures, [false, true, false, false, true], "{scripts:#?}");
    assert!(String::from_utf8_lossy(&out).contains("final screen"));
}

/// A writer whose every write fails, as a hung-up tty or a closed pipe does.
struct Dead;

impl Write for Dead {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::from_raw_os_error(libc::EIO))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::from_raw_os_error(libc::EIO))
    }
}

#[test]
fn a_dead_stdout_neither_panics_nor_ends_the_hold() {
    let fake = Fake::new();
    let _master = master(&fake);
    let local = scratch().join("c.sock");
    fake.push(running()).push(running()).push(done(9));
    let host = host();
    let id = id();
    let specs = [spec(&local)];

    let code = hold(&fake, &lease(&host, &id, &specs), &mut |_| {}, &mut Dead);

    assert_eq!(code, 9);
}

// -- signals ----------------------------------------------------------------------

#[derive(Default)]
struct Recorded(Vec<&'static str>);

impl SignalSteps for Recorded {
    fn redirect_stdio(&mut self) {
        self.0.push("redirect");
    }
    fn spawn_stop(&mut self) {
        self.0.push("stop");
    }
    fn unforward(&mut self) {
        self.0.push("unforward");
    }
}

#[test]
fn the_signal_path_redirects_then_stops_then_unforwards_then_exits() {
    for (signal, code) in [
        (libc::SIGHUP, 129),
        (libc::SIGINT, 130),
        (libc::SIGTERM, 143),
    ] {
        let mut steps = Recorded::default();
        assert_eq!(on_signal(&mut steps, signal), code);
        assert_eq!(steps.0, ["redirect", "stop", "unforward"]);
    }
}
