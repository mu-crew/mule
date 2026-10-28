//! Test layer 1 for `mule exec`: the logic, over `Fake`.
//!
//! A `Fake` returns canned output whatever script it is handed, so these tests
//! cannot see a wrong script -- `tests/exec_tmux.rs` runs the real wrapper and
//! the real collect script for that. What is checked here is what only logic
//! decides: which stream goes where, what is silent, the exit code, the cadence.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use mule::config::{Config, Host};
use mule::errors::{EXIT_TRUNCATED, MuleError};
use mule::exec::{Request, finish, finish_within, next_interval, start};
use mule::transport::{Fake, Output};
use mule::wrapper::JobId;

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-exec-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Config::parse("[hosts.dev]\ntarget = \"build.example\"\nsocket = \"/tmp/mule.sock\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn id() -> JobId {
    "a1b2c3".parse().unwrap()
}

fn state(state: &str) -> Output {
    Output::ok(format!("exists=1\n{state}\nsize=0\nbytes:\n"))
}

/// The collect round trip's reply: header, then log bytes, then err bytes.
fn collected(truncated: bool, log: &str, err: &str) -> Output {
    Output::ok(format!(
        "trunc={}\nlive=0\nlog={}\nerr={}\nbytes:\n{log}{err}",
        u8::from(truncated),
        log.len(),
        err.len()
    ))
}

fn no_pause() -> impl Fn(Duration) {
    |_| {}
}

#[test]
fn stdout_is_the_log_and_stderr_is_the_err_file_and_nothing_else_is_printed() {
    let fake = Fake::new();
    fake.push(state("rc=3\nalive=0"));
    fake.push(collected(false, "the answer\n", "boom: why\n"));
    let (mut out, mut err) = (Vec::new(), Vec::new());

    let code = finish(&fake, &host(), &id(), &no_pause(), &mut out, &mut err).unwrap();

    assert_eq!(code, 3, "exec exits with the job's own code");
    assert_eq!(String::from_utf8(out).unwrap(), "the answer\n");
    // No id line, no `next:` hints: stderr is the command's, byte for byte, so
    // a caller reading its first line gets the command's reason.
    assert_eq!(String::from_utf8(err).unwrap(), "boom: why\n");
}

#[test]
fn a_clean_run_prints_nothing_to_stderr() {
    let fake = Fake::new();
    fake.push(state("rc=0\nalive=0"));
    fake.push(collected(false, "ok\n", ""));
    let (mut out, mut err) = (Vec::new(), Vec::new());

    assert_eq!(
        finish(&fake, &host(), &id(), &no_pause(), &mut out, &mut err).unwrap(),
        0
    );
    assert!(err.is_empty(), "{:?}", String::from_utf8_lossy(&err));
}

#[test]
fn start_dispatches_one_silent_round_trip_even_over_the_running_cap() {
    let fake = Fake::new();
    // 99 running sessions against max_running = 4 would print a warning in
    // `run`; exec must say nothing.
    fake.push(Output::ok("99\n"));

    start(
        &fake,
        &host(),
        id(),
        &Request {
            cmd: "git rev-parse HEAD",
            cwd: None,
            max_secs: None,
        },
    )
    .unwrap();

    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 1, "dispatch stays one round trip");
    assert!(scripts[0].contains("new-session -d -s mule-a1b2c3"));
    // The wrapper mode, not the merged-log one.
    assert!(
        scripts[0].contains("2>> "),
        "exec splits stderr: {}",
        scripts[0]
    );
}

#[test]
fn start_without_a_master_is_the_exit_three_error() {
    let error = start(
        &Fake::no_master(),
        &host(),
        id(),
        &Request {
            cmd: "true",
            cwd: None,
            max_secs: None,
        },
    )
    .unwrap_err();

    assert!(matches!(
        error.downcast_ref::<MuleError>(),
        Some(MuleError::NoMaster { .. })
    ));
    assert_eq!(mule::errors::exit_code(&error), 3);
}

#[test]
fn a_truncated_log_exits_seven_instead_of_the_job_code() {
    let fake = Fake::new();
    fake.push(state("rc=0\nalive=0"));
    fake.push(collected(true, "partial", ""));
    let (mut out, mut err) = (Vec::new(), Vec::new());

    let code = finish(&fake, &host(), &id(), &no_pause(), &mut out, &mut err).unwrap();

    assert_eq!(code, EXIT_TRUNCATED);
    assert_eq!(code, 7);
    // The partial output is still delivered; the code says it is partial.
    assert_eq!(String::from_utf8(out).unwrap(), "partial");
    assert!(String::from_utf8(err).unwrap().contains("max_log_bytes"));
}

#[test]
fn the_job_is_removed_in_the_same_round_trip_that_reads_it() {
    let fake = Fake::new();
    fake.push(state("rc=0\nalive=0"));
    fake.push(collected(false, "", ""));

    finish(
        &fake,
        &host(),
        &id(),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap();

    let scripts = fake.scripts();
    let last = scripts.last().unwrap();
    assert!(last.contains("rm -rf \"$d\""), "{last}");
    assert!(last.contains("kill-session -t mule-a1b2c3"), "{last}");
    assert!(last.contains("kill-session -t watch-a1b2c3"), "{last}");
    assert_eq!(scripts.len(), 2, "one probe, one collect");
}

#[test]
fn polling_starts_at_100ms_and_never_exceeds_one_second() {
    let fake = Fake::new();
    for _ in 0..12 {
        fake.push(state("rc=\nalive=1"));
    }
    fake.push(state("rc=0\nalive=0"));
    fake.push(collected(false, "", ""));
    let pauses = RefCell::new(Vec::new());

    finish(
        &fake,
        &host(),
        &id(),
        &|interval| pauses.borrow_mut().push(interval),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap();

    let pauses = pauses.into_inner();
    assert_eq!(pauses.first(), Some(&Duration::from_millis(100)));
    assert!(pauses.len() > 6);
    assert!(
        pauses.iter().all(|p| *p <= Duration::from_secs(1)),
        "{pauses:?}"
    );
    assert_eq!(
        pauses.last(),
        Some(&Duration::from_secs(1)),
        "settles at the cap, not the 5s detached-job backoff"
    );
    assert!(pauses.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn the_interval_doubles_to_a_one_second_cap() {
    let mut interval = Duration::from_millis(100);
    let mut seen = vec![interval];
    for _ in 0..6 {
        interval = next_interval(interval);
        seen.push(interval);
    }
    assert_eq!(
        seen,
        [100, 200, 400, 800, 1000, 1000, 1000].map(Duration::from_millis)
    );
}

#[test]
fn a_missing_job_and_an_orphan_are_errors_not_output() {
    let fake = Fake::new();
    fake.push(Output::ok("exists=0\nrc=\nalive=0\nsize=0\nbytes:\n"));
    let error = finish(
        &fake,
        &host(),
        &id(),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<MuleError>(),
        Some(MuleError::MissingJob { .. })
    ));

    let fake = Fake::new();
    fake.push(state("rc=\nalive=0"));
    let error = finish(
        &fake,
        &host(),
        &id(),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();
    assert_eq!(mule::errors::exit_code(&error), 5);
}

#[test]
fn a_dropped_connection_is_a_fatal_error_that_names_the_host() {
    let fake = Fake::new();
    fake.push(Output::fail(255, "Connection reset"));

    let error = finish(
        &fake,
        &host(),
        &id(),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("dev"), "{error:#}");
}

#[test]
fn argv_commands_keep_their_boundaries_for_env_prefixed_shells() {
    // `mule exec -- env K=v sh -c 'echo "$K"'`: several words, so each is quoted
    // and the remote shell sees the same argv the caller typed.
    let args: Vec<String> = ["env", "K=v", "sh", "-c", "echo \"$K\" \"it's here\""]
        .map(String::from)
        .to_vec();
    let command = mule::cli::command_from_args(&args);

    let output = std::process::Command::new("sh")
        .args(["-c", &command])
        .output()
        .unwrap();

    assert_eq!(String::from_utf8_lossy(&output.stdout), "v it's here\n");
}

/// The collect reply when the job's session was still alive at read time.
fn collected_live(log: &str) -> Output {
    Output::ok(format!(
        "trunc=0\nlive=1\nlog={}\nerr=0\nbytes:\n{log}",
        log.len()
    ))
}

#[test]
fn a_session_that_outlives_rc_is_waited_for_in_probes_not_inside_the_collect() {
    let fake = Fake::new();
    // rc landed, but the log pipeline has not drained: two probes see it.
    fake.push(state("rc=0\nalive=1"));
    fake.push(state("rc=0\nalive=1"));
    fake.push(state("rc=0\nalive=0"));
    fake.push(collected(false, "all\n", ""));
    let (mut out, mut err) = (Vec::new(), Vec::new());

    let code = finish(&fake, &host(), &id(), &no_pause(), &mut out, &mut err).unwrap();

    assert_eq!((code, String::from_utf8(out).unwrap()), (0, "all\n".into()));
    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 4, "three short probes, then one collect");
    // A sleep inside the locked collect held the ticket ~14s in the field.
    let collect = scripts.last().unwrap();
    assert!(!collect.contains("sleep"), "{collect}");
    assert!(!collect.contains("while"), "{collect}");
}

#[test]
fn a_session_still_alive_at_collect_is_exit_seven_not_the_job_code_beside_a_partial_log() {
    // A background child holds stdout: the session never ends. Probes keep
    // saying `alive=1` until the settle bound passes; the next call is the
    // collect. The bound is wall-clock, so the pause really sleeps and the
    // probe count is not fixed -- the reply is chosen by what was asked.
    struct ByScript(Fake);
    impl mule::transport::Transport for ByScript {
        fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
            self.0.run_unlocked(host, script)?;
            Ok(if script.contains("live=0; tmux") {
                collected_live("")
            } else {
                state("rc=0\nalive=1")
            })
        }
        fn mux_op(&self, host: &Host, op: mule::transport::MuxOp) -> anyhow::Result<Output> {
            self.0.mux_op(host, op)
        }
    }
    let fake = ByScript(Fake::new());
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let started = Instant::now();

    let code = finish_within(
        &fake,
        &host(),
        &id(),
        Duration::from_millis(500),
        &std::thread::sleep,
        &mut out,
        &mut err,
    )
    .unwrap();

    assert_eq!(code, 7, "never rc 0 with a possibly-empty log");
    let err = String::from_utf8(err).unwrap();
    assert!(err.contains("output incomplete"), "{err}");
    assert!(err.contains("stdout"), "{err}");
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(500) && waited < Duration::from_millis(1500),
        "waits a bounded wall-clock time for the session: {waited:?}"
    );
    assert!(
        fake.0.scripts().last().unwrap().contains("rm -rf \"$d\""),
        "the job is still removed"
    );
}

#[test]
fn the_settle_bound_counts_probe_time_not_just_requested_pauses() {
    // A probe that takes real time must eat into the bound: with a no-op pause
    // the only time passing is the probes' own, and the bound still ends the
    // wait. Each probe costs 3x SETTLE_INTERVAL, so a bound that counted only
    // requested pauses would need 500ms / 100ms = 5 probes (1.5s) where the
    // wall-clock bound needs 2 (600ms).
    struct Slow(Fake);
    impl mule::transport::Transport for Slow {
        fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
            std::thread::sleep(Duration::from_millis(300));
            self.0.run_unlocked(host, script)
        }
        fn mux_op(&self, host: &Host, op: mule::transport::MuxOp) -> anyhow::Result<Output> {
            self.0.mux_op(host, op)
        }
    }
    let fake = Fake::new();
    for _ in 0..100 {
        fake.push(state("rc=0\nalive=1"));
    }
    let slow = Slow(fake);
    let started = Instant::now();

    // Pauses are no-ops; each probe costs 300ms. 500ms of bound is two probes
    // by the clock, five by a count of requested 100ms pauses.
    let _ = finish_within(
        &slow,
        &host(),
        &id(),
        Duration::from_millis(500),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    );

    // The probe count is the deterministic discriminator (2 by the clock, 5 by
    // a pause count); elapsed time is only a loose sanity bound, since a busy
    // machine stretches every 300ms probe.
    let waited = started.elapsed();
    assert!(
        waited < Duration::from_secs(5),
        "bound ignored probe time: {waited:?}"
    );
    let probes = slow.0.scripts().len();
    assert!(probes <= 3, "bound ignored probe time: {probes} probes");
}

#[test]
fn a_failed_removal_is_a_fatal_exit_one_naming_it_never_the_commands_code() {
    let fake = Fake::new();
    fake.push(state("rc=0\nalive=0"));
    // The collect script's removal step failed: its own status, with the proof
    // script's reason on stderr.
    fake.push(Output {
        stdout: b"trunc=0\nlive=0\nlog=3\nerr=0\nbytes:\nhi\n".to_vec(),
        stderr: "state dir /x could not be removed (kept)\n".into(),
        code: 97,
    });

    let error = finish(
        &fake,
        &host(),
        &id(),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap_err();

    let text = format!("{error:#}");
    assert!(text.contains("could not remove"), "{text}");
    assert!(text.contains("could not be removed"), "{text}");
    assert_eq!(mule::errors::exit_code(&error), 1);
}

#[test]
fn the_collect_script_removes_with_the_rm_proof_not_a_bare_rm() {
    let fake = Fake::new();
    fake.push(state("rc=0\nalive=0"));
    fake.push(collected(false, "", ""));

    finish(
        &fake,
        &host(),
        &id(),
        &no_pause(),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .unwrap();

    let scripts = fake.scripts();
    let collect = scripts.last().unwrap();
    assert!(
        collect.contains("list-sessions"),
        "removal is verified by seeing absence: {collect}"
    );
    assert!(collect.contains("cannot verify"), "{collect}");
}
