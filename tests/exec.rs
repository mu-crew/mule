//! Test layer 1 for `mule exec`: the logic, over `Fake`.
//!
//! A `Fake` returns canned output whatever script it is handed, so these tests
//! cannot see a wrong script -- `tests/exec_tmux.rs` runs the real wrapper and
//! the real collect script for that. What is checked here is what only logic
//! decides: which stream goes where, what is silent, the exit code, the cadence.

use std::cell::RefCell;
use std::time::Duration;

use mule::config::{Config, Host};
use mule::errors::{EXIT_TRUNCATED, MuleError};
use mule::exec::{Request, finish, next_interval, start};
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
        "trunc={}\nlog={}\nerr={}\nbytes:\n{log}{err}",
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
    assert!(last.contains("rm -rf $d"), "{last}");
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
