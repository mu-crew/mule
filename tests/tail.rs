use std::io::Cursor;

use coop::config::{Config, Host};
use coop::errors::{CoopError, EXIT_DROPPED, EXIT_ORPHAN, EXIT_TIMEOUT, exit_code};
use coop::tail::{Selection, follow, follow_deferred, once};
use coop::transport::{Fake, Output};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// `lock_path` honours `$XDG_STATE_HOME`, and without this the suite writes
/// lock directories into the developer's real `~/.local/state/coop`, mixed in
/// with live job state.
///
/// `set_var` is safe here because it runs once, before any thread reads it.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("coop-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Config::parse("[hosts.dev]\ntarget = \"build.example\"\nsocket = \"/tmp/coop.sock\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn reply(state: &str, size: u64, bytes: &[u8]) -> Output {
    let mut stdout = format!("{state}\nsize={size}\nbytes:\n").into_bytes();
    stdout.extend_from_slice(bytes);
    Output::ok(stdout)
}

#[test]
fn follow_streams_each_byte_once_and_returns_the_job_rc() {
    isolate_state();
    let fake = Fake::new();
    fake.push(reply("rc=\nalive=1", 1, b"a"));
    fake.push(reply("rc=\nalive=1", 2, b"b"));
    fake.push(reply("rc=3\nalive=0", 2, b""));
    // The terminal re-read on the done path: nothing further arrived.
    fake.push(reply("rc=3\nalive=0", 2, b""));
    let mut out = Cursor::new(Vec::new());

    let code = follow(&fake, &host(), &"abc123".parse().unwrap(), 0, &mut out).unwrap();

    assert_eq!(code, 3);
    assert_eq!(out.into_inner(), b"ab");
    let scripts = fake.scripts();
    assert!(scripts[0].contains("tail -c +1"));
    assert!(scripts[1].contains("tail -c +2"));
    assert!(scripts[2].contains("tail -c +3"));
}

#[test]
fn follow_does_not_trust_an_overlapping_reported_size_as_the_offset() {
    isolate_state();
    let fake = Fake::new();
    fake.push(reply("rc=\nalive=1", 20, b"a"));
    fake.push(reply("rc=0\nalive=0", 20, b"b"));
    fake.push(reply("rc=0\nalive=0", 20, b"")); // terminal re-read
    let mut out = Cursor::new(Vec::new());

    follow(&fake, &host(), &"abc123".parse().unwrap(), 7, &mut out).unwrap();

    assert_eq!(out.into_inner(), b"ab");
    assert!(fake.scripts()[1].contains("tail -c +9"));
}

#[test]
fn follow_reports_an_orphan_instead_of_inventing_an_exit_code() {
    isolate_state();
    let fake = Fake::new();
    fake.push(reply("rc=\nalive=0", 4, b"last"));
    let mut out = Cursor::new(Vec::new());

    let error = follow(&fake, &host(), &"dead42".parse().unwrap(), 0, &mut out).unwrap_err();

    assert!(matches!(
        error.downcast_ref::<CoopError>(),
        Some(CoopError::Orphan { id }) if id == "dead42"
    ));
    assert_eq!(exit_code(&error), EXIT_ORPHAN);
    assert_eq!(out.into_inner(), b"last");
}

#[test]
fn follow_names_the_resume_command_after_a_connection_error() {
    isolate_state();
    let fake = Fake::new();
    fake.push(reply("rc=\nalive=1", 1, b"a"));
    fake.push(Output::fail(255, "connection reset"));
    let mut out = Cursor::new(Vec::new());

    let error = follow(&fake, &host(), &"abc123".parse().unwrap(), 0, &mut out).unwrap_err();

    assert!(matches!(
        error.downcast_ref::<CoopError>(),
        Some(CoopError::Dropped { id }) if id == "abc123"
    ));
    assert_eq!(exit_code(&error), EXIT_DROPPED);
    assert!(error.to_string().contains("coop tail abc123"));
}

#[test]
fn deferred_follow_writes_the_log_only_after_completion() {
    isolate_state();
    let fake = Fake::new();
    fake.push(reply("rc=\nalive=1", 1, b"a"));
    fake.push(reply("rc=0\nalive=0", 2, b"b"));
    fake.push(Output::ok("ab"));
    let mut out = Cursor::new(Vec::new());

    assert_eq!(
        follow_deferred(&fake, &host(), &"abc123".parse().unwrap(), &mut out).unwrap(),
        0
    );
    assert_eq!(out.into_inner(), b"ab");
    assert!(fake.scripts()[2].contains("cat"));
}

#[test]
fn one_shot_tail_limits_the_remote_read_before_taking_stdout() {
    isolate_state();
    let cases = [
        (Selection::LastBytes, "tail -c 65536"),
        (Selection::All, "cat"),
        (Selection::Lines(12), "tail -n 12"),
    ];

    for (selection, command) in cases {
        let fake = Fake::new();
        fake.push(Output::ok([0, 0xff, b'x']));
        let mut out = Cursor::new(Vec::new());

        once(
            &fake,
            &host(),
            &"abc123".parse().unwrap(),
            selection,
            &mut out,
        )
        .unwrap();

        assert_eq!(out.into_inner(), [0, 0xff, b'x']);
        assert!(fake.scripts()[0].contains(command));
    }
}

#[test]
fn a_job_that_finishes_within_one_probe_still_prints_its_output() {
    // `rc` and `log` are written by different ends of a pipeline, so `rc` can
    // land while the log's last bytes are still in flight -- measured, with the
    // log file not yet created. Returning on the first `Done` therefore dropped
    // the output of any job short enough to finish inside one probe interval,
    // which is most of them: `coop run --wait ls` printed the id and nothing.
    isolate_state();
    let fake = Fake::new();
    // First probe: already done, and no bytes yet.
    fake.push(Output::ok("rc=0\nalive=0\nsize=6\nbytes:\n"));
    // The terminal re-read finds them.
    fake.push(Output::ok("rc=0\nalive=0\nsize=6\nbytes:\nlate\n"));

    let mut out = Vec::new();
    let code = coop::tail::follow(&fake, &host(), &"abc123".parse().unwrap(), 0, &mut out).unwrap();

    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8_lossy(&out),
        "late\n",
        "output arriving with rc must not be lost"
    );
    assert_eq!(fake.scripts().len(), 2, "one extra read on the done path");
}

#[test]
fn a_zero_timeout_returns_a_typed_timeout_after_one_state_probe() {
    isolate_state();
    let fake = Fake::new();
    fake.push(reply("rc=\nalive=1", 0, b""));
    let id = "abc123".parse().unwrap();

    let error = coop::tail::wait_only(&fake, &host(), &id, Some(0)).unwrap_err();

    assert!(matches!(
        error.downcast_ref::<CoopError>(),
        Some(CoopError::Timeout { id }) if id == "abc123"
    ));
    assert_eq!(exit_code(&error), EXIT_TIMEOUT);
    assert_eq!(fake.scripts().len(), 1);
}

#[test]
fn a_plain_wait_does_not_pay_for_the_terminal_read() {
    // `wait` prints nothing, so the extra round trip would be pure cost on the
    // channel it is meant to protect.
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok("rc=2\nalive=0\nsize=0\nbytes:\n"));

    let code = coop::tail::wait_only(&fake, &host(), &"abc123".parse().unwrap(), None).unwrap();

    assert_eq!(code, 2);
    assert_eq!(fake.scripts().len(), 1, "no terminal read when not tailing");
}
