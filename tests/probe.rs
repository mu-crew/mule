use std::path::PathBuf;
use std::time::Duration;

use coop::config::Host;
use coop::probe::{State, next_interval, probe};
use coop::transport::{Fake, Output};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// `lock_path` honours `$XDG_STATE_HOME`, and without this the suite writes one
/// directory per test run into the developer's real `~/.local/state/coop`. A
/// full run left 151 of them behind, mixed in with live job state.
///
/// `set_var` is safe here because it runs once, before any thread that reads it.
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
    Host {
        name: format!("probe-test-{}", std::process::id()),
        target: "dev".into(),
        socket: PathBuf::from("/tmp/coop.sock"),
        tmux_socket: "coop".into(),
        max_running: 4,
        default_cwd: None,
        keep_days: 14,
    }
}

#[test]
fn maps_rc_and_session_presence_to_job_state() {
    isolate_state();
    let cases = [
        ("rc=0\nalive=0\nsize=0\nbytes:\n", State::Done(0)),
        ("rc=3\nalive=0\nsize=0\nbytes:\n", State::Done(3)),
        ("rc=\nalive=1\nsize=0\nbytes:\n", State::Running),
        ("rc=\nalive=0\nsize=0\nbytes:\n", State::Orphan),
        ("rc=7\nalive=1\nsize=0\nbytes:\n", State::Done(7)),
    ];

    for (reply, expected) in cases {
        let fake = Fake::new();
        fake.push(Output::ok(reply));
        assert_eq!(
            probe(&fake, &host(), &"abc123".parse().unwrap(), 0)
                .unwrap()
                .state,
            expected
        );
    }
}

#[test]
fn preserves_arbitrary_log_bytes_after_the_header() {
    isolate_state();
    let bytes = b"rc=\nalive=1\nsize=22\nbytes:\nrc=9\nbytes:\n\xff\0tail";
    let fake = Fake::new();
    fake.push(Output::ok(bytes.as_slice()));

    let result = probe(&fake, &host(), &"abc123".parse().unwrap(), 17).unwrap();

    assert_eq!(result.log_size, 22);
    assert_eq!(result.bytes, b"rc=9\nbytes:\n\xff\0tail");
    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 1);
    assert!(scripts[0].contains("tail -c +18"));
}

#[test]
fn probe_is_one_remote_round_trip() {
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok("rc=\nalive=1\nsize=0\nbytes:\n"));

    probe(&fake, &host(), &"abc123".parse().unwrap(), 0).unwrap();

    assert_eq!(fake.scripts().len(), 1);
}

#[test]
fn polling_interval_backs_off_to_five_seconds_and_resets_on_output() {
    isolate_state();
    assert_eq!(
        next_interval(Duration::from_secs(1), false),
        Duration::from_secs(2)
    );
    assert_eq!(
        next_interval(Duration::from_secs(2), false),
        Duration::from_secs(4)
    );
    assert_eq!(
        next_interval(Duration::from_secs(4), false),
        Duration::from_secs(5)
    );
    assert_eq!(
        next_interval(Duration::from_secs(5), false),
        Duration::from_secs(5)
    );
    assert_eq!(
        next_interval(Duration::from_secs(5), true),
        Duration::from_secs(1)
    );
}

#[test]
fn state_only_fetches_no_log_bytes() {
    // A plain `wait` wants rc, not output. Shipping the log to discard it would
    // hold the lock for the transfer.
    //
    // This was a real bug: the caller expressed "no bytes" as an offset of
    // u64::MAX, the +1 that `tail -c +N` needs overflowed, and the remote
    // command became `tail -c +18446744073709551615` -> "Invalid argument".
    // Every `--wait --no-tail` then failed with a spurious "lost contact while
    // waiting", which reads as a network problem.
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok("rc=0\nalive=0\nsize=12\nbytes:\n"));

    let result = probe(
        &fake,
        &host(),
        &"abc123".parse().unwrap(),
        coop::probe::From::StateOnly,
    )
    .unwrap();

    assert_eq!(result.state, State::Done(0));
    assert!(result.bytes.is_empty());
    let script = &fake.scripts()[0];
    assert!(
        !script.contains("tail -c"),
        "a state-only probe must not ask for log bytes: {script}"
    );
    assert!(
        !script.contains("18446744073709551615"),
        "no sentinel offset may reach the remote shell: {script}"
    );
    // The size field still arrives, so a later tail knows where to start.
    assert_eq!(result.log_size, 12);
}
