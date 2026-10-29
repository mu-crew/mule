use std::path::PathBuf;
use std::time::Duration;

use mule::config::Host;
use mule::probe::{State, next_interval, probe};
use mule::transport::{Fake, Output};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// `lock_path` honours `$XDG_STATE_HOME`, and without this the suite writes one
/// directory per test run into the developer's real `~/.local/state/mule`. A
/// full run left 151 of them behind, mixed in with live job state.
///
/// `set_var` is safe here because it runs once, before any thread that reads it.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Host {
        name: format!("probe-test-{}", std::process::id()),
        target: "dev".into(),
        socket: PathBuf::from("/tmp/mule.sock"),
        tmux_socket: "mule".into(),
        max_running: 4,
        default_cwd: None,
        keep_days: 14,
        max_log_bytes: 100 * 1024 * 1024,
        max_job_secs: 0,
    }
}

#[test]
fn maps_rc_and_session_presence_to_job_state() {
    isolate_state();
    let cases = [
        ("exists=1\nrc=0\nalive=0\nsize=0\nbytes:\n", State::Done(0)),
        ("exists=1\nrc=3\nalive=0\nsize=0\nbytes:\n", State::Done(3)),
        ("exists=1\nrc=\nalive=1\nsize=0\nbytes:\n", State::Running),
        ("exists=1\nrc=\nalive=0\nsize=0\nbytes:\n", State::Orphan),
        ("exists=0\nrc=\nalive=0\nsize=0\nbytes:\n", State::Missing),
        ("exists=1\nrc=7\nalive=1\nsize=0\nbytes:\n", State::Done(7)),
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
    let bytes = b"exists=1\nrc=\nalive=1\nsize=22\nbytes:\nrc=9\nbytes:\n\xff\0tail";
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
    fake.push(Output::ok("exists=1\nrc=\nalive=1\nsize=0\nbytes:\n"));

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
    fake.push(Output::ok("exists=1\nrc=0\nalive=0\nsize=12\nbytes:\n"));

    let result = probe(
        &fake,
        &host(),
        &"abc123".parse().unwrap(),
        mule::probe::From::StateOnly,
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

#[test]
fn poll_does_not_download_the_log() {
    // `poll` discards bytes, so requesting them would ship the whole log
    // through the one session channel while holding the lock. A 200MB log would
    // make the cheapest verb in the tool the one that starves everything.
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok(
        "exists=1\nrc=\nalive=1\nsize=209715200\nbytes:\n",
    ));

    mule::cli::poll(&fake, &host(), &"abc123".parse().unwrap(), false).unwrap();

    let script = &fake.scripts()[0];
    assert!(
        !script.contains("tail -c"),
        "poll must not read log bytes: {script}"
    );
}

#[test]
fn a_screen_probe_carries_the_pane_capture_in_the_same_round_trip() {
    // The lease mirrors the remote screen while mu's startup scan reads it.
    // Folding the capture into the probe keeps that one locked call per tick
    // rather than two.
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok(
        "exists=1\nrc=\nalive=1\nsize=900\nbytes:\npi ready\n> \n",
    ));

    let result = probe(
        &fake,
        &host(),
        &"abc123".parse().unwrap(),
        mule::probe::From::Screen,
    )
    .unwrap();

    assert_eq!(result.state, State::Running);
    assert_eq!(result.bytes, b"pi ready\n> \n");
    let scripts = fake.scripts();
    assert_eq!(scripts.len(), 1, "one locked call, capture included");
    let script = &scripts[0];
    assert!(
        script.contains("capture-pane -p -J -t mule-abc123"),
        "{script}"
    );
    // A pane that is gone falls back to the saved final screen, never to the
    // raw transcript (TUI-jobs spec, `mule tail` contract).
    assert!(script.contains("/screen"), "{script}");
    assert!(!script.contains("tail -c"), "no log bytes: {script}");
}

/// Runs the real probe script under `sh`, with a `tmux` shim first on PATH.
struct Shimmed {
    path: String,
    state: PathBuf,
}

impl mule::transport::Transport for Shimmed {
    fn run_unlocked(&self, _: &Host, script: &str) -> anyhow::Result<Output> {
        let out = std::process::Command::new("sh")
            .args(["-c", script])
            .env("PATH", &self.path)
            .env("XDG_STATE_HOME", &self.state)
            .stdin(std::process::Stdio::null())
            .output()?;
        Ok(Output {
            stdout: out.stdout,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        })
    }

    fn mux_op(&self, _: &Host, _: mule::transport::MuxOp) -> anyhow::Result<Output> {
        Ok(Output::ok(""))
    }
}

#[test]
fn a_job_that_ends_between_the_reads_is_done_not_orphaned() {
    // Every path that ends a job writes `rc` before its session goes, so the
    // probe must look at the session first and `rc` second. Read the other way
    // round, a job finishing between the two reads shows "no rc, no session":
    // an orphan. Under three parallel layer-3 suites the lease read a job
    // that exited 7 as orphan (exit 5) in 3 of 3 runs, with rc=7 on disk a
    // moment later.
    //
    // The shim makes that interleaving certain: its `has-session` is the
    // instant the job ends -- it writes `rc`, then reports the session gone.
    isolate_state();
    let root = std::env::temp_dir().join(format!("mule-probe-race-{}", std::process::id()));
    let state = root.join("state");
    let id: mule::wrapper::JobId = "abc123".parse().unwrap();
    let job = state.join("mule/jobs").join(id.as_str());
    std::fs::create_dir_all(&job).unwrap();
    std::fs::write(job.join("cmd"), "exit 7\n").unwrap();
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let shim = bin.join("tmux");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh\ncase \"$*\" in *has-session*) echo 7 > '{}'; exit 1;; esac\nexit 1\n",
            job.join("rc").display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    let transport = Shimmed {
        path: format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
        state,
    };

    let result = probe(&transport, &host(), &id, mule::probe::From::StateOnly);
    let _ = std::fs::remove_dir_all(&root);

    assert_eq!(result.unwrap().state, State::Done(7));
}
