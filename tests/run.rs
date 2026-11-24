use std::io::Cursor;

use mule::config::Config;
use mule::run::Dispatched;
use mule::transport::Transport;
use mule::transport::{Fake, Output};
use mule::wrapper::{Job, JobId, JobMetadata, JobMode};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// `lock_path` honours `$XDG_STATE_HOME`, and without this the suite writes
/// lock directories into the developer's real `~/.local/state/mule`, mixed in
/// with live job state.
///
/// `set_var` is safe here because it runs once, before any thread reads it.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

/// A plain pipe job under a fresh id, as `mule run` dispatches one.
fn dispatch(
    t: &dyn Transport,
    cmd: &str,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<JobId> {
    let host = host();
    let job = Job {
        id: mule::wrapper::new_id().parse().unwrap(),
        cmd: cmd.into(),
        cwd: None,
        max_secs: host.max_job_secs,
        metadata: JobMetadata::Human,
        mode: JobMode::Pipe,
    };
    mule::run::dispatch(t, &host, job, None, warnings).map(Dispatched::into_id)
}

fn host() -> mule::config::Host {
    Config::parse(
        "[hosts.dev]\ntarget = \"build.example\"\nsocket = \"/tmp/mule-dev.sock\"\nmax_running = 4\n",
    )
    .unwrap()
    .host(None)
    .unwrap()
    .clone()
}

#[test]
fn dispatch_is_one_round_trip_and_returns_the_job_id() {
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok("1\n"));

    let id = dispatch(&fake, "echo hi", &mut std::io::sink()).unwrap();
    let scripts = fake.scripts();

    assert_eq!(id.as_str().len(), 6);
    assert!(id.as_str().bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(scripts.len(), 1, "dispatch must stay one round trip");
    assert!(scripts[0].contains("new-session -d"));
    assert!(scripts[0].contains(id.as_str()));
}

#[test]
fn no_master_error_prints_the_exact_command_to_open_one() {
    isolate_state();
    let error = dispatch(&Fake::no_master(), "true", &mut std::io::sink()).unwrap_err();
    let message = error.to_string();

    assert!(message.contains("no control master for dev"));
    assert!(message.contains("ssh -MNf -S /tmp/mule-dev.sock -o ControlPersist=8h -o StreamLocalBindUnlink=yes build.example"));
}

#[test]
fn zero_running_sessions_is_a_successful_dispatch_reply() {
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok("0\n"));

    assert!(dispatch(&fake, "true", &mut std::io::sink()).is_ok());
    // The count is advisory: one it cannot take is `?` and no warning, never
    // a failed dispatch of a job that has started.
    assert!(
        fake.scripts()[0].contains("|| echo '?'"),
        "an untaken count must not fail the dispatch"
    );
    let fake = Fake::new();
    fake.push(Output::ok("?\n"));
    assert!(dispatch(&fake, "true", &mut std::io::sink()).is_ok());
}

#[test]
fn warns_only_when_the_retrospective_count_exceeds_the_cap() {
    isolate_state();
    let above = Fake::new();
    above.push(Output::ok("5\n"));
    let mut warning = Cursor::new(Vec::new());
    let id = dispatch(&above, "true", &mut warning).unwrap();
    assert_eq!(
        String::from_utf8(warning.into_inner()).unwrap(),
        format!("mule: dispatched {id}; 5 now running on dev, cap 4\n")
    );

    let at_cap = Fake::new();
    at_cap.push(Output::ok("4\n"));
    let mut warning = Cursor::new(Vec::new());
    dispatch(&at_cap, "true", &mut warning).unwrap();
    assert!(warning.into_inner().is_empty());
}
