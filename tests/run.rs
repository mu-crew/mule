use std::io::Cursor;

use mule::config::Config;
use mule::transport::{Fake, Output};
use mule::wrapper::JobMetadata;

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

    let id = mule::run::dispatch(
        &fake,
        &host(),
        "echo hi",
        None,
        None,
        JobMetadata::Human,
        mule::wrapper::JobMode::Pipe,
    )
    .unwrap();
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
    let error = mule::run::dispatch(
        &Fake::no_master(),
        &host(),
        "true",
        None,
        None,
        JobMetadata::Human,
        mule::wrapper::JobMode::Pipe,
    )
    .unwrap_err();
    let message = error.to_string();

    assert!(message.contains("no control master for dev"));
    assert!(message.contains("ssh -MNf -S /tmp/mule-dev.sock -o ControlPersist=8h build.example"));
}

#[test]
fn zero_running_sessions_is_a_successful_dispatch_reply() {
    isolate_state();
    let fake = Fake::new();
    fake.push(Output::ok("0\n"));

    assert!(
        mule::run::dispatch(
            &fake,
            &host(),
            "true",
            None,
            None,
            JobMetadata::Human,
            mule::wrapper::JobMode::Pipe,
        )
        .is_ok()
    );
    assert!(
        fake.scripts()[0].contains("grep -c '^mule-' || true"),
        "grep reports no matches with status 1; the combined dispatch must normalize it"
    );
}

#[test]
fn warns_only_when_the_retrospective_count_exceeds_the_cap() {
    isolate_state();
    let above = Fake::new();
    above.push(Output::ok("5\n"));
    let mut warning = Cursor::new(Vec::new());
    let id = mule::run::dispatch_with_warnings(
        &above,
        &host(),
        "true",
        None,
        None,
        JobMetadata::Human,
        mule::wrapper::JobMode::Pipe,
        &mut warning,
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(warning.into_inner()).unwrap(),
        format!("mule: dispatched {id}; 5 now running on dev, cap 4\n")
    );

    let at_cap = Fake::new();
    at_cap.push(Output::ok("4\n"));
    let mut warning = Cursor::new(Vec::new());
    mule::run::dispatch_with_warnings(
        &at_cap,
        &host(),
        "true",
        None,
        None,
        JobMetadata::Human,
        mule::wrapper::JobMode::Pipe,
        &mut warning,
    )
    .unwrap();
    assert!(warning.into_inner().is_empty());
}
