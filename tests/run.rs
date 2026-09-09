use std::io::Cursor;

use coop::config::Config;
use coop::transport::{Fake, Output};

fn host() -> coop::config::Host {
    Config::parse(
        "[hosts.dev]\ntarget = \"build.example\"\nsocket = \"/tmp/coop-dev.sock\"\nmax_running = 4\n",
    )
    .unwrap()
    .host(None)
    .unwrap()
    .clone()
}

#[test]
fn dispatch_is_one_round_trip_and_returns_the_job_id() {
    let fake = Fake::new();
    fake.push(Output::ok("1\n"));

    let id = coop::run::dispatch(&fake, &host(), "echo hi", None).unwrap();
    let scripts = fake.scripts();

    assert_eq!(id.len(), 6);
    assert!(id.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(scripts.len(), 1, "dispatch must stay one round trip");
    assert!(scripts[0].contains("new-session -d"));
    assert!(scripts[0].contains(&id));
}

#[test]
fn no_master_error_prints_the_exact_command_to_open_one() {
    let error = coop::run::dispatch(&Fake::no_master(), &host(), "true", None).unwrap_err();
    let message = error.to_string();

    assert!(message.contains("no control master for dev"));
    assert!(message.contains("ssh -MNf -S /tmp/coop-dev.sock -o ControlPersist=8h build.example"));
}

#[test]
fn zero_running_sessions_is_a_successful_dispatch_reply() {
    let fake = Fake::new();
    fake.push(Output::ok("0\n"));

    assert!(coop::run::dispatch(&fake, &host(), "true", None).is_ok());
    assert!(
        fake.scripts()[0].contains("grep -c '^coop-' || true"),
        "grep reports no matches with status 1; the combined dispatch must normalize it"
    );
}

#[test]
fn warns_only_when_the_retrospective_count_exceeds_the_cap() {
    let above = Fake::new();
    above.push(Output::ok("5\n"));
    let mut warning = Cursor::new(Vec::new());
    let id =
        coop::run::dispatch_with_warnings(&above, &host(), "true", None, &mut warning).unwrap();
    assert_eq!(
        String::from_utf8(warning.into_inner()).unwrap(),
        format!("coop: dispatched {id}; 5 now running on dev, cap 4\n")
    );

    let at_cap = Fake::new();
    at_cap.push(Output::ok("4\n"));
    let mut warning = Cursor::new(Vec::new());
    coop::run::dispatch_with_warnings(&at_cap, &host(), "true", None, &mut warning).unwrap();
    assert!(warning.into_inner().is_empty());
}
