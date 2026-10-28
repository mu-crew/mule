//! Layer 1 for the signal infrastructure and the detached-cleanup command.
//!
//! The behaviour end to end (a real signal, a real sshd) is in
//! `tests/local_sshd.rs`; these pin the pieces that decide it.

use std::ffi::OsStr;
use std::path::Path;
use std::time::{Duration, Instant};

use mule::cleanup::{Signals, exit_status_for, rm_command};
use mule::wrapper::JobId;

fn args(command: &std::process::Command) -> Vec<&OsStr> {
    command.get_args().collect()
}

#[test]
fn the_cleanup_child_gets_the_effective_config_quiet_host_and_the_id() {
    let id: JobId = "abc123".parse().unwrap();
    let command = rm_command(
        Path::new("/opt/mule"),
        Some(Path::new("/tmp/private.toml")),
        "dev",
        &id,
    );

    assert_eq!(command.get_program(), "/opt/mule");
    assert_eq!(
        args(&command),
        [
            "--config",
            "/tmp/private.toml",
            "--quiet",
            "rm",
            "--host",
            "dev",
            "abc123"
        ]
    );
}

#[test]
fn a_relative_config_is_made_absolute_so_the_child_reads_the_same_file() {
    let id: JobId = "abc123".parse().unwrap();
    let command = rm_command(
        Path::new("/opt/mule"),
        Some(Path::new("c.toml")),
        "dev",
        &id,
    );

    let config = Path::new(args(&command)[1]);
    assert!(config.is_absolute(), "{config:?}");
    assert!(config.ends_with("c.toml"));
}

#[test]
fn no_config_flag_means_the_child_is_not_given_one() {
    let id: JobId = "abc123".parse().unwrap();
    let command = rm_command(Path::new("/opt/mule"), None, "dev", &id);

    assert_eq!(
        args(&command),
        ["--quiet", "rm", "--host", "dev", "abc123"],
        "an absent --config stays absent; the child resolves the default itself"
    );
}

#[test]
fn the_program_is_this_executable_and_never_a_path_lookup() {
    let id: JobId = "abc123".parse().unwrap();
    let exe = std::env::current_exe().unwrap();

    let command = rm_command(&exe, None, "dev", &id);

    assert!(Path::new(command.get_program()).is_absolute());
    assert_ne!(command.get_program(), "mule");
}

#[test]
fn exit_statuses_follow_the_shell_convention() {
    assert_eq!(exit_status_for(libc::SIGHUP), 129);
    assert_eq!(exit_status_for(libc::SIGINT), 130);
    assert_eq!(exit_status_for(libc::SIGTERM), 143);
}

#[test]
fn a_signal_sets_the_flag_and_wakes_a_sleeper_early() {
    // SIGUSR2 so the test cannot be mistaken for, or kill, a real stop signal.
    let signals = Signals::install(&[libc::SIGUSR2]).unwrap();
    assert_eq!(signals.pending(), None);

    let started = Instant::now();
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(100));
        unsafe { libc::raise(libc::SIGUSR2) };
    });
    let seen = signals.wait(Some(Duration::from_secs(10)));

    assert_eq!(seen, Some(libc::SIGUSR2));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the sleep must be interruptible"
    );
    assert_eq!(signals.pending(), Some(libc::SIGUSR2), "sticky");
}

#[test]
fn waiting_without_a_signal_times_out_with_none() {
    let signals = Signals::install(&[libc::SIGUSR1]).unwrap();
    let started = Instant::now();

    assert_eq!(signals.wait(Some(Duration::from_millis(50))), None);
    assert!(started.elapsed() >= Duration::from_millis(40));
}
