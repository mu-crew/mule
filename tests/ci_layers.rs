//! With `MULE_REQUIRE_LAYERS` set, fail if any layer 2 or 3 test would skip.
//!
//! Those tests return early, printing "skipping", when tmux, zsh, `which` or
//! `/usr/sbin/sshd` is missing, or when running as root makes file modes
//! unenforceable. That is right on a laptop, but on CI it means a green run
//! that tested nothing the layers exist for. CI sets the variable, so this one
//! test checks every skip condition at once, rather than each skip site
//! learning to panic.

mod common;

use std::process::Command;

fn on_path(tool: &str, version_flag: &str) -> bool {
    Command::new(tool).arg(version_flag).output().is_ok()
}

#[test]
fn every_layer_can_run_when_required() {
    if std::env::var_os("MULE_REQUIRE_LAYERS").is_none() {
        return;
    }
    let mut missing = Vec::new();
    if !common::local::tmux_available() {
        missing.push("tmux is not on PATH".to_string());
    }
    if !on_path("zsh", "--version") {
        missing.push("zsh is not on PATH".to_string());
    }
    if !on_path("which", "which") {
        missing.push("`which` is not on PATH".to_string());
    }
    if let Some(reason) = common::sshd::unavailable() {
        missing.push(reason);
    }
    // SAFETY: geteuid has no preconditions and cannot fail.
    if unsafe { libc::geteuid() } == 0 {
        missing.push("running as root, so file modes are not enforced".to_string());
    }
    assert!(
        missing.is_empty(),
        "MULE_REQUIRE_LAYERS is set, but these layer 2/3 tests would skip: {}",
        missing.join("; ")
    );
}
