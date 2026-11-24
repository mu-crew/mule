//! `ls` encodes each job's command in awk: layer 2, the real script run here.
//!
//! The encoder indexes bytes, so it must not run under a locale where awk
//! counts characters. gawk in a UTF-8 locale does (default awk on Fedora, Arch
//! and RHEL hosts, which ssh hands the caller's `LANG` to); mawk and BSD awk
//! are bytewise whatever the locale. This Mac and the Linux CI image have no
//! gawk, so the contract is pinned twice: with an `awk` that refuses to run
//! outside the C locale, and with the real gawk where one is installed.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::local::{ShimHost, scratch, stderr, stdout, tmux_available};

/// Two- and three-byte characters: one byte-indexed `ord` lookup per byte
/// decodes them only if awk indexed bytes.
const COMMAND: &str = "true # é€ ü";

fn which(tool: &str) -> Option<PathBuf> {
    let out = std::process::Command::new("sh")
        .args(["-c", &format!("command -v {tool}")])
        .output()
        .ok()?;
    let path = String::from_utf8(out.stdout).ok()?.trim().to_string();
    out.status.success().then(|| PathBuf::from(path))
}

/// Dispatch `COMMAND`, then list it with `awk_dir` ahead of the real awk and a
/// UTF-8 login locale, as an ssh session onto a gawk host would have.
fn listed_command(tag: &str, awk_dir: Option<&Path>) -> Option<String> {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return None;
    }
    let host = ShimHost::new(tag);
    let id = stdout(&host.mule(&["run", "--quiet", COMMAND]));
    let out = host
        .command(&["ls", "--json"])
        .env("PATH", host.path_env(awk_dir))
        .env("LANG", "en_US.UTF-8")
        .env_remove("LC_ALL")
        .env_remove("LC_CTYPE")
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let row = value["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == id.as_str())
        .unwrap_or_else(|| panic!("{id} not listed: {value}"));
    Some(row["cmd"].as_str().unwrap().to_string())
}

#[test]
fn a_multibyte_command_lists_intact_under_the_default_awk_and_a_utf8_locale() {
    if let Some(cmd) = listed_command("lsloc-default", None) {
        assert_eq!(cmd, COMMAND);
    }
}

#[test]
fn the_listing_awk_runs_in_the_c_locale() {
    let bin = scratch("lsloc-shim");
    let Some(real) = which("awk") else {
        eprintln!("skipping: no awk");
        return;
    };
    // Stands in for gawk: a character-counting awk would corrupt the encoding,
    // so this one fails whenever the locale it inherits is not byte-oriented.
    let shim = bin.join("awk");
    common::local::write_exec(
        &shim,
        format!(
            "#!/bin/sh\n\
             case ${{LC_ALL:-${{LC_CTYPE:-$LANG}}}} in\n\
               *[Uu][Tt][Ff]-8*|*[Uu][Tt][Ff]8*) echo 'awk: counts characters here' >&2; exit 3;;\n\
             esac\n\
             exec '{}' \"$@\"\n",
            real.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    if let Some(cmd) = listed_command("lsloc-c", Some(&bin)) {
        assert_eq!(cmd, COMMAND);
    }
}

#[test]
fn a_multibyte_command_lists_intact_under_gawk_in_a_utf8_locale() {
    let Some(gawk) = which("gawk") else {
        eprintln!("skipping: gawk is not installed");
        return;
    };
    let bin = scratch("lsloc-gawk");
    std::os::unix::fs::symlink(gawk, bin.join("awk")).unwrap();
    if let Some(cmd) = listed_command("lsloc-gawk", Some(&bin)) {
        assert_eq!(cmd, COMMAND);
    }
}
