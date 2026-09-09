//! Config loading: a hand-edited TOML file, and the host-name-derived defaults
//! that make a minimal entry two lines.

use coop::config::Config;

/// A temp dir unique to this test binary run, so concurrent runs cannot collide.
fn tmp(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("coop-cfg-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn write_config(tag: &str, body: &str) -> std::path::PathBuf {
    let p = tmp(tag).join("config.toml");
    std::fs::write(&p, body).unwrap();
    p
}

/// The whole error chain, not just the outermost context.
///
/// `to_string()` on an `anyhow::Error` renders only the top frame, so a test
/// asserting on the cause silently checks the wrong string. `{:#}` is what the
/// user actually sees, so it is what the test should read.
fn chain(e: anyhow::Error) -> String {
    format!("{e:#}")
}

#[test]
fn minimal_host_entry_gets_defaults() {
    let p = write_config("min", "[hosts.bubba]\ntarget = \"bubba\"\n");
    let c = Config::load(&p).unwrap();
    let h = c.host(Some("bubba")).unwrap();

    assert_eq!(h.target, "bubba");
    assert_eq!(h.tmux_socket, "coop");
    assert_eq!(h.max_running, 4);
    assert_eq!(h.keep_days, 14);
    assert_eq!(h.default_cwd, None);

    let sock = h.socket.to_string_lossy();
    assert!(sock.ends_with(".ssh/coop/bubba.sock"), "got {sock}");
    // `~` must be expanded at load time: it reaches ssh -S as a real path, and
    // ssh does not expand it for us.
    assert!(!sock.starts_with('~'), "~ must be expanded, got {sock}");

    std::fs::remove_dir_all(tmp("min")).ok();
}

#[test]
fn target_defaults_to_the_section_name() {
    // Two lines is the documented minimum, but even `target` is derivable.
    let p = write_config("named", "[hosts.dev]\n");
    let c = Config::load(&p).unwrap();
    assert_eq!(c.host(Some("dev")).unwrap().target, "dev");
    std::fs::remove_dir_all(tmp("named")).ok();
}

#[test]
fn explicit_values_override_every_default() {
    let p = write_config(
        "over",
        r#"
[hosts.dev]
target      = "dev.example.com"
socket      = "/tmp/custom.sock"
tmux_socket = "mine"
max_running = 9
default_cwd = "~/work"
keep_days   = 30
"#,
    );
    let c = Config::load(&p).unwrap();
    let h = c.host(Some("dev")).unwrap();
    assert_eq!(h.target, "dev.example.com");
    assert_eq!(h.socket.to_string_lossy(), "/tmp/custom.sock");
    assert_eq!(h.tmux_socket, "mine");
    assert_eq!(h.max_running, 9);
    assert_eq!(h.default_cwd.as_deref(), Some("~/work"));
    assert_eq!(h.keep_days, 30);
    std::fs::remove_dir_all(tmp("over")).ok();
}

#[test]
fn single_host_needs_no_name() {
    let p = write_config("one", "[hosts.only]\n");
    let c = Config::load(&p).unwrap();
    assert_eq!(c.host(None).unwrap().name, "only");
    std::fs::remove_dir_all(tmp("one")).ok();
}

#[test]
fn ambiguous_host_names_the_choices() {
    let p = write_config("two", "[hosts.dev]\n[hosts.bubba]\n");
    let c = Config::load(&p).unwrap();
    let err = chain(c.host(None).unwrap_err());
    // Naming the choices is the whole value of the error: the fix is one flag
    // away and the user should not have to open the config to learn the names.
    assert!(err.contains("dev"), "{err}");
    assert!(err.contains("bubba"), "{err}");
    assert!(err.contains("--host"), "{err}");
    std::fs::remove_dir_all(tmp("two")).ok();
}

#[test]
fn unknown_host_names_the_choices_too() {
    let p = write_config("unk", "[hosts.dev]\n");
    let c = Config::load(&p).unwrap();
    let err = chain(c.host(Some("nope")).unwrap_err());
    assert!(err.contains("nope"), "{err}");
    assert!(err.contains("dev"), "{err}");
    std::fs::remove_dir_all(tmp("unk")).ok();
}

#[test]
fn hosts_are_listed_in_a_stable_order() {
    // `coop host list` output should not reshuffle between invocations.
    let p = write_config("order", "[hosts.zed]\n[hosts.alpha]\n[hosts.mid]\n");
    let c = Config::load(&p).unwrap();
    let names: Vec<_> = c.hosts().iter().map(|h| h.name.as_str()).collect();
    assert_eq!(names, ["alpha", "mid", "zed"]);
    std::fs::remove_dir_all(tmp("order")).ok();
}

#[test]
fn missing_file_says_where_it_looked() {
    let p = tmp("gone").join("nope.toml");
    let err = chain(Config::load(&p).unwrap_err());
    assert!(err.contains("nope.toml"), "{err}");
    std::fs::remove_dir_all(tmp("gone")).ok();
}

#[test]
fn a_config_with_no_hosts_is_an_error_not_an_empty_success() {
    let p = write_config("empty", "# nothing here\n");
    let err = chain(Config::load(&p).unwrap_err());
    assert!(err.contains("no hosts"), "{err}");
    std::fs::remove_dir_all(tmp("empty")).ok();
}
