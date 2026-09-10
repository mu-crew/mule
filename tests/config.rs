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

#[test]
fn a_hostile_tmux_socket_is_rejected() {
    // `tmux_socket` is interpolated unquoted into every remote script, so shell
    // syntax here would be command injection from a config file. tmux socket
    // names are a filename component, so the restriction costs nothing real.
    for hostile in [
        "coop; touch /tmp/PWNED",
        "$(id)",
        "`id`",
        "a b",
        "a'b",
        "a\"b",
        "a|b",
        "a&&b",
        "../escape",
        "",
    ] {
        let body = format!(
            "[hosts.dev]\ntmux_socket = \"{}\"\n",
            hostile.escape_debug()
        );
        assert!(
            Config::parse(&body).is_err(),
            "must reject tmux_socket {hostile:?}"
        );
    }
}

#[test]
fn ordinary_tmux_socket_names_are_accepted() {
    for ok in ["coop", "coop-test-123", "coop_2", "coop.alt"] {
        let body = format!("[hosts.dev]\ntmux_socket = \"{ok}\"\n");
        assert_eq!(
            Config::parse(&body)
                .unwrap()
                .host(None)
                .unwrap()
                .tmux_socket,
            ok
        );
    }
}

#[test]
fn seeding_writes_a_template_and_never_clobbers() {
    let dir = tmp("seed");
    let p = dir.join("config.toml");
    std::fs::remove_file(&p).ok();

    assert!(
        coop::config::seed(&p).unwrap(),
        "should create on first call"
    );
    assert!(
        !coop::config::seed(&p).unwrap(),
        "must not report a second create"
    );

    // Idempotent and non-destructive: a real config must survive a stray seed.
    std::fs::write(&p, "[hosts.mine]\n").unwrap();
    assert!(!coop::config::seed(&p).unwrap());
    assert_eq!(std::fs::read_to_string(&p).unwrap(), "[hosts.mine]\n");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn seeding_creates_missing_parent_directories() {
    let dir = tmp("seed-deep");
    let p = dir.join("nested").join("deeper").join("config.toml");
    assert!(coop::config::seed(&p).unwrap());
    assert!(p.exists());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn the_template_is_a_valid_but_empty_config() {
    // As written it configures nothing, so coop still says "no hosts" rather
    // than inventing a host that does not exist.
    let err = chain(Config::parse(coop::config::TEMPLATE).unwrap_err());
    assert!(err.contains("no hosts"), "{err}");
}

#[test]
fn the_template_parses_once_uncommented() {
    // A template that produces a broken config is worse than no template. An
    // earlier draft had two `[hosts.build]` blocks, so following the
    // instructions literally gave `duplicate key` -- caught only by doing it.
    let uncommented: String = coop::config::TEMPLATE
        .lines()
        .filter_map(|line| line.strip_prefix("# "))
        // Keep only the config lines, dropping prose and the shell example
        // (which contains `=` inside `ControlPersist=8h`).
        .filter(|line| {
            let line = line.trim_start();
            line.starts_with('[')
                || line
                    .split_once('=')
                    .is_some_and(|(key, _)| !key.trim().contains(' '))
        })
        .collect::<Vec<_>>()
        .join("\n");

    let cfg = Config::parse(&uncommented)
        .unwrap_or_else(|e| panic!("uncommented template must parse: {e:#}\n{uncommented}"));
    let host = cfg.host(Some("build")).unwrap();
    assert_eq!(host.target, "build");
    assert_eq!(host.tmux_socket, "coop");
    assert_eq!(host.max_running, 4);
    assert_eq!(host.keep_days, 14);
    assert_eq!(host.default_cwd.as_deref(), Some("~/work"));
}
