//! Test layer 2 for remote scripts: run them for real under `sh`, on this
//! machine, against a private tmux server.
//!
//! The `Fake` transport answers whatever it was told to, so it cannot see a
//! wrong script. [`Local`] executes the exact script mule built, with
//! `$XDG_STATE_HOME` pointed at a per-test directory, so the job state it
//! writes is files a test can read and break. No sshd, no master: layer 3
//! covers the channel; this covers the shell.

#![allow(dead_code)] // Each test binary uses a different subset.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};

use mule::config::{Config, Host};
use mule::transport::{MuxOp, Output, Transport};

/// Point this test binary's own lock directory at a temp dir, once.
///
/// `Transport::run` takes the ticket lock under `$XDG_STATE_HOME`, and
/// without this every run would write lock directories into the developer's
/// real `~/.local/state/mule`. `set_var` is safe here because it runs once,
/// before any thread reads it.
pub fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-local-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

/// Is tmux here at all? Layer 2 skips, saying so, when it is not.
pub fn tmux_available() -> bool {
    Command::new("tmux").arg("-V").output().is_ok()
}

/// A fresh scratch directory under the temp dir.
pub fn scratch(tag: &str) -> PathBuf {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "mule-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A private tmux server name, killed (socket file included) on drop.
pub struct LocalTmux {
    pub socket: String,
}

impl LocalTmux {
    pub fn new(tag: &str) -> Self {
        static SEQ: AtomicU32 = AtomicU32::new(0);
        Self {
            socket: format!(
                "mule-l2-{}-{tag}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ),
        }
    }

    /// The configured host using this server.
    pub fn host(&self) -> Host {
        isolate_state();
        Config::parse(&format!(
            "[hosts.local]\ntarget = \"local\"\ntmux_socket = \"{}\"\n",
            self.socket
        ))
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
    }

    /// `tmux -L <this server> <args>`.
    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new("tmux");
        command.args(["-L", &self.socket]).args(args);
        command
    }

    pub fn has_session(&self, name: &str) -> bool {
        Command::new("tmux")
            .args(["-L", &self.socket, "has-session", "-t", name])
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }
}

impl Drop for LocalTmux {
    fn drop(&mut self) {
        // Ask for the socket path before killing, as layer 3 does: kill-server
        // leaves the file behind.
        let path = Command::new("tmux")
            .args([
                "-L",
                &self.socket,
                "display-message",
                "-p",
                "#{socket_path}",
            ])
            .stderr(Stdio::null())
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|p| !p.is_empty());
        let _ = Command::new("tmux")
            .args(["-L", &self.socket, "kill-server"])
            .stderr(Stdio::null())
            .status();
        if let Some(path) = path {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Runs every script under `sh -c` here, with `$XDG_STATE_HOME` at `state`.
/// The master is always up: there is none to lose.
pub struct Local {
    pub state: PathBuf,
    /// Prepended to `PATH`, for a test that shims a remote tool.
    pub path_prefix: Option<PathBuf>,
    /// The "remote" login shell. `sh` by default; a real host's may be zsh,
    /// whose glob and splitting rules differ.
    pub shell: &'static str,
}

impl Local {
    pub fn new(state: &Path) -> Self {
        Self {
            state: state.to_path_buf(),
            path_prefix: None,
            shell: "sh",
        }
    }

    /// The jobs root the scripts write under.
    pub fn jobs(&self) -> PathBuf {
        self.state.join("mule/jobs")
    }
}

impl Transport for Local {
    fn run_unlocked(&self, _: &Host, script: &str) -> anyhow::Result<Output> {
        let path = std::env::var("PATH").unwrap_or_default();
        let path = match &self.path_prefix {
            Some(dir) => format!("{}:{path}", dir.display()),
            None => path,
        };
        let out = Command::new(self.shell)
            .args(["-c", script])
            .env("PATH", path)
            .env("XDG_STATE_HOME", &self.state)
            .stdin(Stdio::null())
            .output()?;
        Ok(Output {
            stdout: out.stdout,
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            code: out.status.code().unwrap_or(-1),
        })
    }

    fn mux_op(&self, _: &Host, op: MuxOp) -> anyhow::Result<Output> {
        Ok(match op {
            MuxOp::Check => Output {
                stdout: Vec::new(),
                stderr: "Master running (pid=1)\r\n".into(),
                code: 0,
            },
            _ => Output::ok(""),
        })
    }
}

/// Wait up to `secs` for `done`.
pub fn wait_until(what: &str, secs: u64, mut done: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while !done() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// The mule binary, with an `ssh` on `PATH` that runs every script right
/// here: layer 2 for a whole verb, its CLI and exit codes included.
///
/// mule's own `Command::new("ssh")` finds the shim, which answers every mux
/// op (an argument that is exactly `-O`; a TUI dispatch script contains
/// ` -O ` too, as `pipe-pane -O`) as a live master (or as none, while [`ShimHost::master_down`] holds) and
/// runs the script -- ssh's last argument -- under `sh`. Remote and local
/// state are then the same directory, `state`, as on the loopback sshd.
pub struct ShimHost {
    pub dir: PathBuf,
    pub state: PathBuf,
    pub config: PathBuf,
    pub tmux: LocalTmux,
}

impl ShimHost {
    pub fn new(tag: &str) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch(tag);
        let state = dir.join("state");
        std::fs::create_dir_all(&state).unwrap();
        let tmux = LocalTmux::new(tag);
        let shim = dir.join("ssh");
        std::fs::write(
            &shim,
            format!(
                "#!/bin/sh\n\
                 for arg; do if [ \"$arg\" = -O ]; then\n\
                   [ -e '{down}' ] && {{ echo 'Control socket connect: No such file or directory' >&2; exit 255; }}\n\
                   echo 'Master running (pid=1)' >&2; exit 0; fi; done\n\
                 [ -e '{down}' ] && exit 255\n\
                 for last; do :; done\n\
                 exec sh -c \"$last\"\n",
                down = dir.join("master-down").display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            format!(
                "[hosts.shim]\ntarget = \"shim\"\nsocket = \"{}\"\ntmux_socket = \"{}\"\n",
                dir.join("mule.sock").display(),
                tmux.socket
            ),
        )
        .unwrap();
        Self {
            dir,
            state,
            config,
            tmux,
        }
    }

    pub fn jobs(&self) -> PathBuf {
        self.state.join("mule/jobs")
    }

    /// Take the master away, or bring it back.
    pub fn master_down(&self, down: bool) {
        let flag = self.dir.join("master-down");
        if down {
            std::fs::write(flag, "").unwrap();
        } else {
            let _ = std::fs::remove_file(flag);
        }
    }

    /// `PATH` with the shim first, then `extra` (a tool shim), then the rest.
    pub fn path_env(&self, extra: Option<&Path>) -> String {
        let rest = std::env::var("PATH").unwrap_or_default();
        match extra {
            Some(extra) => format!("{}:{}:{rest}", self.dir.display(), extra.display()),
            None => format!("{}:{rest}", self.dir.display()),
        }
    }

    pub fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_mule"));
        command
            .arg("--config")
            .arg(&self.config)
            .args(args)
            .env("PATH", self.path_env(None))
            .env("XDG_STATE_HOME", &self.state)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    pub fn mule(&self, args: &[&str]) -> std::process::Output {
        self.command(args).output().expect("mule failed to spawn")
    }
}

impl Drop for ShimHost {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        // A test that broke a mode to make a check fail may have unwound
        // before restoring it.
        let _ = std::fs::set_permissions(&self.state, std::fs::Permissions::from_mode(0o700));
        let _ = std::fs::set_permissions(
            self.state.join("mule"),
            std::fs::Permissions::from_mode(0o700),
        );
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

pub fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}
