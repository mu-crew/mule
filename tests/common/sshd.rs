//! A throwaway `sshd` with `MaxSessions 1`, on loopback.
//!
//! Test layer 3. Every invariant coop exists for is a property of a
//! session-capped ssh connection, and the original design notes concluded none
//! of it was reachable without a real capped host. That was wrong: a non-root
//! `sshd` on an ephemeral port reproduces the contention exactly, including the
//! misleading `Permission denied` fallback, with no hardware token and no
//! remote machine.
//!
//! This fixture is the difference between the design's central claims being
//! verified continuously and being verified by hand. Bugs already found by
//! driving a real sshd manually, all invisible to the `Fake` transport: an
//! unquoted `--cwd` splitting on a space, a sentinel offset overflowing
//! `tail -c +N`, job state colliding with lock files, a job id reaching the
//! remote shell as syntax, and a missing `-f /dev/null` costing 9x on dispatch.

#![allow(dead_code)] // Each test binary uses a different subset.

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Where the real `sshd` lives. Not on `PATH` for a non-root user on macOS.
const SSHD: &str = "/usr/sbin/sshd";

/// A running `sshd`, its key material, and a `PATH` shim so coop's `ssh` calls
/// reach it.
pub struct Sshd {
    pub dir: PathBuf,
    pub port: u16,
    /// coop's private `ControlPath`, once [`Sshd::open_master`] has run.
    pub socket: PathBuf,
    pub state_root: PathBuf,
    pid: Option<u32>,
}

/// Owns one control master and closes it even when a test unwinds.
pub struct Master {
    ssh: PathBuf,
    socket: PathBuf,
}

impl Drop for Master {
    fn drop(&mut self) {
        let _ = Command::new(&self.ssh)
            .arg("-S")
            .arg(&self.socket)
            .args(["-O", "exit", "127.0.0.1"])
            .output();
    }
}

/// Is this environment able to run the layer at all?
///
/// Returns a reason when it cannot, so a skip prints something actionable
/// rather than passing silently. A test that quietly does nothing is worse than
/// no test.
pub fn unavailable() -> Option<String> {
    if !Path::new(SSHD).exists() {
        return Some(format!("{SSHD} is not present"));
    }
    if Command::new("ssh").arg("-V").output().is_err() {
        return Some("ssh is not on PATH".into());
    }
    None
}

/// Ask the OS for a free port, then release it.
///
/// Racy in principle, and the alternative — a hardcoded port — is worse: it
/// collides with a previous run's leaked daemon and fails in a way that looks
/// like an auth problem.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("cannot bind a loopback port")
        .local_addr()
        .unwrap()
        .port()
}

fn run(cmd: &mut Command) -> std::process::Output {
    let out = cmd.output().expect("spawn failed");
    assert!(
        out.status.success(),
        "{cmd:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

impl Sshd {
    /// Start a daemon. Panics if [`unavailable`] would have returned a reason.
    pub fn start() -> Self {
        assert!(unavailable().is_none(), "checked by the caller");

        let dir =
            std::env::temp_dir().join(format!("coop-sshd-{}-{}", std::process::id(), next_seq()));
        std::fs::create_dir_all(&dir).unwrap();
        let port = free_port();

        for name in ["hostkey", "id"] {
            run(Command::new("ssh-keygen").args([
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                dir.join(name).to_str().unwrap(),
            ]));
        }

        // `MaxSessions 1` is the entire point. The rest disables everything
        // that would need a human or root:
        //   UsePAM no                    - PAM needs privilege
        //   StrictModes no               - the temp dir is world-writable
        //   Password/KbdInteractive no   - key auth only, so nothing prompts
        let config = format!(
            "Port {port}\n\
             ListenAddress 127.0.0.1\n\
             HostKey {dir}/hostkey\n\
             AuthorizedKeysFile {dir}/id.pub\n\
             PidFile {dir}/sshd.pid\n\
             MaxSessions 1\n\
             UsePAM no\n\
             StrictModes no\n\
             PasswordAuthentication no\n\
             KbdInteractiveAuthentication no\n\
             AcceptEnv XDG_STATE_HOME\n",
            dir = dir.display()
        );
        let config_path = dir.join("sshd_config");
        std::fs::write(&config_path, config).unwrap();

        // sshd refuses to start on a group- or world-readable key or config.
        for name in ["sshd_config", "id"] {
            run(Command::new("chmod").arg("600").arg(dir.join(name)));
        }

        run(Command::new(SSHD)
            .arg("-f")
            .arg(&config_path)
            .arg("-E")
            .arg(dir.join("sshd.log")));

        // An `ssh` shim on PATH, so coop's own `Command::new("ssh")` reaches
        // this daemon with the right key and port. coop must not learn about
        // test ports.
        let shim = dir.join("ssh");
        {
            let mut f = std::fs::File::create(&shim).unwrap();
            writeln!(
                f,
                "#!/bin/sh\n\
                 exec /usr/bin/ssh -F /dev/null -i {dir}/id \\\n\
                 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \\\n\
                 -o SetEnv=XDG_STATE_HOME={state} -p {port} \"$@\"",
                dir = dir.display(),
                state = dir.join("state").display()
            )
            .unwrap();
        }
        run(Command::new("chmod").arg("755").arg(&shim));

        let mut sshd = Self {
            socket: dir.join("coop.sock"),
            state_root: dir.join("state"),
            dir,
            port,
            pid: None,
        };
        sshd.pid = std::fs::read_to_string(sshd.dir.join("sshd.pid"))
            .ok()
            .and_then(|s| s.trim().parse().ok());
        sshd.await_ready();
        sshd
    }

    /// Block until the daemon answers, rather than sleeping a guessed interval.
    fn await_ready(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if self.ssh(&["true"]).status.success() {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "sshd never became ready; log:\n{}",
                std::fs::read_to_string(self.dir.join("sshd.log")).unwrap_or_default()
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// `PATH` with the shim first, for spawning coop.
    pub fn path_env(&self) -> String {
        format!(
            "{}:{}",
            self.dir.display(),
            std::env::var("PATH").unwrap_or_default()
        )
    }

    /// A direct ssh call through the shim, on no particular control socket.
    pub fn ssh(&self, args: &[&str]) -> std::process::Output {
        Command::new(self.dir.join("ssh"))
            .arg("127.0.0.1")
            .args(args)
            .stdin(Stdio::null())
            .output()
            .expect("ssh shim failed to spawn")
    }

    /// A call multiplexed over `socket`.
    pub fn ssh_via(&self, socket: &Path, args: &[&str]) -> std::process::Output {
        Command::new(self.dir.join("ssh"))
            .arg("-S")
            .arg(socket)
            .args(["-o", "BatchMode=yes"])
            .arg("127.0.0.1")
            .args(args)
            .stdin(Stdio::null())
            .output()
            .expect("ssh shim failed to spawn")
    }

    /// Open a control master on `socket` and wait for it to answer.
    pub fn open_master(&self, socket: &Path) -> Master {
        run(Command::new(self.dir.join("ssh"))
            .args(["-M", "-N", "-f", "-S"])
            .arg(socket)
            .arg("127.0.0.1"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !self.master_alive(socket) {
            assert!(std::time::Instant::now() < deadline, "master never came up");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        Master {
            ssh: self.dir.join("ssh"),
            socket: socket.to_owned(),
        }
    }

    /// `ssh -O check`: the one probe that opens no session channel.
    pub fn master_alive(&self, socket: &Path) -> bool {
        Command::new(self.dir.join("ssh"))
            .arg("-S")
            .arg(socket)
            .args(["-O", "check", "127.0.0.1"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// A coop config naming this daemon, with a private tmux server.
    pub fn write_config(&self, tmux_socket: &str) -> PathBuf {
        let path = self.dir.join("config.toml");
        std::fs::write(
            &path,
            format!(
                "[hosts.live]\n\
                 target = \"127.0.0.1\"\n\
                 socket = \"{}\"\n\
                 tmux_socket = \"{tmux_socket}\"\n",
                self.socket.display()
            ),
        )
        .unwrap();
        path
    }

    /// Run the coop binary under test against this daemon.
    pub fn coop(&self, config: &Path, args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_coop"))
            .arg("--config")
            .arg(config)
            .args(args)
            .env("PATH", self.path_env())
            .env("XDG_STATE_HOME", &self.state_root)
            .stdin(Stdio::null())
            .output()
            .expect("coop failed to spawn")
    }
}

impl Drop for Sshd {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            let _ = Command::new("kill").arg(pid.to_string()).output();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn next_seq() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static SEQ: AtomicU32 = AtomicU32::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Skip the calling test with a printed reason, when the layer cannot run.
#[macro_export]
macro_rules! require_sshd {
    () => {
        if let Some(reason) = $crate::common::sshd::unavailable() {
            eprintln!("skipping local-sshd test: {reason}");
            return;
        }
    };
}
