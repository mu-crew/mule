use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use coop::config::Host;
use coop::wrapper::{Job, dispatch_script};

struct TmuxServer {
    socket: String,
    socket_path: PathBuf,
    root: PathBuf,
}

impl TmuxServer {
    fn start() -> Self {
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let socket = format!("coop-test-{}", std::process::id());
        let root = std::env::temp_dir().join(format!("coop-test-state-{suffix}"));
        fs::create_dir_all(&root).unwrap();

        let status = Command::new("tmux")
            .args([
                "-L",
                &socket,
                "-f",
                "/dev/null",
                "new-session",
                "-d",
                "-s",
                "keeper",
            ])
            .status()
            .unwrap();
        assert!(status.success());

        let output = Command::new("tmux")
            .args(["-L", &socket, "display-message", "-p", "#{socket_path}"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let socket_path = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());

        Self {
            socket,
            socket_path,
            root,
        }
    }

    fn run(&self, id: &str, command: &str, cwd: Option<&str>) -> PathBuf {
        let host = Host {
            name: "local".into(),
            target: "local".into(),
            socket: PathBuf::new(),
            tmux_socket: self.socket.clone(),
            max_running: 4,
            default_cwd: None,
            keep_days: 14,
        };
        let job = Job {
            id: id.into(),
            cmd: command.into(),
            cwd: cwd.map(str::to_owned),
        };
        let script = dispatch_script(&host, &job).replace(
            &format!("{}/{id}", coop::wrapper::JOBS_ROOT),
            &self.root.join(id).display().to_string(),
        );
        let status = Command::new("sh")
            .args(["-c", &script])
            .stdin(Stdio::null())
            .status()
            .unwrap();
        assert!(status.success(), "dispatch failed: {script}");

        let state = self.root.join(id);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !state.join("rc").exists() {
            assert!(Instant::now() < deadline, "timed out waiting for {id}");
            thread::sleep(Duration::from_millis(10));
        }
        state
    }
}

impl Drop for TmuxServer {
    fn drop(&mut self) {
        let _ = Command::new("tmux")
            .args(["-L", &self.socket, "kill-server"])
            .status();
        let _ = fs::remove_file(&self.socket_path);
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).unwrap()
}

#[test]
fn generated_wrapper_runs_jobs_on_a_real_private_tmux_server() {
    if Command::new("tmux").arg("-V").output().is_err() {
        eprintln!("skipping real tmux wrapper test: tmux is not on PATH");
        return;
    }

    let server = TmuxServer::start();

    let hello = server.run("000001", "echo hello", None);
    assert_eq!(read(hello.join("rc")).trim(), "0");
    assert_eq!(read(hello.join("log")).trim(), "hello");
    assert_eq!(read(hello.join("cmd")), "echo hello");

    let exit_three = server.run("000002", "exit 3", None);
    assert_eq!(read(exit_three.join("rc")).trim(), "3");

    let quoted = r#"VAR='value with spaces'; printf '%s\n' "\"quoted\" $VAR""#;
    let round_trip = server.run("000003", quoted, None);
    assert_eq!(
        read(round_trip.join("log")).trim(),
        "\"quoted\" value with spaces"
    );
    assert_eq!(read(round_trip.join("cmd")), quoted);

    let bad_cwd = server.run(
        "000004",
        "echo this-must-not-run",
        Some("/coop/nonexistent/directory"),
    );
    assert_ne!(read(bad_cwd.join("rc")).trim(), "0");
    assert!(
        !bad_cwd.join("log").exists() || !read(bad_cwd.join("log")).contains("this-must-not-run")
    );

    // A cwd containing a space, proven end to end rather than only on the
    // string. Unquoted interpolation splits it into two words, so `cd` either
    // fails or lands somewhere else entirely -- and landing somewhere else is
    // the silent one.
    let spaced = server.root.join("a dir with spaces");
    fs::create_dir_all(&spaced).unwrap();
    let in_spaced = server.run("000005", "pwd", Some(spaced.to_str().unwrap()));
    assert_eq!(read(in_spaced.join("rc")).trim(), "0");
    // Compare the trailing component rather than the whole path: on macOS the
    // temp dir lives under a `/var -> /private/var` symlink, so `pwd` (logical)
    // and `canonicalize()` (physical) legitimately disagree on the prefix. What
    // is being tested is that the spaced segment survived intact.
    let landed = read(in_spaced.join("log")).trim().to_string();
    assert!(
        landed.ends_with("/a dir with spaces"),
        "the job must run in the spaced directory, not a prefix of it: {landed}"
    );
}
