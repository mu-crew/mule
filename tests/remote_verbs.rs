//! Every verb's remote script, executed rather than inspected.
//!
//! The `Fake` transport returns canned output regardless of the script it was
//! handed, so it structurally cannot see a wrong script. Every bug of that
//! class so far was found by driving a real host: an unquoted `--cwd`, a
//! sentinel offset overflowing `tail -c +N`, job state colliding with lock
//! files, a job id reaching the shell as syntax, a missing `-f /dev/null`.
//!
//! These tests assert observable behaviour -- files on disk, sessions present
//! or gone, exit codes -- never substrings of a generated command.

mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::sshd::Sshd;

/// A `touch -t` stamp N days in the past.
///
/// Computed, not hardcoded: a literal date silently drifts past whichever
/// horizon the test is probing, and then the test asserts the opposite of what
/// it means.
fn own_stamp(days_ago: u64) -> String {
    let out = std::process::Command::new("date")
        .args([&format!("-v-{days_ago}d"), "+%Y%m%d0000"])
        .output()
        .expect("date failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

struct Fixture {
    sshd: Sshd,
    config: PathBuf,
    tmux: String,
    ids: Vec<String>,
}

impl Fixture {
    fn new(tag: &str) -> Self {
        let sshd = Sshd::start();
        sshd.open_master(&sshd.socket);
        let tmux = format!("coop-rv-{}-{tag}", std::process::id());
        let config = sshd.write_config(&tmux);
        Self {
            sshd,
            config,
            tmux,
            ids: Vec::new(),
        }
    }

    fn coop(&self, args: &[&str]) -> std::process::Output {
        self.sshd.coop(&self.config, args)
    }

    fn out(&self, args: &[&str]) -> String {
        let o = self.coop(args);
        String::from_utf8_lossy(&o.stdout).trim().to_string()
    }

    /// Dispatch and remember the id for cleanup.
    fn run(&mut self, cmd: &str) -> String {
        let o = self.coop(&["run", cmd]);
        assert!(
            o.status.success(),
            "dispatch failed: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        let id = String::from_utf8_lossy(&o.stdout).trim().to_string();
        self.ids.push(id.clone());
        id
    }

    fn await_done(&self, id: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.out(&["poll", id]) == "running" {
            assert!(Instant::now() < deadline, "job {id} never finished");
            std::thread::sleep(Duration::from_millis(40));
        }
    }

    fn job_dir(&self, id: &str) -> String {
        format!("$HOME/.local/state/coop/jobs/{id}")
    }

    /// Read a job artifact, or empty when absent.
    fn artifact(&self, id: &str, name: &str) -> String {
        let out = self
            .sshd
            .ssh(&[&format!("cat {}/{name} 2>/dev/null", self.job_dir(id))]);
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    fn dir_exists(&self, id: &str) -> bool {
        self.sshd
            .ssh(&[&format!("test -d {}", self.job_dir(id))])
            .status
            .success()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for id in &self.ids {
            let _ = self.sshd.ssh(&[&format!("rm -rf {}", self.job_dir(id))]);
        }
        // A tmux server exits with its last job, so by cleanup time
        // `display-message` usually fails while the socket FILE remains --
        // hence the fallback to the conventional path. Quoted as one script
        // because ssh joins argv for the remote shell to re-parse, which would
        // strip the braces from `#{socket_path}`.
        let script = format!(
            "p=$(tmux -L {name} display-message -p '#{{socket_path}}' 2>/dev/null); \
             tmux -L {name} kill-server 2>/dev/null; \
             for c in \"$p\" \"${{TMUX_TMPDIR:-/tmp}}/tmux-$(id -u)/{name}\"; do \
               [ -n \"$c\" ] && [ -S \"$c\" ] && rm -f \"$c\"; \
             done; exit 0",
            name = self.tmux
        );
        let _ = self.sshd.ssh(&[&script]);
    }
}

#[test]
fn probe_reports_every_state_from_real_artifacts() {
    require_sshd!();
    let mut f = Fixture::new("probe");

    // Running: no rc, session alive.
    let running = f.run("sleep 30");
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(f.out(&["poll", &running]), "running");

    // Done: rc present, and the code is the job's.
    let done = f.run("echo hi; exit 6");
    f.await_done(&done);
    assert_eq!(f.out(&["poll", &done]), "6");

    // Orphan: no rc, session gone. Kill the tmux session directly, bypassing
    // `coop kill` so no rc is written.
    let orphan = f.run("sleep 30");
    std::thread::sleep(Duration::from_millis(600));
    let _ = f
        .sshd
        .ssh(&[&format!("tmux -L {} kill-session -t coop-{orphan}", f.tmux)]);
    assert_eq!(f.out(&["poll", &orphan]), "orphan");

    // rc wins over a live session: a job that finished between the two reads is
    // Done, not Running. Write an rc under a still-alive session to force it.
    let racing = f.run("sleep 30");
    std::thread::sleep(Duration::from_millis(600));
    let _ = f
        .sshd
        .ssh(&[&format!("echo 3 > {}/rc", f.job_dir(&racing))]);
    assert_eq!(
        f.out(&["poll", &racing]),
        "3",
        "rc must take precedence over session presence"
    );
    let _ = f.coop(&["kill", &racing]);
    let _ = f.coop(&["kill", &running]);
}

#[test]
fn probe_survives_arbitrary_log_bytes() {
    require_sshd!();
    let mut f = Fixture::new("bytes");

    // The probe reply is a header followed by raw log bytes. Log content that
    // mimics the header, or is not valid UTF-8, must not confuse the parser or
    // be mangled -- the log is the artifact the whole design treats as truth.
    // Header-lookalike text, then genuinely non-UTF-8 bytes. `printf %b` reads
    // octal escapes from its own argument, so the bytes survive the base64
    // round trip rather than fighting through Rust, base64 and sh quoting.
    let id = f.run("printf 'rc=9\\nalive=0\\nsize=1\\nbytes:\\n'; printf %b '\\0377\\0376ok\\n'");
    f.await_done(&id);

    assert_eq!(f.out(&["poll", &id]), "0", "the real rc is 0");

    let tailed = f.coop(&["tail", &id, "--all"]);
    assert!(tailed.status.success());
    let bytes = tailed.stdout;
    assert!(
        bytes.windows(5).any(|w| w == b"rc=9\n"),
        "header-lookalike log content must survive verbatim"
    );
    assert!(
        bytes.contains(&0xff) && bytes.contains(&0xfe),
        "non-UTF-8 log bytes must survive: {bytes:?}"
    );
    assert!(
        String::from_utf8(bytes.clone()).is_err(),
        "the fixture must actually be invalid UTF-8, or a lossy path would pass"
    );
}

#[test]
fn kill_writes_rc_137_and_destroys_the_session() {
    require_sshd!();
    let mut f = Fixture::new("kill");

    let id = f.run("sleep 60");
    std::thread::sleep(Duration::from_millis(600));

    let out = f.coop(&["kill", &id]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 137 rather than an absent rc is what earns `orphan` its meaning: an
    // orphan means "not coop's doing".
    let deadline = Instant::now() + Duration::from_secs(5);
    while f.artifact(&id, "rc").trim() != "137" {
        assert!(
            Instant::now() < deadline,
            "expected rc 137, got {:?}",
            f.artifact(&id, "rc")
        );
        std::thread::sleep(Duration::from_millis(40));
    }
    assert_eq!(f.out(&["poll", &id]), "137");

    let session = f.sshd.ssh(&[&format!(
        "tmux -L {} has-session -t coop-{id} 2>/dev/null",
        f.tmux
    )]);
    assert!(!session.status.success(), "the session must be gone");
}

#[test]
fn kill_never_overwrites_a_real_exit_code() {
    require_sshd!();
    let mut f = Fixture::new("killrace");

    // The job may finish between the decision to kill and the kill itself. The
    // `[ -f rc ] ||` guard is what stops coop replacing a real exit code with
    // 137 -- losing the actual result of completed work.
    let id = f.run("echo done; exit 5");
    f.await_done(&id);
    assert_eq!(f.artifact(&id, "rc").trim(), "5");

    let out = f.coop(&["kill", &id]);
    assert!(out.status.success() || !out.status.success()); // either is fine
    assert_eq!(
        f.artifact(&id, "rc").trim(),
        "5",
        "kill must not overwrite a completed job's rc with 137"
    );
}

#[test]
fn ls_enumerates_real_jobs_with_their_commands() {
    require_sshd!();
    let mut f = Fixture::new("ls");

    let done = f.run("echo listed; exit 2");
    f.await_done(&done);
    let running = f.run("sleep 60");
    std::thread::sleep(Duration::from_millis(600));

    let listing = f.out(&["ls"]);
    assert!(listing.contains(&done), "a recent finished job must appear");
    assert!(listing.contains(&running), "a running job must appear");
    // The command is a base64 display copy on the host; `ls` must decode it, or
    // the id column is meaningless.
    assert!(
        listing.contains("echo listed; exit 2"),
        "the command must round-trip through the state dir: {listing}"
    );
    assert!(listing.contains("sleep 60"), "{listing}");
    assert!(listing.contains("done"), "{listing}");
    assert!(listing.contains("running"), "{listing}");

    // Lock files live beside the jobs directory; they must never be listed as
    // jobs. This shipped once: `dev.lock` appeared as an orphan.
    assert!(
        !listing.contains(".lock"),
        "lock files are not jobs: {listing}"
    );

    let _ = f.coop(&["kill", &running]);
}

#[test]
fn tail_reads_offsets_and_caps_against_a_real_log() {
    require_sshd!();
    let mut f = Fixture::new("tail");

    let id = f.run("printf 'alpha\\nbeta\\ngamma\\n'");
    f.await_done(&id);

    assert_eq!(f.out(&["tail", &id, "--all"]), "alpha\nbeta\ngamma");
    assert_eq!(f.out(&["tail", &id, "-n", "1"]), "gamma");
    // Default is the last 64KB, which for a short log is the whole thing.
    assert_eq!(f.out(&["tail", &id]), "alpha\nbeta\ngamma");

    // A log larger than the default cap must be truncated to it, because the
    // read happens while holding the single channel and the ticket lock.
    let big = f.run("i=0; while [ $i -lt 3000 ]; do echo 0123456789012345678901234567890123456789; i=$((i+1)); done");
    f.await_done(&big);
    let full = f.coop(&["tail", &big, "--all"]).stdout.len();
    let capped = f.coop(&["tail", &big]).stdout.len();
    assert!(full > 65536, "expected a log past the cap, got {full}");
    assert_eq!(capped, 65536, "default tail must cap the read");
}

#[test]
fn prune_removes_finished_jobs_and_spares_running_and_orphans() {
    require_sshd!();
    let mut f = Fixture::new("prune");

    // Three states, all aged well past keep_days (14) but inside the orphan
    // horizon (56).
    let done = f.run("echo old; exit 0");
    f.await_done(&done);
    let orphan = f.run("sleep 60");
    let running = f.run("sleep 60");
    std::thread::sleep(Duration::from_millis(600));
    let _ = f
        .sshd
        .ssh(&[&format!("tmux -L {} kill-session -t coop-{orphan}", f.tmux)]);

    // Older than keep_days (14) but well inside the orphan horizon (56).
    // A fixed date rots: it was ~8 months old when written, which is past 56
    // days, so the orphan was correctly pruned and the assertion below failed.
    let stamp = own_stamp(20);
    for id in [&done, &orphan] {
        let _ = f
            .sshd
            .ssh(&[&format!("touch -t {stamp} {}", f.job_dir(id))]);
    }

    // Prune runs inside the next dispatch's round trip.
    let trigger = f.run("true");
    f.await_done(&trigger);

    assert!(!f.dir_exists(&done), "an aged finished job must be pruned");
    assert!(
        f.dir_exists(&running),
        "a running job must never be pruned, at any age"
    );
    assert!(
        f.dir_exists(&orphan),
        "an orphan inside the long horizon must be kept: it is evidence"
    );

    // Past the long horizon, an orphan does go -- "never" would make a
    // disk-full incident's residue permanent.
    let ancient = own_stamp(120);
    let _ = f
        .sshd
        .ssh(&[&format!("touch -t {ancient} {}", f.job_dir(&orphan))]);
    let trigger2 = f.run("true");
    f.await_done(&trigger2);
    assert!(
        !f.dir_exists(&orphan),
        "an orphan past the long horizon must be pruned"
    );

    let _ = f.coop(&["kill", &running]);
}

#[test]
fn rm_drops_state_and_tolerates_a_missing_job() {
    require_sshd!();
    let mut f = Fixture::new("rm");

    let id = f.run("echo bye");
    f.await_done(&id);
    assert!(f.dir_exists(&id));

    let out = f.coop(&["rm", &id]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!f.dir_exists(&id), "rm must remove the state directory");

    // Removing something already gone is not an error: `rm` is how a caller
    // cleans up, and it must be safe to repeat.
    let again = f.coop(&["rm", &id]);
    assert!(again.status.success(), "rm must be idempotent");

    // A well-formed id that was never a job behaves the same way.
    let never = f.coop(&["rm", "abc123"]);
    assert!(never.status.success(), "rm of an unknown id must not fail");
}

#[test]
fn a_hostile_job_id_never_reaches_the_remote_shell() {
    require_sshd!();
    let f = Fixture::new("hostile");

    // The end-to-end proof for the injection fix: this once produced
    //   d=$HOME/.local/state/coop/jobs/x$(touch /tmp/PWNED)y; ...
    // on the host. Assert both the refusal AND that nothing executed.
    let canary = "/tmp/coop-injection-canary";
    let _ = f.sshd.ssh(&[&format!("rm -f {canary}")]);

    for hostile in [
        format!("x$(touch {canary})y"),
        format!("abc123; touch {canary}"),
        format!("`touch {canary}`"),
    ] {
        for verb in ["poll", "tail", "kill", "rm"] {
            let out = f.coop(&[verb, &hostile]);
            assert!(
                !out.status.success(),
                "{verb} accepted a hostile id: {hostile}"
            );
        }
    }

    let exists = f.sshd.ssh(&[&format!("test -e {canary}")]).status.success();
    assert!(!exists, "a hostile id executed on the host");
}

#[test]
fn run_wait_prints_the_output_of_an_instant_command() {
    require_sshd!();
    let mut f = Fixture::new("runwait");

    // `coop run --wait ls` printed the id and nothing else. `rc` and `log` are
    // written by opposite ends of a pipeline, so `rc` can land while the log's
    // last bytes are still in flight; the wait loop returned on the first
    // `Done` and dropped them. Any command fast enough to finish inside one
    // probe interval lost its output -- which is most commands.
    //
    // A Fake-level test could not catch this: the Fake replies in whatever
    // order the test queues, so the race does not exist there. This is the
    // plainest possible use of the tool and it needs a real host to verify.
    for _ in 0..3 {
        let out = f.coop(&["run", "--wait", "echo instant-output"]);
        assert!(
            out.status.success(),
            "run --wait failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8_lossy(&out.stdout);
        let mut lines = text.lines();
        let id = lines.next().unwrap_or_default();
        assert_eq!(
            id.len(),
            6,
            "the id must print first, so it survives a later failure"
        );
        f.ids.push(id.to_string());
        assert_eq!(
            lines.next(),
            Some("instant-output"),
            "the job's output must follow the id, got: {text:?}"
        );
    }

    // The exit code is the job's own, not the tail's.
    let failed = f.coop(&["run", "--wait", "echo before-failing; exit 7"]);
    let text = String::from_utf8_lossy(&failed.stdout);
    let mut lines = text.lines();
    f.ids.push(lines.next().unwrap_or_default().to_string());
    assert_eq!(lines.next(), Some("before-failing"), "{text:?}");
    assert_eq!(failed.status.code(), Some(7), "wait returns the job's code");

    // And `ls` is readable: a header, so nobody has to count columns to work
    // out which number is the exit code and which the age.
    let listing = f.out(&["ls"]);
    let header = listing.lines().next().unwrap_or_default();
    for column in ["ID", "HOST", "STATE", "RC", "AGE", "COMMAND"] {
        assert!(
            header.contains(column),
            "missing {column} in header: {header:?}"
        );
    }
    assert!(
        listing.contains("instant-output") || listing.contains("echo instant-output"),
        "the dispatched commands must be listed: {listing}"
    );
}

#[test]
fn a_trivially_successful_job_reports_rc_zero() {
    require_sshd!();
    let mut f = Fixture::new("rczero");

    // `coop run true` reported rc 1. The wrapper ended with
    // `[ -s .overflow ] && echo 1 > truncated`, and that test is the last
    // command in the pipeline, so an empty overflow file made it exit 1 -- which
    // became the job's status. Every successful job was affected.
    for cmd in ["true", "ls", "echo hi", "printf ''"] {
        let id = f.run(cmd);
        f.await_done(&id);
        assert_eq!(
            f.out(&["poll", &id]),
            "0",
            "`{cmd}` must report success, not the truncation check's status"
        );
    }
}
