//! Systematic fault injection: the acceptance test for SPEC § Remote
//! snippets fail closed.
//!
//! For each verb that answers from remote state -- `poll` (the probe behind
//! `wait`, `tail` and the lease), `ls` with and without `--key-prefix` and
//! `--running`, prune (inside a dispatch), `rm`, `run --key` and the keyed
//! lease's claim -- a scenario is set up, the verb is run once to enumerate
//! every external command its remote snippets run, and then it is run again
//! once per command per failure mode, with that one command failing:
//!
//! - `exit2`: exit 2, no output;
//! - `exit1`: exit 1, no output (where 1 means "no" or "false");
//! - `partial`: `1` on stdout, then exit 2 (a value that parses);
//! - `eperm`: `Permission denied` on stderr, exit 1.
//!
//! Every injection must leave: no answer that is an absence or a definite
//! state (`missing`, `orphan`, `done`, `running`, "not found", an empty
//! listing), no state deleted that the verb may not delete, and never a
//! second job for a key. Hostile file states (a key that is a symlink to an
//! unreadable file, an unreadable key under `--running`) run under the same
//! assertions.
//!
//! How: a directory of shims, one per external tool, sits first on `PATH`
//! for the verb only. Each shim counts itself and fails when its count is
//! the injected one, else runs the real tool. The `ssh` shim numbers each
//! remote call, so a lease's injections stay in its first calls (the claim
//! and the adoption probe); the tmux shim strips the fault variables, so
//! the tmux server and the jobs it runs are never counted. Layer 2: no sshd.

mod common;

use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use common::local::{ShimHost, scratch, stdout, tmux_available, wait_until};

/// Every external command a remote snippet may run. Builtins (`[`, `printf`,
/// `read`, `case`) cannot fail this way; `test` is here for `find -exec test`.
const TOOLS: &[&str] = &[
    "awk", "base64", "cat", "date", "find", "grep", "head", "ls", "mkdir", "mv", "rm", "sed",
    "stat", "tail", "test", "tmux", "tr", "uname", "wc",
];

const MODES: &[&str] = &["exit2", "exit1", "partial", "eperm"];

/// The tool's executable on `PATH` (not a shell builtin: `test` must be
/// `/bin/test`, the one `find -exec` runs).
fn real(tool: &str) -> String {
    std::env::var("PATH")
        .unwrap_or_default()
        .split(':')
        .map(|dir| Path::new(dir).join(tool))
        .find(|path| {
            std::fs::metadata(path)
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
        .unwrap_or_else(|| panic!("no {tool} on PATH"))
        .display()
        .to_string()
}

/// The shim directory, made once per test binary.
fn shims() -> &'static Path {
    use std::sync::OnceLock;
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = scratch("fault-shims");
        for tool in TOOLS {
            let real = real(tool);
            // tmux runs with the fault variables stripped: a server it starts,
            // and every job in it, must never be counted.
            let exec = if *tool == "tmux" {
                format!("exec /usr/bin/env -u MULE_FAULT_DIR -u MULE_FAULT_CALL '{real}' \"$@\"")
            } else {
                format!("exec '{real}' \"$@\"")
            };
            let body = format!(
                "#!/bin/sh\n\
                 [ -n \"$MULE_FAULT_DIR\" ] && [ -n \"$MULE_FAULT_CALL\" ] || {exec}\n\
                 F=$MULE_FAULT_DIR\n\
                 while ! /bin/mkdir \"$F/lock\" 2>/dev/null; do :; done\n\
                 n=0; if [ -f \"$F/count\" ]; then read n < \"$F/count\"; fi\n\
                 n=$((n + 1)); echo $n > \"$F/count\"\n\
                 echo \"$n $MULE_FAULT_CALL {tool}\" >> \"$F/log\"\n\
                 /bin/rmdir \"$F/lock\"\n\
                 want=; mode=; if [ -f \"$F/inject\" ]; then read want mode < \"$F/inject\"; fi\n\
                 if [ \"$n\" = \"$want\" ]; then\n\
                   echo \"$n {tool} $mode\" >> \"$F/hit\"\n\
                   case $mode in\n\
                     exit2) exit 2;;\n\
                     exit1) exit 1;;\n\
                     partial) printf '1\\n'; exit 2;;\n\
                     eperm) echo '{tool}: Permission denied' >&2; exit 1;;\n\
                   esac\n\
                 fi\n\
                 {exec}\n"
            );
            let path = dir.join(tool);
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        dir
    })
}

/// A host whose `ssh` numbers its remote calls when faults are on.
fn fault_host(tag: &str) -> ShimHost {
    let host = ShimHost::new(tag);
    let ssh = host.dir.join("ssh");
    std::fs::write(
        &ssh,
        "#!/bin/sh\n\
         for arg; do if [ \"$arg\" = -O ]; then echo 'Master running (pid=1)' >&2; exit 0; fi; done\n\
         for last; do :; done\n\
         if [ -n \"$MULE_FAULT_DIR\" ]; then\n\
           F=$MULE_FAULT_DIR\n\
           while ! /bin/mkdir \"$F/lock\" 2>/dev/null; do :; done\n\
           c=0; if [ -f \"$F/calls\" ]; then read c < \"$F/calls\"; fi\n\
           c=$((c + 1)); echo $c > \"$F/calls\"\n\
           /bin/rmdir \"$F/lock\"\n\
           max=999999; if [ -f \"$F/maxcall\" ]; then read max < \"$F/maxcall\"; fi\n\
           if [ $c -le $max ]; then export MULE_FAULT_CALL=$c; else unset MULE_FAULT_CALL; fi\n\
         fi\n\
         exec sh -c \"$last\"\n",
    )
    .unwrap();
    host
}

/// One fault-run's control directory.
struct Fault {
    dir: PathBuf,
}

impl Fault {
    fn new(inject: Option<(usize, &str)>, max_call: Option<usize>) -> Self {
        let dir = scratch("fault");
        if let Some((n, mode)) = inject {
            std::fs::write(dir.join("inject"), format!("{n} {mode}\n")).unwrap();
        }
        if let Some(max) = max_call {
            std::fs::write(dir.join("maxcall"), format!("{max}\n")).unwrap();
        }
        Self { dir }
    }

    /// The tools run, in order.
    fn log(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.join("log"))
            .unwrap_or_default()
            .lines()
            .map(|line| line.rsplit(' ').next().unwrap_or("").to_string())
            .collect()
    }

    fn hit(&self) -> String {
        std::fs::read_to_string(self.dir.join("hit")).unwrap_or_default()
    }
}

impl Drop for Fault {
    // Hundreds of runs each make one: leave none behind.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Is this run's failure the fixture's, not mule's? The fake master's
/// `-O check` is a shell script, and mule bounds the check at 2s: on a
/// machine loaded enough, the script starts too late and mule reports no
/// master before any remote snippet runs (so no injection can be the cause).
fn fixture_timeout(out: &Output) -> bool {
    String::from_utf8_lossy(&out.stderr).contains("no control master")
}

/// Run `command` (rebuilt by `make`) until it is not a fixture timeout.
/// `fresh` says whether a retry may run: only while nothing remote has run
/// yet, or the retry would act on the first attempt's effects (a lease
/// whose claim landed before its dispatch's master check timed out would
/// hold that unstarted claim).
fn retried(mut make: impl FnMut() -> Command, fresh: impl Fn() -> bool) -> Output {
    let mut out = make().output().unwrap();
    for _ in 0..5 {
        if !fixture_timeout(&out) || !fresh() {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
        out = make().output().unwrap();
    }
    out
}

/// [`mule_with`], killed after `limit` if it is still running, or as soon
/// as a remote call past the fault window has started (the window's calls
/// are then done, and a held lease would only go on holding).
fn mule_for(host: &ShimHost, fault: &Fault, args: &[&str], limit: Duration) -> Output {
    let mut child = host
        .command(args)
        .env("PATH", host.path_env(Some(shims())))
        .env("MULE_FAULT_DIR", &fault.dir)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + limit;
    let window = std::fs::read_to_string(fault.dir.join("maxcall"))
        .ok()
        .and_then(|max| max.trim().parse::<usize>().ok());
    while std::time::Instant::now() < deadline {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        let calls = std::fs::read_to_string(fault.dir.join("calls"))
            .ok()
            .and_then(|calls| calls.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if window.is_some_and(|max| calls > max) {
            // Let the call that crossed the window get going, then stop.
            std::thread::sleep(Duration::from_millis(200));
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // SIGKILL: no signal-path stop, so the check sees what the run left.
    let _ = child.kill();
    child.wait_with_output().unwrap()
}

/// Run mule with the shims first on PATH and `fault` on.
fn mule_with(host: &ShimHost, fault: &Fault, args: &[&str]) -> Output {
    retried(
        || {
            let mut command = host.command(args);
            command
                .env("PATH", host.path_env(Some(shims())))
                .env("MULE_FAULT_DIR", &fault.dir)
                .stdin(Stdio::null());
            command
        },
        || !fault.dir.join("calls").exists(),
    )
}

fn jobs(host: &ShimHost) -> BTreeSet<String> {
    std::fs::read_dir(host.jobs())
        .map(|dir| {
            dir.filter_map(Result::ok)
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn key_of(host: &ShimHost, id: &str) -> Option<String> {
    std::fs::read_to_string(host.jobs().join(id).join("key"))
        .ok()
        .map(|k| k.trim().to_string())
}

fn run_ok(host: &ShimHost, args: &[&str]) -> String {
    // Setup commands are one dispatch each, whose master check comes before
    // its only snippet: a timeout there ran nothing.
    let out = retried(|| host.command(args), || true);
    assert!(
        out.status.success(),
        "setup {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout(&out)
}

fn running(host: &ShimHost, args: &[&str]) -> String {
    let id = run_ok(host, args);
    wait_until("the session", 10, || {
        host.tmux.has_session(&format!("mule-{id}"))
    });
    id
}

fn finished(host: &ShimHost, args: &[&str]) -> String {
    let id = run_ok(host, args);
    wait_until("rc", 10, || host.jobs().join(&id).join("rc").exists());
    wait_until("the session to end", 10, || {
        !host.tmux.has_session(&format!("mule-{id}"))
    });
    id
}

fn age(host: &ShimHost, id: &str) {
    let ok = Command::new("touch")
        .args(["-t", "202001010000"])
        .arg(host.jobs().join(id))
        .status()
        .unwrap();
    assert!(ok.success());
}

/// One scenario sweep at a time, each with its own few threads. Run side
/// by side, seventeen sweeps of four threads loaded the machine until the
/// fake master's `-O check` missed mule's 2s bound and runs failed for the
/// fixture's sake.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// A scenario: build a host's state, then check one verb run against it.
struct Scenario {
    name: &'static str,
    /// Kill the verb after this long: for a lease that rightly holds on.
    hold_for: Option<Duration>,
    /// Remote calls to inject in (a lease runs on; its first calls decide).
    max_call: Option<usize>,
    setup: fn(&ShimHost) -> Vec<String>,
    verb: fn(&[String]) -> Vec<String>,
    /// `Err(why)` when the outcome breaks the discipline.
    check: fn(&ShimHost, &[String], &Output) -> Result<(), String>,
}

fn json(out: &Output) -> Option<serde_json::Value> {
    serde_json::from_slice(&out.stdout).ok()
}

/// Enumerate, then inject every mode at every command; panic listing every
/// broken run. Prints the count, for the record.
fn exercise(scenario: &Scenario) {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let _one_at_a_time = serial();
    let host = fault_host(&format!("fi-{}-enum", scenario.name));
    let ids = (scenario.setup)(&host);
    let args = (scenario.verb)(&ids);
    let fault = Fault::new(None, scenario.max_call);
    let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
    let _ = match scenario.hold_for {
        Some(limit) => mule_for(&host, &fault, &args_ref, limit),
        None => mule_with(&host, &fault, &args_ref),
    };
    let tools = fault.log();
    let count = tools.len();
    eprintln!(
        "HARNESS {} commands={count} [{}]",
        scenario.name,
        tools.join(" ")
    );
    assert!(count > 0, "{}: enumerated no commands", scenario.name);

    let mut cases: Vec<(usize, &str)> = (1..=count)
        .flat_map(|n| MODES.iter().map(move |mode| (n, *mode)))
        .collect();
    // `MULE_FI_CASE=<scenario>:<n>:<mode>` runs one case and shows its log,
    // for debugging a failure the full sweep reported.
    let only = std::env::var("MULE_FI_CASE").ok();
    if let Some(only) = &only {
        cases.retain(|(n, mode)| *only == format!("{}:{n}:{mode}", scenario.name));
    }
    let only = only.is_some();
    let failures = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for chunk in cases.chunks(cases.len().div_ceil(4)) {
            let failures = &failures;
            scope.spawn(move || {
                for &(n, mode) in chunk {
                    // A run that took a shorter path than the enumeration
                    // (fewer commands) never reaches a late injection: run it
                    // again on a fresh setup, so every point is tested.
                    let mut attempt = 0;
                    let (host, ids, out, hit, fault) = loop {
                        attempt += 1;
                        let host =
                            fault_host(&format!("fi-{}-{n}-{mode}-{attempt}", scenario.name));
                        let ids = (scenario.setup)(&host);
                        let args = (scenario.verb)(&ids);
                        let args_ref: Vec<&str> = args.iter().map(String::as_str).collect();
                        let fault = Fault::new(Some((n, mode)), scenario.max_call);
                        let out = match scenario.hold_for {
                            Some(limit) => mule_for(&host, &fault, &args_ref, limit),
                            None => mule_with(&host, &fault, &args_ref),
                        };
                        let hit = fault.hit();
                        if !hit.is_empty() || attempt == 3 {
                            break (host, ids, out, hit, fault);
                        }
                    };
                    if only {
                        eprintln!(
                            "CASE log:\n{}\nstderr: {}",
                            std::fs::read_to_string(fault.dir.join("log")).unwrap_or_default(),
                            String::from_utf8_lossy(&out.stderr)
                        );
                    }
                    let verdict = if hit.is_empty() {
                        // The scenario ran fewer commands this time: say so,
                        // rather than pass a run that injected nothing.
                        Err(format!("injection {n} was never reached"))
                    } else {
                        (scenario.check)(&host, &ids, &out)
                    };
                    if let Err(why) = verdict {
                        failures.lock().unwrap().push(format!(
                            "{} #{n} {mode} ({}): {why}\n    stdout: {}\n    stderr: {}",
                            scenario.name,
                            hit.trim(),
                            stdout(&out),
                            String::from_utf8_lossy(&out.stderr).trim()
                        ));
                    }
                }
            });
        }
    });
    let failures = failures.into_inner().unwrap();
    assert!(
        failures.is_empty(),
        "{} of {} injections broke the discipline:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

// -- checks ---------------------------------------------------------------------

fn poll_is_unknown(_: &ShimHost, _: &[String], out: &Output) -> Result<(), String> {
    match json(out) {
        Some(v) if v["state"] == "unknown" => Ok(()),
        Some(v) => Err(format!("answered {}", v["state"])),
        None if !out.status.success() => Ok(()),
        None => Err("exit 0 with no answer".into()),
    }
}

fn refused(_: &ShimHost, _: &[String], out: &Output) -> Result<(), String> {
    if out.status.success() {
        return Err("answered".into());
    }
    if !out.stdout.is_empty() {
        return Err("printed an answer before failing".into());
    }
    Ok(())
}

/// The job dirs the setup made are all still there.
fn kept(host: &ShimHost, ids: &[String]) -> Result<(), String> {
    let now = jobs(host);
    for id in ids {
        if !now.contains(id) {
            return Err(format!("job {id}'s state was deleted"));
        }
    }
    Ok(())
}

// -- poll -------------------------------------------------------------------------

fn poll_verb(ids: &[String]) -> Vec<String> {
    vec!["poll".into(), ids[0].clone(), "--json".into()]
}

fn poll_check(host: &ShimHost, ids: &[String], out: &Output) -> Result<(), String> {
    poll_is_unknown(host, ids, out)?;
    kept(host, &ids[1..])
}

#[test]
fn poll_of_a_running_job() {
    exercise(&Scenario {
        name: "poll-running",
        hold_for: None,
        max_call: None,
        setup: |h| vec![running(h, &["run", "--quiet", "sleep 60"])],
        verb: poll_verb,
        check: poll_check,
    });
}

#[test]
fn poll_of_a_done_job() {
    exercise(&Scenario {
        name: "poll-done",
        hold_for: None,
        max_call: None,
        setup: |h| vec![finished(h, &["run", "--quiet", "exit 17"])],
        verb: poll_verb,
        check: poll_check,
    });
}

#[test]
fn poll_of_an_orphan() {
    exercise(&Scenario {
        name: "poll-orphan",
        hold_for: None,
        max_call: None,
        setup: |h| {
            let d = h.jobs().join("0f0f01");
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("cmd"), "sleep 1\n").unwrap();
            // A server for list-sessions to answer from.
            h.tmux
                .command(&["new-session", "-d", "-s", "keeper", "sleep 60"])
                .status()
                .unwrap();
            vec!["0f0f01".into()]
        },
        verb: poll_verb,
        check: poll_check,
    });
}

#[test]
fn poll_of_a_missing_job_with_a_live_watchdog() {
    // Round 1's grep finding: absence of `mule-` and `watch-` must be seen.
    exercise(&Scenario {
        name: "poll-missing",
        hold_for: None,
        max_call: None,
        setup: |h| {
            std::fs::create_dir_all(h.jobs()).unwrap();
            h.tmux
                .command(&["new-session", "-d", "-s", "keeper", "sleep 60"])
                .status()
                .unwrap();
            vec!["0e0e01".into()]
        },
        verb: poll_verb,
        check: poll_check,
    });
}

// -- ls ---------------------------------------------------------------------------

fn ls_setup(h: &ShimHost) -> Vec<String> {
    vec![
        running(h, &["run", "--quiet", "--key", "mu.a", "sleep 60"]),
        finished(h, &["run", "--quiet", "--key", "mu.b", "exit 2"]),
        running(h, &["run", "--quiet", "sleep 60"]),
    ]
}

fn ls_check(host: &ShimHost, ids: &[String], out: &Output) -> Result<(), String> {
    refused(host, ids, out)?;
    kept(host, ids)
}

#[test]
fn ls_plain() {
    exercise(&Scenario {
        name: "ls",
        hold_for: None,
        max_call: None,
        setup: ls_setup,
        verb: |_| vec!["ls".into(), "--json".into()],
        check: ls_check,
    });
}

#[test]
fn ls_key_prefix() {
    exercise(&Scenario {
        name: "ls-key-prefix",
        hold_for: None,
        max_call: None,
        setup: ls_setup,
        verb: |_| {
            vec![
                "ls".into(),
                "--json".into(),
                "--key-prefix".into(),
                "mu.".into(),
            ]
        },
        check: ls_check,
    });
}

#[test]
fn ls_running_key_prefix() {
    exercise(&Scenario {
        name: "ls-running-key-prefix",
        hold_for: None,
        max_call: None,
        setup: ls_setup,
        verb: |_| {
            vec![
                "ls".into(),
                "--json".into(),
                "--running".into(),
                "--key-prefix".into(),
                "mu.".into(),
            ]
        },
        check: ls_check,
    });
}

// -- prune (inside a dispatch) -----------------------------------------------------

#[test]
fn prune_in_a_dispatch() {
    // ids: [keyed old done, old running, young done] must survive; an old
    // unkeyed done job and an old orphan may go (prune's job).
    exercise(&Scenario {
        name: "prune",
        hold_for: None,
        max_call: None,
        setup: |h| {
            let keyed = finished(h, &["run", "--quiet", "--key", "mu.old", "true"]);
            let live = running(h, &["run", "--quiet", "sleep 60"]);
            let young = finished(h, &["run", "--quiet", "true"]);
            let old = finished(h, &["run", "--quiet", "true"]);
            let orphan = h.jobs().join("0d0d01");
            std::fs::create_dir_all(&orphan).unwrap();
            std::fs::write(orphan.join("cmd"), "x\n").unwrap();
            for id in [&keyed, &live, &old, &"0d0d01".to_string()] {
                age(h, id);
            }
            vec![keyed, live, young]
        },
        verb: |_| vec!["run".into(), "--quiet".into(), "true".into()],
        check: |host, ids, _| {
            kept(host, ids)?;
            if key_of(host, &ids[0]).as_deref() != Some("mu.old") {
                return Err("the keyed job lost its key".into());
            }
            Ok(())
        },
    });
}

// -- rm ---------------------------------------------------------------------------

fn rm_check(_: &ShimHost, _: &[String], out: &Output) -> Result<(), String> {
    if out.status.success() {
        return Err("rm claimed a proof through a failed command".into());
    }
    Ok(())
}

#[test]
fn rm_of_a_running_job() {
    exercise(&Scenario {
        name: "rm-running",
        hold_for: None,
        max_call: None,
        setup: |h| vec![running(h, &["run", "--quiet", "sleep 60"])],
        verb: |ids| vec!["rm".into(), ids[0].clone()],
        check: rm_check,
    });
}

#[test]
fn rm_of_a_done_job() {
    exercise(&Scenario {
        name: "rm-done",
        hold_for: None,
        max_call: None,
        setup: |h| {
            // The job first: a server the test started would hand its own
            // environment (another XDG_STATE_HOME) to the job's pane.
            let id = finished(h, &["run", "--quiet", "exit 1"]);
            h.tmux
                .command(&["new-session", "-d", "-s", "keeper", "sleep 60"])
                .status()
                .unwrap();
            vec![id]
        },
        verb: |ids| vec!["rm".into(), ids[0].clone()],
        check: rm_check,
    });
}

// -- run --key ----------------------------------------------------------------------

/// After the run: no job dir beyond `ids`, and an id on stdout only if it
/// is the job holding the key.
fn no_second_job(host: &ShimHost, ids: &[String], out: &Output) -> Result<(), String> {
    let now = jobs(host);
    let before: BTreeSet<String> = ids.iter().cloned().collect();
    let extra: Vec<_> = now.difference(&before).collect();
    if !extra.is_empty() {
        return Err(format!("a second job was made: {extra:?}"));
    }
    let printed = stdout(out);
    if !printed.is_empty() && printed != ids[0] {
        return Err(format!("printed {printed}, not the key's job {}", ids[0]));
    }
    kept(host, ids)
}

#[test]
fn run_key_of_an_existing_key() {
    exercise(&Scenario {
        name: "run-key",
        hold_for: None,
        max_call: None,
        setup: |h| vec![running(h, &["run", "--quiet", "--key", "mu.k", "sleep 60"])],
        verb: |_| {
            vec![
                "run".into(),
                "--quiet".into(),
                "--key".into(),
                "mu.k".into(),
                "sleep 60".into(),
            ]
        },
        check: no_second_job,
    });
}

#[test]
fn run_key_of_a_new_key() {
    exercise(&Scenario {
        name: "run-key-new",
        hold_for: None,
        max_call: None,
        setup: |h| {
            vec![running(
                h,
                &["run", "--quiet", "--key", "mu.other", "sleep 60"],
            )]
        },
        verb: |_| {
            vec![
                "run".into(),
                "--quiet".into(),
                "--key".into(),
                "mu.new".into(),
                "sleep 60".into(),
            ]
        },
        check: |host, ids, out| {
            kept(host, ids)?;
            let holders: Vec<_> = jobs(host)
                .into_iter()
                .filter(|id| key_of(host, id).as_deref() == Some("mu.new"))
                .collect();
            if holders.len() > 1 {
                return Err(format!("two jobs hold the key: {holders:?}"));
            }
            let printed = stdout(out);
            if out.status.success() && !holders.contains(&printed) {
                return Err(format!("printed {printed}, which does not hold the key"));
            }
            Ok(())
        },
    });
}

// -- keyed lease (claim) -----------------------------------------------------------

fn lease_verb(file: &str) -> Vec<String> {
    vec![
        "run".into(),
        "--lease".into(),
        "--key".into(),
        "mu.lease".into(),
        "--job-file".into(),
        file.into(),
        // The watchdog's dispatch path too.
        "--max-secs".into(),
        "30".into(),
        "--".into(),
        // A second of life: the dispatch's advisory count runs its `awk`
        // only while the job's server is up, and an instant `exit 4` raced
        // it, so the command count varied from run to run.
        "sleep 1; exit 4".into(),
    ]
}

#[test]
fn keyed_lease_of_an_existing_key() {
    // The claim finds the done job; its claim state and the adoption probe
    // follow. Injected in those three calls, the lease must start nothing
    // and name only that job.
    exercise(&Scenario {
        name: "lease-existing",
        hold_for: None,
        max_call: Some(3),
        setup: |h| {
            let id = finished(h, &["run", "--quiet", "--key", "mu.lease", "exit 4"]);
            vec![id, h.dir.join("w.job").display().to_string()]
        },
        verb: |ids| lease_verb(&ids[1]),
        check: |host, ids, out| {
            let only = &ids[..1];
            let now = jobs(host);
            if now.len() != 1 || !now.contains(&ids[0]) {
                return Err(format!("jobs are {now:?}"));
            }
            if let Ok(named) = std::fs::read_to_string(&ids[1])
                && named.trim() != ids[0]
            {
                return Err(format!("job file names {}", named.trim()));
            }
            match out.status.code() {
                Some(4) | Some(1) => kept(host, only),
                other => Err(format!("exit {other:?}")),
            }
        },
    });
}

#[test]
fn keyed_lease_through_its_first_hold_tick() {
    // The fourth call is the hold's first probe, a screen probe (`capture-pane`
    // or the saved screen): injected there, the lease holds on and ends with
    // the job's code, never with a state it could not read.
    exercise(&Scenario {
        name: "lease-hold",
        hold_for: None,
        max_call: Some(4),
        setup: |h| {
            let id = finished(
                h,
                &["run", "--quiet", "--tui", "--key", "mu.lease", "exit 4"],
            );
            vec![id, h.dir.join("w.job").display().to_string()]
        },
        verb: |ids| lease_verb(&ids[1]),
        check: |host, ids, out| {
            let now = jobs(host);
            if now.len() != 1 || !now.contains(&ids[0]) {
                return Err(format!("jobs are {now:?}"));
            }
            match out.status.code() {
                Some(4) | Some(1) => Ok(()),
                other => Err(format!("exit {other:?}")),
            }
        },
    });
}

#[test]
fn keyed_lease_of_a_new_key() {
    // Injected in the claim or the dispatch, the lease makes at most one job
    // for the key, and its job file names that job or nothing.
    exercise(&Scenario {
        name: "lease-new",
        hold_for: None,
        max_call: Some(2),
        setup: |h| {
            std::fs::create_dir_all(h.jobs()).unwrap();
            vec![h.dir.join("w.job").display().to_string()]
        },
        verb: |ids| lease_verb(&ids[0]),
        check: |host, ids, _| {
            // A failed dispatch's detached `rm` may still be removing the
            // claim; let it.
            std::thread::sleep(Duration::from_millis(300));
            let made: Vec<_> = jobs(host).into_iter().collect();
            if made.len() > 1 {
                return Err(format!("two jobs: {made:?}"));
            }
            if let Ok(named) = std::fs::read_to_string(&ids[0])
                && made.first().is_some_and(|id| id != named.trim())
            {
                return Err(format!(
                    "job file names {}, the key's job is {:?}",
                    named.trim(),
                    made
                ));
            }
            Ok(())
        },
    });
}

// -- hostile file states, under the same assertions ----------------------------------

/// A live keyed job whose key is a symlink to an unreadable file.
fn symlinked_key(h: &ShimHost) -> String {
    let id = running(h, &["run", "--quiet", "--key", "mu.link", "sleep 60"]);
    let key = h.jobs().join(&id).join("key");
    let target = h.dir.join("unreadable-key");
    std::fs::write(&target, "mu.link\n").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::remove_file(&key).unwrap();
    std::os::unix::fs::symlink(&target, &key).unwrap();
    id
}

#[test]
fn a_symlinked_unreadable_key_is_never_an_absence() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let _one_at_a_time = serial();
    let h = fault_host("fi-symlink");
    let id = symlinked_key(&h);
    let none = Fault::new(None, Some(0));

    let ls = mule_with(&h, &none, &["ls", "--json", "--key-prefix", "mu."]);
    let run = mule_with(
        &h,
        &none,
        &["run", "--quiet", "--key", "mu.link", "sleep 60"],
    );
    let mut problems = Vec::new();
    if let Err(why) = refused(&h, &[], &ls) {
        problems.push(format!("ls --key-prefix: {why}: {}", stdout(&ls)));
    }
    if let Err(why) = no_second_job(&h, std::slice::from_ref(&id), &run) {
        problems.push(format!("run --key: {why}"));
    }
    // Aged and done, prune must not take it either.
    let h2 = fault_host("fi-symlink-prune");
    let id2 = finished(&h2, &["run", "--quiet", "--key", "mu.link", "true"]);
    let key = h2.jobs().join(&id2).join("key");
    let target = h2.dir.join("unreadable-key");
    std::fs::write(&target, "mu.link\n").unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::remove_file(&key).unwrap();
    std::os::unix::fs::symlink(&target, &key).unwrap();
    age(&h2, &id2);
    let _ = mule_with(&h2, &none, &["run", "--quiet", "true"]);
    if let Err(why) = kept(&h2, std::slice::from_ref(&id2)) {
        problems.push(format!("prune: {why}"));
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn an_unreadable_key_is_refused_with_running_too() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let _one_at_a_time = serial();
    let h = fault_host("fi-running");
    let id = running(&h, &["run", "--quiet", "--key", "mu.running", "sleep 60"]);
    let key = h.jobs().join(&id).join("key");
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::read(&key).is_ok() {
        eprintln!("skipping: modes are not enforced for this user");
        return;
    }
    let none = Fault::new(None, Some(0));

    let plain = mule_with(&h, &none, &["ls", "--json", "--key-prefix", "mu."]);
    let filtered = mule_with(
        &h,
        &none,
        &["ls", "--json", "--running", "--key-prefix", "mu."],
    );

    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut problems = Vec::new();
    for (what, out) in [("plain", &plain), ("--running", &filtered)] {
        if let Err(why) = refused(&h, &[], out) {
            problems.push(format!("{what}: {why}: {}", stdout(out)));
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// -- review round 3: the reviewer's scenarios for the in-scope verbs ---------------
//
// Adopted from the round-3 review harness. Its `tail`, `kill` and `rm --all`
// scenarios are not here: those verbs are out of scope for the discipline
// (SPEC § Remote snippets fail closed, "Not yet fail closed").

#[test]
fn r3_ls_all_with_uname() {
    exercise(&Scenario {
        name: "ls-all",
        hold_for: None,
        max_call: None,
        setup: |h| {
            let ids = ls_setup(h);
            age(h, &ids[1]);
            ids
        },
        verb: |_| vec!["ls".into(), "--json".into(), "--all".into()],
        check: ls_check,
    });
}

#[test]
fn r3_attach_of_a_running_job() {
    exercise(&Scenario {
        name: "attach-running",
        hold_for: None,
        max_call: Some(1),
        setup: |h| {
            let id = running(h, &["run", "--quiet", "--tui", "sleep 60"]);
            vec![id, h.dir.join("attach.job").display().to_string()]
        },
        verb: |ids| {
            vec![
                "run".into(),
                "--lease".into(),
                "--attach".into(),
                ids[0].clone(),
                "--job-file".into(),
                ids[1].clone(),
            ]
        },
        check: |host, ids, out| {
            refused(host, &ids[..1], out)?;
            if Path::new(&ids[1]).exists() {
                return Err("attach wrote job file after failed probe".into());
            }
            kept(host, &ids[..1])
        },
    });
}

#[test]
fn r3_exec_no_detach() {
    exercise(&Scenario {
        name: "exec-no-detach",
        hold_for: None,
        max_call: None,
        setup: |h| {
            std::fs::create_dir_all(h.jobs()).unwrap();
            vec![]
        },
        verb: |_| {
            vec![
                "exec".into(),
                "--no-detach".into(),
                "echo hello-exec; echo err-exec >&2".into(),
            ]
        },
        check: |host, _, out| {
            if out.status.success() {
                if stdout(out) != "hello-exec" {
                    return Err(format!(
                        "exec exited 0 with wrong/dropped stdout {:?}",
                        stdout(out)
                    ));
                }
                let err = String::from_utf8_lossy(&out.stderr);
                if err.trim() != "err-exec" {
                    return Err(format!(
                        "exec exited 0 with wrong/dropped stderr {:?}",
                        err.trim()
                    ));
                }
            }
            let leftover = jobs(host);
            if !leftover.is_empty() {
                return Err(format!("exec left jobs behind: {leftover:?}"));
            }
            Ok(())
        },
    });
}

#[test]
fn r3_run_key_new_never_poisons_key_with_unstarted_orphan() {
    exercise(&Scenario {
        name: "run-key-new-strict",
        hold_for: None,
        max_call: None,
        setup: |h| {
            vec![running(
                h,
                &["run", "--quiet", "--key", "mu.other", "sleep 60"],
            )]
        },
        verb: |_| {
            vec![
                "run".into(),
                "--quiet".into(),
                "--key".into(),
                "mu.new".into(),
                "sleep 60".into(),
            ]
        },
        check: |host, ids, out| {
            kept(host, ids)?;
            let holders: Vec<_> = jobs(host)
                .into_iter()
                .filter(|id| key_of(host, id).as_deref() == Some("mu.new"))
                .collect();
            if holders.len() > 1 {
                return Err(format!("two jobs hold the key: {holders:?}"));
            }
            if !out.status.success() {
                for h_id in &holders {
                    let none = Fault::new(None, Some(0));
                    let polled = mule_with(host, &none, &["poll", h_id, "--json"]);
                    if let Some(v) = json(&polled)
                        && v["state"] != "unknown"
                        && v["state"] != "running"
                    {
                        return Err(format!(
                            "failed run --key left holder {h_id} in definite state {}",
                            v["state"]
                        ));
                    }
                }
            }
            Ok(())
        },
    });
}

#[test]
fn r3_run_tui_key_new_and_watchdog() {
    exercise(&Scenario {
        name: "run-tui-key-wd",
        hold_for: None,
        max_call: None,
        setup: |h| {
            std::fs::create_dir_all(h.jobs()).unwrap();
            vec![]
        },
        verb: |_| {
            vec![
                "run".into(),
                "--quiet".into(),
                "--tui".into(),
                "--max-secs".into(),
                "30".into(),
                "--key".into(),
                "mu.tui".into(),
                "true".into(),
            ]
        },
        check: |host, _, out| {
            let holders: Vec<_> = jobs(host)
                .into_iter()
                .filter(|id| key_of(host, id).as_deref() == Some("mu.tui"))
                .collect();
            if holders.len() > 1 {
                return Err(format!("two jobs hold the key: {holders:?}"));
            }
            if !out.status.success() {
                for h_id in &holders {
                    if host.tmux.has_session(&format!("mule-{h_id}")) {
                        return Err(format!(
                            "failed --tui dispatch left live session mule-{h_id} running/hung"
                        ));
                    }
                    let none = Fault::new(None, Some(0));
                    let polled = mule_with(host, &none, &["poll", h_id, "--json"]);
                    if let Some(v) = json(&polled)
                        && v["state"] != "unknown"
                    {
                        return Err(format!(
                            "failed --tui dispatch left holder {h_id} in definite state {}",
                            v["state"]
                        ));
                    }
                }
            }
            Ok(())
        },
    });
}

#[test]
fn r3_non_numeric_rc_in_ls_and_mule_regular_toctou() {
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let _one_at_a_time = serial();
    let h = fault_host("fi-bad-rc");
    let good = running(&h, &["run", "--quiet", "--key", "mu.good", "sleep 60"]);
    let bad = h.jobs().join("bad001");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(
        bad.join("cmd"),
        "true
",
    )
    .unwrap();
    std::fs::write(
        bad.join("rc"),
        "notanumber
",
    )
    .unwrap();
    let none = Fault::new(None, Some(0));
    let polled = mule_with(&h, &none, &["poll", "bad001", "--json"]);
    let listed = mule_with(&h, &none, &["ls", "--json", "--key-prefix", "mu.good"]);

    // Also test mule_regular TOCTOU: when $d/rc appears just as ls -d $d/rc runs
    // after [ -f $d/rc ] returned false.
    let h2 = fault_host("fi-toctou");
    let running_id = running(&h2, &["run", "--quiet", "sleep 60"]);
    let rc_path = h2.jobs().join(&running_id).join("rc");
    let real_ls = real("ls");
    let ls_shim = h2.dir.join("ls");
    std::fs::write(
        &ls_shim,
        format!(
            "#!/bin/sh\nfor a; do case $a in */rc) echo 0 > '{}' ;; esac; done\nexec '{real_ls}' \"$@\"\n",
            rc_path.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&ls_shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    let toctou_wait = h2.mule(&["wait", &running_id]);

    let mut problems = Vec::new();
    if json(&polled)
        .and_then(|v| v["state"].as_str().map(str::to_string))
        .as_deref()
        != Some("unknown")
    {
        problems.push(format!(
            "poll bad001: expected unknown, got {}",
            stdout(&polled)
        ));
    }
    if !listed.status.success() {
        problems.push(format!(
            "ls --json --key-prefix mu.good failed when another job (bad001) had non-numeric rc (good={good}): {}",
            String::from_utf8_lossy(&listed.stderr).trim()
        ));
    }
    if !toctou_wait.status.success() {
        problems.push(format!(
            "wait failed on mule_regular TOCTOU when rc appeared between [ -f ] and ls -d: {}",
            String::from_utf8_lossy(&toctou_wait.stderr).trim()
        ));
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

// -- the claim proof path (ruling 3) ---------------------------------------------------

/// An unstarted claim of `mu.lease` by `claimer`: a state dir with a `key`
/// and a `claim` record, and no `cmd`.
fn claim_by(h: &ShimHost, id: &str, claimer: &mule::claimer::Claimer) {
    let d = h.jobs().join(id);
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join("claim"), format!("{}\n", claimer.line())).unwrap();
    std::fs::write(d.join("key"), "mu.lease\n").unwrap();
}

/// A pid that was this host's and has exited.
fn dead_pid() -> u32 {
    let mut child = Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

fn here() -> String {
    mule::claimer::local_host().expect("a host name")
}

#[test]
fn keyed_lease_of_an_abandoned_claim() {
    // The claimer is gone (same host, pid dead), no session: the lease rolls
    // the claim back and dispatches once. Injected anywhere in the claim,
    // the claim state, the rollback, the second claim or the dispatch, it
    // makes at most one job for the key, and names that job or nothing.
    exercise(&Scenario {
        name: "lease-abandoned",
        hold_for: None,
        max_call: Some(5),
        setup: |h| {
            let claimer = mule::claimer::Claimer {
                pid: dead_pid(),
                host: here(),
                start: "Thu Jan  1 00:00:00 1970".into(),
            };
            claim_by(h, "0c1a1d", &claimer);
            vec![h.dir.join("w.job").display().to_string()]
        },
        verb: |ids| lease_verb(&ids[0]),
        check: |host, ids, _| {
            std::thread::sleep(Duration::from_millis(300));
            let holders: Vec<_> = jobs(host)
                .into_iter()
                .filter(|id| key_of(host, id).as_deref() == Some("mu.lease"))
                .collect();
            if holders.len() > 1 {
                return Err(format!("two jobs hold the key: {holders:?}"));
            }
            if let Ok(named) = std::fs::read_to_string(&ids[0])
                && !holders.iter().any(|id| id == named.trim())
                && jobs(host).contains(named.trim())
            {
                return Err(format!(
                    "job file names {}, not the key's job",
                    named.trim()
                ));
            }
            Ok(())
        },
    });
}

#[test]
fn keyed_lease_of_a_live_claimers_claim() {
    // The claimer is alive (this test process): nothing proves the claim
    // abandoned, under any injection. The lease never rolls it back and
    // never dispatches; it holds the claim as unknown until killed.
    exercise(&Scenario {
        name: "lease-claim-alive",
        hold_for: Some(Duration::from_secs(30)),
        max_call: Some(3),
        setup: |h| {
            let me = mule::claimer::Claimer {
                pid: std::process::id(),
                host: here(),
                start: mule::claimer::start_time(std::process::id()).unwrap(),
            };
            claim_by(h, "0c1a1e", &me);
            vec!["0c1a1e".into(), h.dir.join("w.job").display().to_string()]
        },
        verb: |ids| lease_verb(&ids[1]),
        check: |host, ids, _| {
            let now = jobs(host);
            if now.len() != 1 || !now.contains(&ids[0]) {
                return Err(format!("the live claim was replaced: jobs are {now:?}"));
            }
            if host.jobs().join(&ids[0]).join("cmd").exists() {
                return Err("the live claimer's job was started by another lease".into());
            }
            Ok(())
        },
    });
}

// -- a failed dispatch whose rollback cannot kill its session ------------------------

#[test]
fn a_failed_dispatch_keeps_its_key_while_its_session_will_not_die() {
    // Two faults, which the sweep never makes: the TUI `pipe-pane` fails,
    // and then so does the rollback's `kill-session`. The session may run,
    // so its dir -- the key with it -- must stay, `pending` and all: `poll`
    // says unknown, and the key's next `run` finds that job rather than
    // start a second.
    if !tmux_available() {
        eprintln!("skipping: tmux is not on PATH");
        return;
    }
    let _one_at_a_time = serial();
    let h = fault_host("fi-stuck-rollback");
    std::fs::create_dir_all(h.jobs()).unwrap();
    let bin = scratch("fi-stuck-rollback-shim");
    std::fs::write(
        bin.join("tmux"),
        format!(
            "#!/bin/sh\ncase \"$*\" in *pipe-pane\\ -O*|*kill-session*) echo 'tmux: Permission denied' >&2; exit 1;; esac\nexec '{}' \"$@\"\n",
            real("tmux")
        ),
    )
    .unwrap();
    std::fs::set_permissions(bin.join("tmux"), std::fs::Permissions::from_mode(0o755)).unwrap();

    let failed = h
        .command(&["run", "--quiet", "--tui", "--key", "mu.stuck", "true"])
        .env("PATH", h.path_env(Some(&bin)))
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let made = jobs(&h);
    let mut problems = Vec::new();
    if failed.status.success() {
        problems.push(format!("the dispatch succeeded: {}", stdout(&failed)));
    }
    let id = made.iter().next().cloned().unwrap_or_default();
    if made.len() != 1 || key_of(&h, &id).as_deref() != Some("mu.stuck") {
        problems.push(format!("the job's dir and key went: jobs are {made:?}"));
    } else {
        if !h.jobs().join(&id).join("pending").exists() {
            problems.push("no pending marker".into());
        }
        let polled = h.mule(&["poll", &id, "--json"]);
        if json(&polled).is_some_and(|v| v["state"] != "unknown") {
            problems.push(format!("poll: {}", stdout(&polled)));
        }
        let again = h.mule(&["run", "--quiet", "--key", "mu.stuck", "true"]);
        if jobs(&h).len() != 1 {
            problems.push(format!("a second job for the key: {:?}", jobs(&h)));
        }
        if again.status.success() && stdout(&again) != id {
            problems.push(format!("the retry printed {}", stdout(&again)));
        }
    }
    let _ = h
        .tmux
        .command(&["kill-session", "-t", &format!("mule-{id}")])
        .status();
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
