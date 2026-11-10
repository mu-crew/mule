//! Test layer 1 for `rm --retry-for`: a lease's signal-path stop that fails is
//! retried, then recorded (2026-11-04 aiv-blog incident; spec
//! 2026-11-04-lease-adopt § G2).
//!
//! Over `Fake`, with a virtual clock (a ten-minute deadline costs nothing) and
//! a real stop fence in a scratch directory: the fence is a local `flock`,
//! which a fake could not show waiting.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mule::config::{Config, Host};
use mule::jobs::{
    Clock, MASTER_POLL, RECORD_MAX_BYTES, RetryPolicy, StopOutcome, record_line, remove_with_retry,
};
use mule::lease::{stop_failed_path, write_line_file};
use mule::stopfence::StopFence;
use mule::transport::{Fake, MuxOp, Output, Transport};
use mule::wrapper::JobId;

fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-stop-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Safe: runs once, before any thread reads it.
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

fn host() -> Host {
    isolate_state();
    Config::parse("[hosts.stop]\ntarget = \"h\"\n")
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
}

fn id() -> JobId {
    "a1b2c3".parse().unwrap()
}

fn scratch() -> PathBuf {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "mule-stop-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    // Pids are reused, and nothing removes these dirs: a later run with
    // this run's pid found an old run's job file and fence in its scratch
    // dir (all of tests/attach.rs failed at once). Start empty.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A fence at generation `generation`, under `dir`.
fn fence_at(dir: &Path, generation: u64) -> StopFence {
    let fence = StopFence::at(dir.join("stopping").join(id().as_str()));
    let mut lock = fence.lock().unwrap();
    for _ in 0..generation {
        lock.bump().unwrap();
    }
    fence
}

type Hook = Box<dyn FnMut(Duration) + Send>;
type NowHook = Box<dyn FnMut() + Send>;

/// Virtual time. `sleep` advances it, records the request, and runs a hook
/// that can change the world mid-retry (bring the master up, bump the fence).
#[derive(Clone, Default)]
struct VClock {
    now: Arc<Mutex<Duration>>,
    sleeps: Arc<Mutex<Vec<Duration>>>,
    on_sleep: Arc<Mutex<Option<Hook>>>,
    on_now: Arc<Mutex<Option<NowHook>>>,
}

impl VClock {
    fn advance(&self, by: Duration) {
        *self.now.lock().unwrap() += by;
    }
    fn sleeps(&self) -> Vec<Duration> {
        self.sleeps.lock().unwrap().clone()
    }
    fn on_sleep(&self, hook: impl FnMut(Duration) + Send + 'static) {
        *self.on_sleep.lock().unwrap() = Some(Box::new(hook));
    }
}

impl Clock for VClock {
    fn now(&self) -> Duration {
        if let Some(hook) = self.on_now.lock().unwrap().as_mut() {
            hook();
        }
        *self.now.lock().unwrap()
    }
    fn sleep(&self, duration: Duration) {
        self.sleeps.lock().unwrap().push(duration);
        self.advance(duration);
        if let Some(hook) = self.on_sleep.lock().unwrap().as_mut() {
            hook(duration);
        }
    }
    fn unix_now(&self) -> u64 {
        1_700_000_000
    }
}

fn secs(list: &[u64]) -> Vec<Duration> {
    list.iter().copied().map(Duration::from_secs).collect()
}

fn rm_failed(stderr: &str) -> Output {
    Output::fail(1, stderr)
}

fn policy(dir: &Path, generation: u64) -> RetryPolicy {
    RetryPolicy {
        retry_for: Duration::from_secs(600),
        fence_gen: Some(generation),
        failed_file: Some(stop_failed_path(&dir.join("w.job"), &id())),
    }
}

fn record(policy: &RetryPolicy) -> Option<String> {
    std::fs::read_to_string(policy.failed_file.as_ref().unwrap()).ok()
}

#[test]
fn a_master_that_returns_after_three_checks_gets_one_rm_and_no_record() {
    let (dir, fake, clock) = (scratch(), Fake::no_master(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    let checks = Arc::new(AtomicU32::new(0));
    {
        let checks = Arc::clone(&checks);
        clock.on_sleep(move |_| {
            checks.fetch_add(1, Ordering::SeqCst);
        });
    }
    // The hook counts sleeps; the transport raises the master on the fourth
    // check, i.e. after three 5s polls found none.
    struct Raises<'a>(&'a Fake, Arc<AtomicU32>);
    impl Transport for Raises<'_> {
        fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
            self.0.run_unlocked(host, script)
        }
        fn mux_op(&self, host: &Host, op: MuxOp) -> anyhow::Result<Output> {
            if self.1.load(Ordering::SeqCst) >= 3 {
                self.0.set_master(Some(1));
            }
            self.0.mux_op(host, op)
        }
    }
    let t = Raises(&fake, Arc::clone(&checks));

    let outcome = remove_with_retry(&t, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Removed(vec![]));
    assert_eq!(outcome.exit_code(), 0);
    assert_eq!(clock.sleeps(), vec![MASTER_POLL; 3], "every 5s, lock-free");
    assert_eq!(fake.scripts().len(), 1, "one rm, and only with a master");
    assert_eq!(record(&policy), None);
    assert!(!fence.path().exists(), "a proven stop unlinks the fence");
}

#[test]
fn no_master_throughout_never_runs_rm_and_records_no_control_master() {
    let (dir, fake, clock) = (scratch(), Fake::no_master(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome.exit_code(), 3);
    assert!(fake.scripts().is_empty(), "zero rm without a master");
    assert!(
        fake.mux_ops().iter().all(|op| *op == MuxOp::Check),
        "it never opens or touches a master"
    );
    let line = record(&policy).expect("the failure is recorded");
    assert!(
        line.starts_with("a1b2c3 on stop not stopped: no control master"),
        "{line}"
    );
    assert!(
        line.contains("gave up after 0 attempts, 600s; at unix 1700000000"),
        "{line}"
    );
    assert!(line.ends_with(")\n"), "one line: {line:?}");
    assert!(
        clock.now() <= policy.retry_for + MASTER_POLL,
        "virtual time {:?} past the bound plus one interval",
        clock.now()
    );
    assert!(
        fence.path().exists(),
        "the job may still run: the fence stays"
    );
}

#[test]
fn a_failed_rm_backs_off_five_then_ten_seconds() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    fake.push(rm_failed(
        "session still alive after kill-session: mule-a1b2c3",
    ))
    .push(rm_failed("cannot verify session state: boom"))
    .push(Output::ok("a1b2c3\n"));

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Removed(vec!["a1b2c3".into()]));
    assert_eq!(clock.sleeps(), secs(&[5, 10]));
    assert_eq!(fake.scripts().len(), 3);
    assert_eq!(record(&policy), None);
}

#[test]
fn the_backoff_caps_at_thirty_seconds_and_sleeps_are_clamped_to_the_deadline() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let mut policy = policy(&dir, 0);
    policy.retry_for = Duration::from_secs(100);
    for _ in 0..10 {
        fake.push(rm_failed("nope"));
    }

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    // 5 + 10 + 20 + 30 = 65, then 30 more is 95, then the last 5 left.
    assert_eq!(clock.sleeps(), secs(&[5, 10, 20, 30, 30, 5]));
    assert_eq!(clock.now(), Duration::from_secs(100));
    // One attempt before each of the six sleeps, none after the last: the
    // clamped sleep wakes on the deadline, and no attempt starts there.
    assert_eq!(fake.scripts().len(), 6);
    assert_eq!(outcome.exit_code(), 1);
    let line = record(&policy).unwrap();
    assert!(line.contains("gave up after 6 attempts, 100s;"), "{line}");
}

/// A sleep that wakes `late` after it was due: scheduling delay, or a SIGSTOP
/// held across it.
fn oversleeps(clock: &VClock, late: Duration) {
    let extra = clock.clone();
    clock.on_sleep(move |_| extra.advance(late));
}

#[test]
fn a_sleep_that_wakes_past_the_deadline_starts_no_new_attempt() {
    // The reviewer's repro: budget 1s, the backoff clamped to 1s, the sleep
    // resumes at 900s. A new rm then would kill the job long after the
    // window, where its failure record belongs instead.
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let mut policy = policy(&dir, 0);
    policy.retry_for = Duration::from_secs(1);
    oversleeps(&clock, Duration::from_secs(899));
    fake.push(rm_failed("cannot verify session state: boom"))
        .push(Output::ok("a1b2c3\n"));

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(clock.sleeps(), secs(&[1]));
    assert_eq!(fake.scripts().len(), 1, "no rm after the deadline");
    assert!(
        matches!(outcome, StopOutcome::GaveUp { code: 1, .. }),
        "{outcome:?}"
    );
    let line = record(&policy).unwrap();
    assert!(line.contains("gave up after 1 attempts, 900s;"), "{line}");
    assert!(
        fence.path().exists(),
        "the job may still run: the fence stays"
    );
}

#[test]
fn a_master_back_only_after_an_overslept_poll_gets_no_rm() {
    // The incident's path: master down, the 5s poll wakes late, the master is
    // up by then. Past the deadline that is a record, not a late kill.
    let (dir, fake, clock) = (scratch(), Arc::new(Fake::no_master()), VClock::default());
    let fence = fence_at(&dir, 0);
    let mut policy = policy(&dir, 0);
    policy.retry_for = Duration::from_secs(5);
    {
        let (fake, extra) = (Arc::clone(&fake), clock.clone());
        clock.on_sleep(move |_| {
            extra.advance(Duration::from_secs(600));
            fake.set_master(Some(1));
        });
    }

    let outcome = remove_with_retry(&*fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert!(fake.scripts().is_empty(), "no rm after the deadline");
    assert_eq!(outcome.exit_code(), 3, "no master was seen in the window");
    let line = record(&policy).unwrap();
    assert!(
        line.starts_with(
            "a1b2c3 on stop not stopped: no control master (gave up after 0 attempts,"
        ),
        "{line}"
    );
}

#[test]
fn an_rm_that_always_fails_records_its_first_stderr_line() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    for _ in 0..64 {
        fake.push(rm_failed(
            "state dir /x could not be removed (kept)\nsecond line, never shown",
        ));
    }

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    let line = record(&policy).unwrap();
    assert!(line.starts_with("a1b2c3 "), "{line}");
    assert!(
        line.contains("not stopped: rm failed on stop: state dir /x could not be removed (kept) ("),
        "{line}"
    );
    assert!(!line.contains("second line"), "{line}");
    assert_eq!(line.lines().count(), 1);
    let n = fake.scripts().len();
    assert!(
        line.contains(&format!("gave up after {n} attempts, 600s;")),
        "{line}"
    );
    assert!(matches!(outcome, StopOutcome::GaveUp { code: 1, .. }));
    // 0600: the record sits beside a job file in a world-writable /tmp.
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(policy.failed_file.as_ref().unwrap())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600);
}

/// A transport whose one `rm` takes longer than the whole deadline: the
/// attempt in flight is not bounded, so the record is late, not absent.
struct Blocks<'a> {
    fake: &'a Fake,
    clock: VClock,
    takes: Duration,
}

impl Transport for Blocks<'_> {
    fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
        self.clock.advance(self.takes);
        let _ = self.fake.run_unlocked(host, script);
        Ok(rm_failed("cannot verify session state: hung"))
    }
    fn mux_op(&self, host: &Host, op: MuxOp) -> anyhow::Result<Output> {
        self.fake.mux_op(host, op)
    }
}

#[test]
fn an_attempt_blocked_past_the_deadline_is_one_attempt_then_a_late_record() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    let t = Blocks {
        fake: &fake,
        clock: clock.clone(),
        takes: Duration::from_secs(900),
    };

    let outcome = remove_with_retry(&t, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(
        fake.scripts().len(),
        1,
        "no second attempt after the deadline"
    );
    assert!(clock.sleeps().is_empty(), "nothing left to sleep");
    let line = record(&policy).unwrap();
    assert!(
        line.contains("gave up after 1 attempts, 900s;"),
        "overran: {line}"
    );
    assert_eq!(outcome.exit_code(), 1);
}

#[test]
fn a_long_multibyte_reason_is_cut_to_512_bytes_on_a_char_boundary_keeping_the_id() {
    let reason = "é".repeat(1000); // 2000 bytes
    assert_eq!(reason.len(), 2000);

    let line = record_line(&id(), "dev1", &reason, 42, 600, 1_700_000_000);

    assert!(line.len() <= RECORD_MAX_BYTES, "{}", line.len());
    assert!(line.starts_with("a1b2c3 on dev1 not stopped: é"));
    assert!(
        line.ends_with(" (gave up after 42 attempts, 600s; at unix 1700000000)"),
        "the counts survive: {line}"
    );
    // A `&str` is UTF-8 by construction; prove the cut did not split a char
    // by round-tripping the bytes, which is what mu reads.
    assert!(std::str::from_utf8(line.as_bytes()).is_ok());
    assert!(
        line.len() > RECORD_MAX_BYTES - 2,
        "cut no shorter than needed"
    );
}

#[test]
fn a_long_reason_through_the_loop_still_writes_a_valid_short_record() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    for _ in 0..64 {
        fake.push(rm_failed(&"ü".repeat(1000)));
    }

    remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    let bytes = std::fs::read(policy.failed_file.as_ref().unwrap()).unwrap();
    // One line of at most 512 bytes, plus its newline.
    assert!(bytes.len() <= RECORD_MAX_BYTES + 1, "{}", bytes.len());
    let text = String::from_utf8(bytes).expect("valid UTF-8");
    assert!(text.starts_with("a1b2c3 "));
}

#[test]
fn a_generation_bumped_while_sleeping_stops_nothing_and_records_nothing() {
    let (dir, fake, clock) = (scratch(), Fake::no_master(), VClock::default());
    let fence = fence_at(&dir, 4);
    let policy = policy(&dir, 4);
    {
        let fence = fence.clone();
        clock.on_sleep(move |_| {
            // An attach: it proved the job running and bumped the fence.
            fence.lock().unwrap().bump().unwrap();
        });
    }

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Fenced);
    assert_eq!(outcome.exit_code(), 0);
    assert!(fake.scripts().is_empty(), "no rm of a reattached job");
    assert_eq!(record(&policy), None);
    assert_eq!(
        fence.generation_unlocked().unwrap(),
        5,
        "the fence is attach's"
    );
}

#[test]
fn an_attach_that_waited_out_the_last_attempt_gets_no_record() {
    // The last attempt runs past the deadline while an attach waits on the
    // fence. The loop's own checks are behind it, so only the record write's
    // check under the fence can see the bump.
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    let bumper: Bumper = Arc::default();
    {
        // The first clock read after the attempt (its lock already dropped)
        // lets the attach finish, deterministically before the record write.
        let bumper = Arc::clone(&bumper);
        *clock.on_now.lock().unwrap() = Some(Box::new(move || {
            if let Some(handle) = bumper.lock().unwrap().take() {
                handle.join().unwrap();
            }
        }));
    }
    let t = Contended {
        fake: &fake,
        fence: fence.clone(),
        bumper,
        clock: Some((clock.clone(), Duration::from_secs(900))),
    };

    let outcome = remove_with_retry(&t, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Fenced);
    assert_eq!(fake.scripts().len(), 1);
    assert_eq!(
        record(&policy),
        None,
        "a reattached job is not recorded as lost"
    );
}

#[test]
fn a_fence_already_moved_when_the_child_starts_is_obeyed_at_once() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 2);
    let policy = policy(&dir, 1);

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Fenced);
    assert!(fake.scripts().is_empty());
    assert!(fake.mux_ops().is_empty(), "decided before any ssh");
}

type Bumper = Arc<Mutex<Option<std::thread::JoinHandle<u64>>>>;

/// A transport whose `rm` starts a bumper (an attach) mid-attempt and proves
/// it waits for the attempt's fence lock.
struct Contended<'a> {
    fake: &'a Fake,
    fence: StopFence,
    bumper: Bumper,
    /// How long the attempt takes in virtual time, if at all.
    clock: Option<(VClock, Duration)>,
}

impl Transport for Contended<'_> {
    fn run_unlocked(&self, host: &Host, script: &str) -> anyhow::Result<Output> {
        let _ = self.fake.run_unlocked(host, script);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let fence = self.fence.clone();
        let bumper = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            fence.lock().unwrap().bump().unwrap()
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !bumper.is_finished(),
            "the bumper must wait out the attempt"
        );
        if let Some((clock, takes)) = &self.clock {
            clock.advance(*takes);
        }
        *self.bumper.lock().unwrap() = Some(bumper);
        Ok(rm_failed(
            "session still alive after kill-session: mule-a1b2c3",
        ))
    }
    fn mux_op(&self, host: &Host, op: MuxOp) -> anyhow::Result<Output> {
        self.fake.mux_op(host, op)
    }
}

#[test]
fn a_bump_during_an_attempt_waits_for_it_and_then_fences_the_retry() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    let bumper: Bumper = Arc::default();
    {
        // The backoff after the failed attempt joins the bumper, so the next
        // iteration deterministically sees the attach's generation.
        let bumper = Arc::clone(&bumper);
        clock.on_sleep(move |_| {
            if let Some(handle) = bumper.lock().unwrap().take() {
                assert_eq!(handle.join().unwrap(), 1, "it bumped once released");
            }
        });
    }
    let t = Contended {
        fake: &fake,
        fence: fence.clone(),
        bumper,
        clock: None,
    };

    let outcome = remove_with_retry(&t, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Fenced);
    assert_eq!(fake.scripts().len(), 1, "no further attempt after the bump");
    assert_eq!(clock.sleeps(), secs(&[5]));
    assert_eq!(record(&policy), None);
}

#[test]
fn success_removes_its_own_record_and_leaves_another_jobs() {
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = fence_at(&dir, 0);
    let policy = policy(&dir, 0);
    let ours = policy.failed_file.clone().unwrap();
    let other: JobId = "ffff00".parse().unwrap();
    let theirs = stop_failed_path(&dir.join("w.job"), &other);
    write_line_file(&ours, "a1b2c3 on stop not stopped: an earlier child").unwrap();
    write_line_file(&theirs, "ffff00 on stop not stopped: a newer job").unwrap();

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    assert_eq!(outcome.exit_code(), 0);
    assert!(!ours.exists(), "its own record goes on success");
    assert_eq!(
        std::fs::read_to_string(&theirs).unwrap(),
        "ffff00 on stop not stopped: a newer job\n",
        "another job's record under the same name is untouched"
    );
}

#[test]
fn the_record_path_names_the_job_beside_the_job_file() {
    assert_eq!(
        stop_failed_path(Path::new("/tmp/w1.job"), &id()),
        Path::new("/tmp/w1.job.a1b2c3.stop-failed")
    );
}

#[test]
fn without_a_fence_or_record_the_retry_still_runs_and_reports() {
    let (dir, fake, clock) = (scratch(), Fake::no_master(), VClock::default());
    let fence = StopFence::at(dir.join("never"));
    let policy = RetryPolicy {
        retry_for: Duration::from_secs(12),
        fence_gen: None,
        failed_file: None,
    };

    let outcome = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence).unwrap();

    match outcome {
        StopOutcome::GaveUp { line, code } => {
            assert_eq!(code, 3);
            assert!(line.starts_with("a1b2c3 on stop not stopped: no control master"));
        }
        other => panic!("{other:?}"),
    }
    assert!(!fence.path().exists(), "an unfenced stop creates no fence");
    assert_eq!(clock.sleeps(), secs(&[5, 5, 2]));
}

// -- the fence itself ---------------------------------------------------------

#[test]
fn a_missing_fence_is_generation_zero_and_bumps_count_up() {
    let fence = StopFence::at(scratch().join("stopping/a1b2c3"));
    assert_eq!(fence.generation_unlocked().unwrap(), 0);
    {
        let mut lock = fence.lock().unwrap();
        assert_eq!(lock.generation().unwrap(), 0);
        assert_eq!(lock.bump().unwrap(), 1);
        assert_eq!(lock.generation().unwrap(), 1);
        for _ in 0..9 {
            lock.bump().unwrap();
        }
    }
    assert_eq!(fence.generation_unlocked().unwrap(), 10);
    assert_eq!(std::fs::read_to_string(fence.path()).unwrap(), "10");
}

#[test]
fn the_fence_excludes_a_second_holder_until_released() {
    let fence = StopFence::at(scratch().join("stopping/a1b2c3"));
    let held = fence.lock().unwrap();
    assert!(fence.try_lock().unwrap().is_none());
    drop(held);
    assert!(fence.try_lock().unwrap().is_some());
}

#[test]
fn rm_retry_flags_need_an_id_and_refuse_all() {
    use clap::Parser;
    use mule::cli::Cli;
    let ok = |args: &[&str]| Cli::try_parse_from(args).is_ok();
    assert!(ok(&["mule", "rm", "a1b2c3", "--retry-for", "600"]));
    assert!(ok(&[
        "mule",
        "rm",
        "a1b2c3",
        "--retry-for",
        "600",
        "--fence-gen",
        "3",
        "--failed-file",
        "/tmp/x"
    ]));
    assert!(!ok(&["mule", "rm", "--all", "--retry-for", "600"]));
    assert!(
        !ok(&["mule", "rm", "a1b2c3", "--fence-gen", "3"]),
        "requires --retry-for"
    );
    assert!(
        !ok(&["mule", "rm", "a1b2c3", "--failed-file", "/x"]),
        "requires --retry-for"
    );
}

// -- the generation a carrier owns (review of mule_stop, findings 1 and 2) ----

fn lease_request() -> mule::lease::Request<'static> {
    mule::lease::Request {
        cmd: "pi",
        cwd: None,
        max_secs: None,
        metadata: mule::wrapper::JobMetadata::Human,
    }
}

/// `lease::start` with its job file in `dir` and the fences where production
/// keeps them (this test process's isolated state dir), so a stop that read
/// the fence again would see what an attach did to it.
fn lease_start(fake: &Fake, dir: &Path) -> anyhow::Result<mule::lease::Owned> {
    fake.push(Output::ok("1\n"));
    let host = host();
    mule::lease::start(
        fake,
        &host,
        &dir.join("w.job"),
        &StopFence::dir(),
        &lease_request(),
        &|_| {},
        &mut std::io::sink(),
    )
}

#[test]
fn an_attach_after_the_old_carrier_let_go_is_not_stopped_by_its_late_stop() {
    // The reviewer's race: the old carrier's pane is gone and its probe and
    // ticket released; an attach proves the job running and bumps the fence;
    // only then does the old signal thread build its stop. Read at that
    // moment, the generation was attach's own, and the child killed the
    // reattached job. Owned at dispatch, it is the old one, and stands down.
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let owned = lease_start(&fake, &dir).unwrap();
    assert_eq!(owned.fence_gen, 0, "a fresh id has no fence: generation 0");
    let fence = StopFence::for_job(&owned.id);
    assert_eq!(fence.lock().unwrap().bump().unwrap(), 1, "the attach");

    let failed = stop_failed_path(&dir.join("w.job"), &owned.id);
    let retry = mule::lease::stop_retry(&owned, &failed, Duration::from_secs(600));
    assert_eq!(
        retry.fence_gen,
        Some(0),
        "the owned generation, not attach's"
    );
    let policy = RetryPolicy {
        retry_for: retry.retry_for,
        fence_gen: retry.fence_gen,
        failed_file: retry.failed_file.map(Path::to_path_buf),
    };
    let dispatched = fake.scripts().len();

    let outcome = remove_with_retry(&fake, &host(), &owned.id, &policy, &clock, &fence).unwrap();

    assert_eq!(outcome, StopOutcome::Fenced);
    assert_eq!(
        fake.scripts().len(),
        dispatched,
        "no rm of the reattached job"
    );
    assert!(!failed.exists(), "and no record calling it lost");
}

#[test]
fn a_lease_stop_always_carries_the_owned_generation() {
    // The old fallback dropped --fence-gen on any read error, arming a stop
    // no bump could cancel. Nothing is read at signal time now, so even a
    // fence wrecked after dispatch (a directory, as in the repro) cannot
    // change the argv.
    let dir = scratch();
    let owned = mule::lease::Owned {
        id: id(),
        fence_gen: 7,
    };
    std::fs::create_dir_all(dir.join("stopping").join(id().as_str())).unwrap();
    let failed = stop_failed_path(&dir.join("w.job"), &id());

    let retry = mule::lease::stop_retry(&owned, &failed, Duration::from_secs(600));
    let command = mule::cleanup::rm_command(Path::new("/m"), None, "stop", &id(), Some(&retry));

    let args: Vec<_> = command.get_args().map(|a| a.to_str().unwrap()).collect();
    let at = args
        .iter()
        .position(|a| *a == "--fence-gen")
        .expect("fenced");
    assert_eq!(args[at + 1], "7");
}

#[test]
fn an_unreadable_fence_at_start_launches_nothing() {
    // `stopping` is a file, so the fence cannot be read (ENOTDIR). Without a
    // generation the stop could not be fenced, so the lease refuses before
    // any job exists.
    let (dir, fake) = (scratch(), Fake::new());
    std::fs::write(dir.join("stopping"), "not a directory").unwrap();

    let error = mule::lease::start(
        &fake,
        &host(),
        &dir.join("w.job"),
        &dir.join("stopping"),
        &lease_request(),
        &|_| panic!("nothing may be dispatched"),
        &mut std::io::sink(),
    )
    .unwrap_err();

    assert!(format!("{error:#}").contains("stop fence"), "{error:#}");
    assert_eq!(mule::errors::exit_code(&error), 1);
    assert!(fake.scripts().is_empty(), "no dispatch");
    assert!(fake.mux_ops().is_empty(), "not even a master check");
}

#[test]
fn an_invalid_fence_is_an_error_not_a_guess() {
    let fence = StopFence::at(scratch().join("stopping/a1b2c3"));
    std::fs::create_dir_all(fence.path().parent().unwrap()).unwrap();
    std::fs::write(fence.path(), "garbage").unwrap();
    let error = fence.generation_unlocked().unwrap_err();
    assert!(
        format!("{error:#}").contains("invalid stop fence"),
        "{error:#}"
    );
}

#[test]
fn a_fenced_child_whose_fence_breaks_runs_no_rm() {
    // Fail closed: a generation that cannot be checked under the lock is
    // never treated as a match.
    let (dir, fake, clock) = (scratch(), Fake::new(), VClock::default());
    let fence = StopFence::at(dir.join("stopping").join(id().as_str()));
    std::fs::create_dir_all(fence.path()).unwrap();
    let policy = policy(&dir, 0);

    let result = remove_with_retry(&fake, &host(), &id(), &policy, &clock, &fence);

    assert!(result.is_err(), "{result:?}");
    assert!(fake.scripts().is_empty(), "no rm without a checked fence");
}
