use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// `lock_path` honours `$XDG_STATE_HOME`, and without this the suite writes one
/// directory per test run into the developer's real `~/.local/state/mule`. A
/// full run left 151 of them behind, mixed in with live job state.
///
/// `set_var` is safe here because it runs once, before any thread that reads it.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("mule-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("XDG_STATE_HOME", &dir) };
    });
}

/// Serialises the timing-sensitive lock tests against each other.
///
/// Two tests here deliberately create lock contention, and running them
/// concurrently means each measures the other's load rather than the lock's
/// fairness. That is not a flaky assertion, it is the wrong experiment: the
/// property under test is per-host queue behaviour, so a second unrelated queue
/// running beside it is noise.
static TIMING: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn timing_lane() -> std::sync::MutexGuard<'static, ()> {
    TIMING.lock().unwrap_or_else(|e| e.into_inner())
}

#[test]
fn concurrent_callers_are_granted_the_lock_in_ticket_order() {
    isolate_state();
    let host = format!("locktest-fifo-{}", std::process::id());
    let path = mule::lock::lock_path(&host);
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let holder_host = host.clone();
    let holder = std::thread::spawn(move || {
        mule::lock::with_lock(&holder_host, || {
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
    });
    held_rx.recv().unwrap();

    let granted = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut callers = Vec::new();
    for expected_ticket in 1..=4 {
        let host = host.clone();
        let granted = Arc::clone(&granted);
        callers.push(std::thread::spawn(move || {
            mule::lock::with_lock(&host, || granted.lock().unwrap().push(expected_ticket)).unwrap();
        }));
        while fs::read_to_string(path.join("next")).unwrap().trim()
            != (expected_ticket + 1).to_string()
        {
            std::thread::yield_now();
        }
    }

    release_tx.send(()).unwrap();
    holder.join().unwrap();
    for caller in callers {
        caller.join().unwrap();
    }

    // Fairness is FIFO, an ordering property rather than a duration. The naive
    // retrying mutex measured waits of 6x the work, including one 0.25s
    // operation waiting 4.14s. The first replacement asserted an absolute
    // 600ms and flaked under thirteen parallel test binaries. Scaling that to
    // 3x four callers' 50ms work still measured scheduler starvation and also
    // flaked. Recording the requested ticket order and granted order directly
    // cannot be changed by machine load.
    assert_eq!(*granted.lock().unwrap(), [1, 2, 3, 4]);
}

#[test]
fn mutual_exclusion_holds() {
    isolate_state();
    let host = format!("locktest-exclusive-{}", std::process::id());
    let inside = Arc::new(AtomicUsize::new(0));
    let mut threads = Vec::new();

    for _ in 0..4 {
        let host = host.clone();
        let inside = Arc::clone(&inside);
        threads.push(std::thread::spawn(move || {
            for _ in 0..10 {
                mule::lock::with_lock(&host, || {
                    assert_eq!(inside.fetch_add(1, Ordering::SeqCst), 0);
                    std::thread::sleep(Duration::from_millis(5));
                    assert_eq!(inside.fetch_sub(1, Ordering::SeqCst), 1);
                })
                .unwrap();
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn dead_holder_is_stolen() {
    isolate_state();
    let host = format!("locktest-dead-{}", std::process::id());
    let path = mule::lock::lock_path(&host);
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("serving"), "0\n").unwrap();
    fs::write(path.join("next"), "1\n").unwrap();
    fs::write(path.join("holder"), "999999\n").unwrap();

    let started = Instant::now();
    mule::lock::with_lock(&host, || {}).unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "dead holder was not stolen promptly"
    );
}

#[test]
fn two_hosts_do_not_serialise() {
    isolate_state();
    let suffix = std::process::id();
    let host_a = format!("locktest-host-a-{suffix}");
    let host_b = format!("locktest-host-b-{suffix}");
    let (held_tx, held_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let holder = std::thread::spawn(move || {
        mule::lock::with_lock(&host_a, || {
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
    });
    held_rx.recv().unwrap();

    // Ordering, not timing: host A's holder is still inside its critical
    // section (it blocks on `release_rx` until we say so), so if B's
    // acquisition completes at all before we release A, B did not queue behind
    // A. That is exactly the property -- one lock file per host -- and it needs
    // no clock.
    //
    // This previously asserted `elapsed() < 200ms`, which measured the
    // MACHINE's load rather than the lock: it failed under a full `cargo test`
    // running thirteen test binaries in parallel, while passing alone.
    mule::lock::with_lock(&host_b, || {}).unwrap();

    release_tx.send(()).unwrap();
    holder.join().unwrap();
}

#[test]
fn a_ticket_abandoned_before_its_turn_does_not_wedge_the_host() {
    isolate_state();
    // The dangerous shape of a dead caller: it claimed a ticket, then died
    // BEFORE its turn arrived, so it never wrote `holder`. The holder-stealing
    // branch cannot see it -- there is no holder -- and every later caller
    // queues behind a number that will never be claimed. Reproduced as an
    // indefinite wedge before `waiter.<n>` files existed.

    let host = format!("abandon-{}", std::process::id());
    let p = mule::lock::lock_path(&host);
    std::fs::create_dir_all(&p).unwrap();
    // Simulate: tickets 0 and 1 were handed out; 0 was abandoned (died before
    // ever writing `holder`), so serving sits at 0 with no holder on disk.
    std::fs::write(p.join("next"), "2\n").unwrap();
    std::fs::write(p.join("serving"), "0\n").unwrap();
    let _ = std::fs::remove_file(p.join("holder"));

    let start = std::time::Instant::now();
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d2 = done.clone();
    let h = host.clone();
    std::thread::spawn(move || {
        mule::lock::with_lock(&h, || ()).unwrap();
        d2.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    while start.elapsed() < std::time::Duration::from_secs(3) {
        if done.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    std::fs::remove_dir_all(&p).ok();
    assert!(
        done.load(std::sync::atomic::Ordering::SeqCst),
        "a lock claimed by a caller that died before its turn wedged the host for {:?}",
        start.elapsed()
    );
}

#[test]
fn waiter_files_do_not_accumulate() {
    isolate_state();
    // `waiter.<n>` is per-ticket, so a long-lived host directory would collect
    // one file per lock acquisition ever made if they were not cleaned up.
    let host = format!("waiters-{}", std::process::id());
    let dir = mule::lock::lock_path(&host);
    for _ in 0..12 {
        mule::lock::with_lock(&host, || ()).unwrap();
    }
    let strays: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("waiter."))
        .collect();
    std::fs::remove_dir_all(&dir).ok();
    assert!(strays.is_empty(), "leaked waiter files: {strays:?}");
}

#[test]
fn the_transport_locks_without_the_caller_asking() {
    isolate_state();
    let _lane = timing_lane();
    // The lock covers every ssh mule issues except `master_alive`. That used to
    // be prose, enforced by six call sites each remembering to wrap the
    // transport in `with_lock` -- a seventh that forgot would compile, pass
    // every test, and quietly reintroduce the contention the tool exists to
    // remove. Now `Transport::run` is a provided method that takes the lock
    // itself, so forgetting is not expressible.
    //
    // Proven by observation rather than inspection: a transport whose unlocked
    // primitive blocks lets us check that a second caller cannot get in.
    use mule::transport::{Output, Transport};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Blocking {
        inside: AtomicUsize,
        peak: AtomicUsize,
    }
    impl Transport for Blocking {
        fn run_unlocked(
            &self,
            _host: &mule::config::Host,
            _script: &str,
        ) -> anyhow::Result<Output> {
            let n = self.inside.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(40));
            self.inside.fetch_sub(1, Ordering::SeqCst);
            Ok(Output::ok(""))
        }
        fn mux_op(
            &self,
            _host: &mule::config::Host,
            _op: mule::transport::MuxOp,
        ) -> anyhow::Result<Output> {
            Ok(Output::ok(""))
        }
    }

    let cfg = mule::config::Config::parse(&format!(
        "[hosts.locktest-transport-{}]\ntarget = \"h\"\n",
        std::process::id()
    ))
    .unwrap();
    let host = cfg.host(None).unwrap();
    let transport = Arc::new(Blocking {
        inside: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });

    let mut handles = vec![];
    for _ in 0..4 {
        let t = Arc::clone(&transport);
        let h = host.clone();
        handles.push(std::thread::spawn(move || {
            t.run(&h, "echo hi").unwrap();
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(
        transport.peak.load(Ordering::SeqCst),
        1,
        "Transport::run must serialise: four callers overlapped inside the \
         unlocked primitive, so the lock is not being applied"
    );

    let dir = mule::lock::lock_path(&host.name);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn mux_ops_never_take_the_lock_while_run_callers_still_never_overlap() {
    isolate_state();
    let _lane = timing_lane();
    // The other half of the observation above: `mux_op` is the lone lock-free
    // path, so it must get through while a `run` holds the ticket. If it
    // queued, a lease's drift check would wait behind every probe.
    use mule::transport::{MuxOp, Output, Transport};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Probe {
        inside: AtomicUsize,
        peak: AtomicUsize,
        mux_during_run: AtomicUsize,
    }
    impl Transport for Probe {
        fn run_unlocked(
            &self,
            _host: &mule::config::Host,
            _script: &str,
        ) -> anyhow::Result<Output> {
            let n = self.inside.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(40));
            self.inside.fetch_sub(1, Ordering::SeqCst);
            Ok(Output::ok(""))
        }
        fn mux_op(&self, _host: &mule::config::Host, _op: MuxOp) -> anyhow::Result<Output> {
            if self.inside.load(Ordering::SeqCst) > 0 {
                self.mux_during_run.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Output::ok(""))
        }
    }

    let cfg = mule::config::Config::parse(&format!(
        "[hosts.locktest-mux-{}]\ntarget = \"h\"\n",
        std::process::id()
    ))
    .unwrap();
    let host = cfg.host(None).unwrap();
    let transport = Arc::new(Probe {
        inside: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        mux_during_run: AtomicUsize::new(0),
    });

    let mut handles = vec![];
    for _ in 0..4 {
        let (t, h) = (Arc::clone(&transport), host.clone());
        handles.push(std::thread::spawn(move || {
            t.run(&h, "echo hi").unwrap();
        }));
    }
    // Poll the ops while the four runs queue and execute: 160ms of work.
    let started = Instant::now();
    let mut ops = 0;
    while started.elapsed() < Duration::from_millis(120) {
        transport.mux_op(host, MuxOp::Check).unwrap();
        transport
            .mux_op(host, MuxOp::CancelUnix("/l".into(), "/r".into()))
            .unwrap();
        ops += 2;
        std::thread::sleep(Duration::from_millis(5));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(transport.peak.load(Ordering::SeqCst), 1, "runs overlapped");
    assert!(
        transport.mux_during_run.load(Ordering::SeqCst) > 0,
        "{ops} mux ops ran and none overlapped a run: they are queueing"
    );
    assert!(
        transport.master_alive(host),
        "master_alive is Check through mux_op, so it is lock-free too"
    );

    std::fs::remove_dir_all(mule::lock::lock_path(&host.name)).ok();
}

/// A transport that answers each `run_unlocked` from a script of replies and
/// records, per attempt, the ticket it ran under -- read from the lock
/// directory, which is the only witness of re-queueing.
mod refusal {
    use super::*;
    use mule::errors::MuleError;
    use mule::transport::{MuxOp, Output, Transport};
    use std::sync::Mutex;

    pub enum Reply {
        Refused,
        Busy,
        Ok,
    }

    pub struct Scripted {
        pub replies: Mutex<std::collections::VecDeque<Reply>>,
        pub tickets: Mutex<Vec<u64>>,
        pub master: std::sync::atomic::AtomicBool,
        pub checks: AtomicUsize,
        pub lock: std::path::PathBuf,
    }

    impl Scripted {
        pub fn new(host: &mule::config::Host, replies: Vec<Reply>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                tickets: Mutex::default(),
                master: true.into(),
                checks: AtomicUsize::new(0),
                lock: mule::lock::lock_path(&host.name),
            }
        }
        pub fn attempts(&self) -> usize {
            self.tickets.lock().unwrap().len()
        }
    }

    impl Transport for Scripted {
        fn run_unlocked(&self, _: &mule::config::Host, _: &str) -> anyhow::Result<Output> {
            let serving = fs::read_to_string(self.lock.join("serving")).unwrap();
            self.tickets
                .lock()
                .unwrap()
                .push(serving.trim().parse().unwrap());
            match self.replies.lock().unwrap().pop_front() {
                Some(Reply::Refused) => Err(MuleError::MuxRefusedPreExec.into()),
                Some(Reply::Busy) => Err(MuleError::SessionChannelBusy.into()),
                Some(Reply::Ok) | None => Ok(Output::ok("done")),
            }
        }
        fn mux_op(&self, _: &mule::config::Host, op: MuxOp) -> anyhow::Result<Output> {
            assert_eq!(op, MuxOp::Check);
            self.checks.fetch_add(1, Ordering::SeqCst);
            Ok(if self.master.load(Ordering::SeqCst) {
                Output::ok("")
            } else {
                Output::fail(255, "Control socket connect: No such file or directory")
            })
        }
    }

    pub fn host(tag: &str) -> mule::config::Host {
        mule::config::Config::parse(&format!(
            "[hosts.locktest-refusal-{tag}-{}]\ntarget = \"h\"\n",
            std::process::id()
        ))
        .unwrap()
        .host(None)
        .unwrap()
        .clone()
    }
}

#[test]
fn a_proven_refusal_is_retried_on_a_fresh_ticket_until_it_succeeds() {
    use refusal::*;
    isolate_state();
    let host = host("recover");
    let t = Scripted::new(&host, vec![Reply::Refused, Reply::Refused, Reply::Ok]);

    let out = mule::transport::Transport::run(&t, &host, "rm job").unwrap();

    assert_eq!(out.stdout, b"done");
    let tickets = t.tickets.lock().unwrap().clone();
    // A fresh ticket per attempt: the refused attempt gave its ticket back
    // before backing off, rather than holding the queue through the wait.
    assert_eq!(tickets, [0, 1, 2]);
    // The master was asked between attempts, lock-free.
    assert_eq!(t.checks.load(Ordering::SeqCst), 2);
    std::fs::remove_dir_all(mule::lock::lock_path(&host.name)).ok();
}

#[test]
fn a_refused_caller_re_queues_behind_a_caller_that_arrived_during_its_backoff() {
    use refusal::*;
    isolate_state();
    let _lane = timing_lane();
    let host = host("requeue");
    let t = Arc::new(Scripted::new(
        &host,
        vec![Reply::Refused, Reply::Ok, Reply::Ok],
    ));
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));

    // The refused caller's first attempt is ticket 0; while it backs off
    // (100ms), a second caller takes ticket 1 and must be served first.
    let first = {
        let (t, h, order) = (Arc::clone(&t), host.clone(), Arc::clone(&order));
        std::thread::spawn(move || {
            mule::transport::Transport::run(&*t, &h, "a").unwrap();
            order.lock().unwrap().push("first");
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while t.attempts() == 0 {
        assert!(Instant::now() < deadline, "first attempt never ran");
        std::thread::sleep(Duration::from_millis(1));
    }
    let second = {
        let (t, h, order) = (Arc::clone(&t), host.clone(), Arc::clone(&order));
        std::thread::spawn(move || {
            mule::transport::Transport::run(&*t, &h, "b").unwrap();
            order.lock().unwrap().push("second");
        })
    };
    first.join().unwrap();
    second.join().unwrap();

    assert_eq!(*order.lock().unwrap(), ["second", "first"]);
    assert_eq!(*t.tickets.lock().unwrap(), [0, 1, 2]);
    std::fs::remove_dir_all(mule::lock::lock_path(&host.name)).ok();
}

#[test]
fn refusal_recovery_is_bounded_and_ends_as_the_busy_error() {
    use mule::errors::MuleError;
    use refusal::*;
    isolate_state();
    let _lane = timing_lane();
    let host = host("bounded");
    // Refused forever.
    let t = Scripted::new(&host, (0..1000).map(|_| Reply::Refused).collect());

    let started = Instant::now();
    let error =
        mule::transport::run_within(&t, &host, "rm job", Duration::from_millis(750)).unwrap_err();
    let took = started.elapsed();

    assert_eq!(
        error.downcast_ref::<MuleError>(),
        Some(&MuleError::SessionChannelBusy),
        "exhaustion is the user-facing busy error, never the internal one: {error:#}"
    );
    assert_eq!(mule::errors::exit_code(&error), 1);
    // Backoffs 100, 200, 400ms fit in 750ms; the next (800ms) never does, so
    // the loop stops before overrunning rather than sleeping past the budget.
    // The count is 3 or 4 depending on scheduling, never more.
    assert!(took < Duration::from_millis(1050), "{took:?}");
    assert!((3..=4).contains(&t.attempts()), "{}", t.attempts());
    std::fs::remove_dir_all(mule::lock::lock_path(&host.name)).ok();
}

#[test]
fn nothing_but_a_proven_refusal_with_a_live_master_is_retried() {
    use mule::errors::MuleError;
    use refusal::*;
    isolate_state();

    // Busy (ambiguous keyboard-interactive, or refusal with output): once.
    let host_a = host("busy");
    let t = Scripted::new(&host_a, vec![Reply::Busy, Reply::Ok]);
    let error = mule::transport::Transport::run(&t, &host_a, "x").unwrap_err();
    assert_eq!(
        error.downcast_ref::<MuleError>(),
        Some(&MuleError::SessionChannelBusy)
    );
    assert_eq!(t.attempts(), 1);
    assert_eq!(t.checks.load(Ordering::SeqCst), 0);
    std::fs::remove_dir_all(mule::lock::lock_path(&host_a.name)).ok();

    // A proven refusal with the master gone: no point waiting, once.
    let host_b = host("nomaster");
    let t = Scripted::new(&host_b, vec![Reply::Refused, Reply::Ok]);
    t.master.store(false, Ordering::SeqCst);
    let error = mule::transport::Transport::run(&t, &host_b, "x").unwrap_err();
    assert_eq!(
        error.downcast_ref::<MuleError>(),
        Some(&MuleError::SessionChannelBusy)
    );
    assert_eq!(t.attempts(), 1);
    std::fs::remove_dir_all(mule::lock::lock_path(&host_b.name)).ok();
}
