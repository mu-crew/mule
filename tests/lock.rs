use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// Point the lock directory at a temp dir for the whole test binary.
///
/// `lock_path` honours `$XDG_STATE_HOME`, and without this the suite writes one
/// directory per test run into the developer's real `~/.local/state/coop`. A
/// full run left 151 of them behind, mixed in with live job state.
///
/// `set_var` is safe here because it runs once, before any thread that reads it.
fn isolate_state() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let dir = std::env::temp_dir().join(format!("coop-state-{}", std::process::id()));
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
fn four_concurrent_callers_all_wait_bounded() {
    isolate_state();
    let _lane = timing_lane();
    let host = format!("locktest-bounded-{}", std::process::id());
    let mut hs = vec![];
    for _ in 0..4 {
        let h = host.clone();
        hs.push(std::thread::spawn(move || {
            let mut worst = Duration::ZERO;
            for _ in 0..5 {
                let t = Instant::now();
                coop::lock::with_lock(&h, || {
                    std::thread::sleep(Duration::from_millis(50));
                })
                .unwrap();
                worst = worst.max(t.elapsed());
            }
            worst
        }));
    }
    let worst = hs.into_iter().map(|h| h.join().unwrap()).max().unwrap();

    // Bounded RELATIVE to the work in the queue, not against a wall-clock
    // constant. Four callers each doing 50ms of work means a fair queue makes
    // a caller wait for at most the three ahead of it, so ~4x50ms plus
    // per-handoff overhead. The failure this guards against is unbounded
    // starvation: the naive spin measured 6x its work, with one 0.25s
    // operation waiting 4.14s.
    //
    // An absolute bound (600ms) measured the MACHINE, not the lock -- it
    // flaked under a full `cargo test` running thirteen binaries in parallel
    // while passing alone. Scaling by the work keeps the property under load,
    // where a starvation bug is most likely to show.
    let work = Duration::from_millis(50);
    let fair_ceiling = work * 4;
    assert!(
        worst < fair_ceiling * 3,
        "worst wait {worst:?} against a fair ceiling of {fair_ceiling:?}; \
         a fair queue tops out near the ceiling, a starving one runs away"
    );
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
                coop::lock::with_lock(&host, || {
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
    let path = coop::lock::lock_path(&host);
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("serving"), "0\n").unwrap();
    fs::write(path.join("next"), "1\n").unwrap();
    fs::write(path.join("holder"), "999999\n").unwrap();

    let started = Instant::now();
    coop::lock::with_lock(&host, || {}).unwrap();

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
        coop::lock::with_lock(&host_a, || {
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
    coop::lock::with_lock(&host_b, || {}).unwrap();

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
    let p = coop::lock::lock_path(&host);
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
        coop::lock::with_lock(&h, || ()).unwrap();
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
    let dir = coop::lock::lock_path(&host);
    for _ in 0..12 {
        coop::lock::with_lock(&host, || ()).unwrap();
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
    // The lock covers every ssh coop issues except `master_alive`. That used to
    // be prose, enforced by six call sites each remembering to wrap the
    // transport in `with_lock` -- a seventh that forgot would compile, pass
    // every test, and quietly reintroduce the contention the tool exists to
    // remove. Now `Transport::run` is a provided method that takes the lock
    // itself, so forgetting is not expressible.
    //
    // Proven by observation rather than inspection: a transport whose unlocked
    // primitive blocks lets us check that a second caller cannot get in.
    use coop::transport::{Output, Transport};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Blocking {
        inside: AtomicUsize,
        peak: AtomicUsize,
    }
    impl Transport for Blocking {
        fn run_unlocked(
            &self,
            _host: &coop::config::Host,
            _script: &str,
        ) -> anyhow::Result<Output> {
            let n = self.inside.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(40));
            self.inside.fetch_sub(1, Ordering::SeqCst);
            Ok(Output::ok(""))
        }
        fn master_alive(&self, _host: &coop::config::Host) -> bool {
            true
        }
    }

    let cfg = coop::config::Config::parse(&format!(
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

    let dir = coop::lock::lock_path(&host.name);
    std::fs::remove_dir_all(&dir).ok();
}
