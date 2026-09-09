use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

#[test]
fn four_concurrent_callers_all_wait_bounded() {
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
    // 4 callers x 50ms of work: a fair queue tops out near 200ms + overhead.
    // The naive spin measured 6x its work; assert we are nowhere near that.
    assert!(worst < Duration::from_millis(600), "worst wait {worst:?}");
}

#[test]
fn mutual_exclusion_holds() {
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

    let started = Instant::now();
    coop::lock::with_lock(&host_b, || {}).unwrap();
    assert!(started.elapsed() < Duration::from_millis(200));

    release_tx.send(()).unwrap();
    holder.join().unwrap();
}
