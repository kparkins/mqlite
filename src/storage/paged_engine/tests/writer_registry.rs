use super::*;
use std::thread;

const NS_ID: i64 = 42;
const ADMIT_TIMEOUT: Duration = Duration::from_secs(5);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const ZERO_TIMEOUT: Duration = Duration::from_millis(0);

fn lane_counters(reg: &NsWriterRegistry, ns_id: i64) -> (u64, u64, bool) {
    let lane = reg
        .lanes
        .get(&ns_id)
        .map(|e| e.value().clone())
        .expect("lane present");
    let g = lane.inner.lock();
    (g.admits, g.releases, g.closed)
}

#[test]
fn test_admit_and_release_counters_balance() {
    let reg = Arc::new(NsWriterRegistry::new());
    let mut t1 = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit 1");
    let (admits, releases, closed) = lane_counters(&reg, NS_ID);
    assert_eq!(admits, 1, "admits bumped per admit");
    assert_eq!(releases, 0, "no releases yet");
    assert!(!closed);
    t1.finish_body();
    let mut t2 = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit 2");
    t2.finish_body();
    let mut t3 = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit 3");
    t3.finish_body();
    drop(t1);
    let (admits, releases, _) = lane_counters(&reg, NS_ID);
    assert_eq!(admits, 3);
    assert_eq!(releases, 1, "one release after first drop");
    drop(t2);
    drop(t3);
    let (admits, releases, _) = lane_counters(&reg, NS_ID);
    assert_eq!(admits, releases, "all tickets released");
    assert_eq!(admits, 3);
}

#[test]
fn test_close_and_drain_blocks_new_admits() {
    let reg = Arc::new(NsWriterRegistry::new());
    // Prime the lane.
    let t = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("prime admit");
    // Drain in a worker — it must block until the prime ticket drops.
    let reg_drain = Arc::clone(&reg);
    let drain_handle = thread::spawn(move || reg_drain.close_and_drain(NS_ID, DRAIN_TIMEOUT));
    // Give the drain thread time to set closed=true and start waiting.
    thread::sleep(Duration::from_millis(50));
    // While drain is waiting, admit must fail with WriterBusy at zero timeout
    // because closed=true.
    let busy = reg.admit(NS_ID, ZERO_TIMEOUT);
    assert!(matches!(busy, Err(Error::WriterBusy)));
    // Release the priming writer; drain should now finish.
    drop(t);
    drain_handle
        .join()
        .expect("drain thread joined")
        .expect("drain succeeds after release");
    // Lane stays closed until reopen is called by the guard.
    let (_, _, closed) = lane_counters(&reg, NS_ID);
    assert!(closed, "drain leaves lane closed");
}

#[test]
fn test_close_cannot_be_starved_by_new_admits() {
    // §10.13.6 — close_and_drain sets closed=true BEFORE waiting, so
    // new admits queued after close cannot delay drain progress.
    let reg = Arc::new(NsWriterRegistry::new());
    let prime = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("prime admit");

    // Start the drain first; it will acquire the lane mutex, flip
    // closed=true, then wait for prime to drop. Hammer is spawned
    // only after closed=true is observable so the test can assert
    // that NO admit succeeds after the gate is set.
    let reg_drain = Arc::clone(&reg);
    let drain_handle = thread::spawn(move || reg_drain.close_and_drain(NS_ID, DRAIN_TIMEOUT));

    // Spin until the drain has flipped closed=true. Bounded by the
    // drain timeout itself so a buggy implementation cannot deadlock
    // the test.
    let close_observed_deadline = Instant::now() + DRAIN_TIMEOUT;
    loop {
        let (_, _, closed) = lane_counters(&reg, NS_ID);
        if closed {
            break;
        }
        assert!(
            Instant::now() < close_observed_deadline,
            "drain failed to set closed=true within DRAIN_TIMEOUT"
        );
        thread::sleep(Duration::from_millis(1));
    }

    // Now hammer admit() at zero timeout — every attempt must fail
    // because the gate is already closed.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_hammer = Arc::clone(&stop);
    let reg_hammer = Arc::clone(&reg);
    let hammer = thread::spawn(move || {
        let mut admits_after_close: u64 = 0;
        while !stop_hammer.load(std::sync::atomic::Ordering::Acquire) {
            if reg_hammer.admit(NS_ID, ZERO_TIMEOUT).is_ok() {
                admits_after_close = admits_after_close.saturating_add(1);
            }
        }
        admits_after_close
    });

    // Let hammer pound the closed gate for a measurable window.
    thread::sleep(Duration::from_millis(75));

    // Drop the prime ticket so drain can finish.
    drop(prime);
    let drain_res = drain_handle.join().expect("drain thread joined");
    assert!(
        drain_res.is_ok(),
        "drain must complete within timeout despite hammer"
    );

    stop.store(true, std::sync::atomic::Ordering::Release);
    let admits_after_close = hammer.join().expect("hammer joined");
    assert_eq!(
        admits_after_close, 0,
        "no new admits succeeded once close set the gate"
    );
}

#[test]
fn test_barrier_guard_drop_without_commit_reopens() {
    let reg = Arc::new(NsWriterRegistry::new());
    // Seed a lane so close_and_drain_guard has something to act on.
    drop(reg.admit(NS_ID, ADMIT_TIMEOUT).expect("seed admit"));
    {
        let _guard = reg
            .close_and_drain_guard(NS_ID, DRAIN_TIMEOUT)
            .expect("close_and_drain_guard");
        let (_, _, closed) = lane_counters(&reg, NS_ID);
        assert!(closed, "guard scope keeps lane closed");
    }
    // After guard Drop without commit/mark_dropped → lane reopened.
    let (_, _, closed) = lane_counters(&reg, NS_ID);
    assert!(!closed, "Drop in Closed state reopens the lane");
    // And new admits succeed again.
    let _t = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit after reopen");
}

#[test]
fn test_barrier_guard_commit_reopens_immediately() {
    let reg = Arc::new(NsWriterRegistry::new());
    drop(reg.admit(NS_ID, ADMIT_TIMEOUT).expect("seed admit"));
    let guard = reg
        .close_and_drain_guard(NS_ID, DRAIN_TIMEOUT)
        .expect("close_and_drain_guard");
    guard.commit();
    // After commit, the lane is open and admits succeed.
    let (_, _, closed) = lane_counters(&reg, NS_ID);
    assert!(!closed, "commit reopens immediately");
    let _t = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit after commit");
}

#[test]
fn test_close_and_drain_guard_timeout_reopens() {
    // Regression: a drain timeout must not leave the lane closed.
    // `close_and_drain_guard` constructs the guard BEFORE the drain
    // wait, so a `WriterBusy` propagation drops the guard, which
    // reopens the lane in `Closed` state.
    let reg = Arc::new(NsWriterRegistry::new());
    let prime = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("prime admit");
    // ZERO_TIMEOUT forces the drain to fail immediately because
    // prime is still in flight.
    match reg.close_and_drain_guard(NS_ID, ZERO_TIMEOUT) {
        Ok(_) => panic!("drain must time out while prime is still admitted"),
        Err(Error::WriterBusy) => {}
        Err(other) => panic!("expected WriterBusy, got: {other:?}"),
    }
    // Guard is dropped on the error path; reopen must have cleared
    // `closed` so subsequent admits succeed.
    let (_, _, closed) = lane_counters(&reg, NS_ID);
    assert!(!closed, "drain timeout must reopen the lane via guard Drop");
    // Drop prime and confirm a fresh admit + drain cycle works.
    drop(prime);
    let _t = reg
        .admit(NS_ID, ADMIT_TIMEOUT)
        .expect("admit after drain-timeout reopen");
}

#[test]
fn test_barrier_guard_mark_dropped_removes_lane() {
    let reg = Arc::new(NsWriterRegistry::new());
    drop(reg.admit(NS_ID, ADMIT_TIMEOUT).expect("seed admit"));
    {
        let mut guard = reg
            .close_and_drain_guard(NS_ID, DRAIN_TIMEOUT)
            .expect("close_and_drain_guard");
        guard.mark_dropped();
    }
    // Drop in MarkedDropped state must remove the lane entry.
    assert!(
        reg.lanes.get(&NS_ID).is_none(),
        "MarkedDropped guard Drop removes the lane"
    );
}
