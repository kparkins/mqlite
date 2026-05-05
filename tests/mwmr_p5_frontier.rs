//! Phase 5 §10.19 C-1 / US-037 — `ReadView` reads the live
//! `PublishSequencer.published_frontier` instead of a cached
//! `PublishedEpoch.sequencer_frontier` snapshot.
//!
//! The duplicated `PublishedEpoch.sequencer_frontier` field was removed in
//! US-005 / US-037. This test locks in the new contract: a `ReadView`
//! pinned at `read_ts` evaluates foreign-Pending visibility through the
//! live frontier provider, so advancing the sequencer frontier *without*
//! rebuilding the epoch flips visibility for the same view.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use mqlite::error::EngineFatalReason;
use mqlite::mvcc::{
    ChainSnapshot, TestFrontierHandle, Ts, VersionData, VersionEntry, VersionState,
};
use mqlite::{
    us020_upgrade_loser_backoff_progress, us020_writer_preference_bounds_reader_starvation, Error,
    Result, Us020PublishSequencer, Us020WriterRegistry,
};

const KEY: &[u8] = b"us-037-frontier";
const PAYLOAD: &[u8] = b"foreign-pending-payload";
const FOREIGN_TXN_ID: u64 = 4242;
const READER_TXN_ID: u64 = 9999;
const PENDING_START: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};
const FRONTIER_BEFORE: Ts = Ts {
    physical_ms: 199,
    logical: 0,
};
const FRONTIER_AFTER: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};
const READ_TS: Ts = Ts {
    physical_ms: 300,
    logical: 0,
};
const CONCURRENT_WRITERS: usize = 16;
const FRONTIER_SAMPLES: usize = 10_000;
const HLC_SKIP_MS: u64 = 5_000;
const LARGE_HLC_GAP_MS: u64 = 3_000;
const TEST_NS_ID: i64 = 20;
const ZERO_TIMEOUT_MS: u64 = 0;
const DRAIN_TIMEOUT_MS: u64 = 5_000;
const WRITER_PREFERENCE_READERS: usize = 8;
const WRITER_PREFERENCE_TIMEOUT_MS: u64 = 2_000;
const UPGRADE_RACE_ROUNDS: usize = 64;
const SHORT_SLEEP: Duration = Duration::from_millis(25);
const SETTLE_DEADLINE: Duration = Duration::from_secs(2);

fn assert_engine_fatal<T>(result: Result<T>, reason: &EngineFatalReason, label: &str) {
    match result {
        Err(Error::EngineFatal { reason: got }) => assert_eq!(&got, reason, "{label} reason"),
        Err(other) => panic!("{label} expected EngineFatal({reason:?}), got {other:?}"),
        Ok(_) => panic!("{label} expected EngineFatal({reason:?}), got Ok"),
    }
}

fn join_result<T>(handle: thread::JoinHandle<Result<T>>, label: &str) -> Result<T> {
    handle
        .join()
        .map_err(|_| Error::Internal(format!("{label} thread panicked")))?
}

fn wait_until<F>(label: &str, mut ready: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !ready() {
        assert!(Instant::now() < deadline, "{label}");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn test_commit_ts_monotonic_frontier_under_concurrent_writers() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let ready = Arc::new(Barrier::new(CONCURRENT_WRITERS));
    let done = Arc::new(AtomicBool::new(false));
    let publish_log = Arc::new(Mutex::new(Vec::with_capacity(CONCURRENT_WRITERS)));

    let sampler_seq = sequencer.clone();
    let sampler_done = Arc::clone(&done);
    let sampler = thread::spawn(move || -> Result<Vec<Ts>> {
        let mut observations = Vec::with_capacity(FRONTIER_SAMPLES);
        while observations.len() < FRONTIER_SAMPLES || !sampler_done.load(Ordering::Acquire) {
            let frontier = sampler_seq.published_frontier();
            if let Some(previous) = observations.last() {
                assert!(
                    *previous <= frontier,
                    "published frontier regressed from {previous:?} to {frontier:?}",
                );
            }
            observations.push(frontier);
            thread::yield_now();
        }
        Ok(observations)
    });

    let mut writers = Vec::with_capacity(CONCURRENT_WRITERS);
    for _ in 0..CONCURRENT_WRITERS {
        let writer_seq = sequencer.clone();
        let writer_ready = Arc::clone(&ready);
        let writer_log = Arc::clone(&publish_log);
        writers.push(thread::spawn(move || -> Result<(u64, Ts)> {
            let slot = writer_seq.register()?;
            let seq = slot.seq()?;
            let commit_ts = slot.commit_ts()?;
            writer_ready.wait();
            writer_seq.mark_ready_recording(slot, writer_log)?;
            Ok((seq, commit_ts))
        }));
    }

    let mut commits = Vec::with_capacity(CONCURRENT_WRITERS);
    for writer in writers {
        commits.push(join_result(writer, "US-020 concurrent writer")?);
    }
    done.store(true, Ordering::Release);
    let observations = join_result(sampler, "US-020 frontier sampler")?;

    assert!(
        observations.len() >= FRONTIER_SAMPLES,
        "sampler must collect at least {FRONTIER_SAMPLES} observations",
    );

    commits.sort_by_key(|(seq, _)| *seq);
    for window in commits.windows(2) {
        assert!(
            window[0].1 < window[1].1,
            "commit_ts must be monotonic in dense publish order",
        );
    }

    let log = publish_log
        .lock()
        .map_err(|_| Error::Internal("US-020 publish log poisoned".into()))?;
    let expected: Vec<u64> = (1..=CONCURRENT_WRITERS as u64).collect();
    assert_eq!(
        log.as_slice(),
        expected.as_slice(),
        "publish closures must run in dense publish_seq order",
    );
    assert_eq!(
        sequencer.published_frontier(),
        commits
            .last()
            .map(|(_, ts)| *ts)
            .expect("US-020 concurrent writer set is non-empty"),
        "final live frontier must equal the last dense commit timestamp",
    );
    Ok(())
}

#[test]
fn test_publish_waits_for_earlier_commit() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let slot1 = sequencer.register()?;
    let slot2 = sequencer.register()?;
    assert_eq!(slot1.seq()?, 1);
    assert_eq!(slot2.seq()?, 2);

    let publish_log = Arc::new(Mutex::new(Vec::with_capacity(2)));
    let successor_ready = Arc::new(AtomicBool::new(false));
    let successor_finished = Arc::new(AtomicBool::new(false));
    let worker_seq = sequencer.clone();
    let worker_log = Arc::clone(&publish_log);
    let worker_ready = Arc::clone(&successor_ready);
    let worker_finished = Arc::clone(&successor_finished);
    let successor = thread::spawn(move || -> Result<()> {
        worker_ready.store(true, Ordering::Release);
        let result = worker_seq.mark_ready_recording(slot2, worker_log);
        worker_finished.store(true, Ordering::Release);
        result
    });

    wait_until("successor did not enter mark_ready", || {
        successor_ready.load(Ordering::Acquire)
    });
    thread::sleep(SHORT_SLEEP);
    assert!(
        !successor_finished.load(Ordering::Acquire),
        "successor mark_ready must wait for the earlier dense slot",
    );
    assert_eq!(sequencer.last_published_seq(), 0);

    sequencer.mark_ready_recording(slot1, Arc::clone(&publish_log))?;
    join_result(successor, "US-020 successor publisher")?;

    let log = publish_log
        .lock()
        .map_err(|_| Error::Internal("US-020 publish log poisoned".into()))?;
    assert_eq!(log.as_slice(), &[1, 2]);
    assert_eq!(sequencer.last_published_seq(), 2);
    Ok(())
}

#[test]
fn test_post_register_pre_durability_failure_marks_slot_aborted() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let slot1 = sequencer.register()?;
    let slot2 = sequencer.register()?;

    sequencer.mark_aborted(slot1);
    assert_eq!(
        sequencer.last_published_seq(),
        1,
        "pre-durable abort must advance past the failed slot",
    );

    let publish_log = Arc::new(Mutex::new(Vec::with_capacity(1)));
    sequencer.mark_ready_recording(slot2, Arc::clone(&publish_log))?;
    assert_eq!(sequencer.last_published_seq(), 2);
    assert_eq!(
        publish_log
            .lock()
            .map_err(|_| Error::Internal("US-020 publish log poisoned".into()))?
            .as_slice(),
        &[2],
        "successor publish closure must run after the aborted predecessor",
    );
    Ok(())
}

#[test]
fn test_oracle_skip_on_hlc_advance_is_tolerated() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let slot1 = sequencer.register()?;
    let first_ts = slot1.commit_ts()?;
    sequencer.set_oracle_min(Ts {
        physical_ms: first_ts.physical_ms + HLC_SKIP_MS,
        logical: 0,
    });
    let slot2 = sequencer.register()?;
    assert!(
        slot2.commit_ts()?.physical_ms >= first_ts.physical_ms + HLC_SKIP_MS,
        "second commit_ts must skip to the raised HLC floor",
    );

    sequencer.mark_ready(slot1)?;
    sequencer.mark_ready(slot2)?;
    assert_eq!(sequencer.last_published_seq(), 2);
    Ok(())
}

#[test]
fn test_sequencer_tolerates_large_hlc_gaps_and_monotonic_regression() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let slot1 = sequencer.register()?;
    let ts1 = slot1.commit_ts()?;
    sequencer.set_oracle_min(Ts {
        physical_ms: ts1.physical_ms + LARGE_HLC_GAP_MS,
        logical: 0,
    });
    let slot2 = sequencer.register()?;
    let ts2 = slot2.commit_ts()?;
    let slot3 = sequencer.register()?;
    let ts3 = slot3.commit_ts()?;

    assert!(ts2.physical_ms >= ts1.physical_ms + LARGE_HLC_GAP_MS);
    assert!(ts3 > ts2, "logical component must preserve monotonicity");

    let publish_log = Arc::new(Mutex::new(Vec::with_capacity(3)));
    let late_seq = sequencer.clone();
    let late_log = Arc::clone(&publish_log);
    let late = thread::spawn(move || late_seq.mark_ready_recording(slot3, late_log));
    thread::sleep(SHORT_SLEEP);
    assert!(
        publish_log
            .lock()
            .map_err(|_| Error::Internal("US-020 publish log poisoned".into()))?
            .is_empty(),
        "slot 3 must not publish before slots 1 and 2",
    );

    sequencer.mark_ready_recording(slot1, Arc::clone(&publish_log))?;
    sequencer.mark_ready_recording(slot2, Arc::clone(&publish_log))?;
    join_result(late, "US-020 late HLC publisher")?;

    assert_eq!(sequencer.last_published_seq(), 3);
    assert_eq!(
        publish_log
            .lock()
            .map_err(|_| Error::Internal("US-020 publish log poisoned".into()))?
            .as_slice(),
        &[1, 2, 3],
    );
    Ok(())
}

#[test]
fn test_closure_failure_after_journal_mutex_produces_engine_fatal_not_aborted_slot() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let slot1 = sequencer.register()?;
    let err = sequencer
        .mark_ready_failing(slot1)
        .expect_err("injected post-journal publish failure must surface");
    assert!(
        matches!(err, Error::Internal(_)),
        "raw closure failure is translated by the caller-side poison path",
    );
    assert_eq!(
        sequencer.last_published_seq(),
        0,
        "post-durable closure failure must not mark the durable slot Aborted",
    );

    let reason = EngineFatalReason::PostDurablePublishFailure;
    sequencer.poison(reason.clone());
    assert_engine_fatal(sequencer.register(), &reason, "post-poison register");
    Ok(())
}

#[test]
fn test_successor_before_poison_wakes_and_returns_engine_fatal() -> Result<()> {
    let sequencer = Us020PublishSequencer::new();
    let _predecessor = sequencer.register()?;
    let successor_slot = sequencer.register()?;
    let publish_log = Arc::new(Mutex::new(Vec::new()));
    let waiting = Arc::new(AtomicBool::new(false));

    let worker_seq = sequencer.clone();
    let worker_log = Arc::clone(&publish_log);
    let worker_waiting = Arc::clone(&waiting);
    let worker = thread::spawn(move || -> Result<()> {
        let successor_seq = successor_slot.seq()?;
        worker_waiting.store(true, Ordering::Release);
        worker_seq.wait_for_predecessor_or_poison(successor_seq)?;
        worker_seq.mark_ready_recording(successor_slot, worker_log)
    });

    wait_until("successor did not enter predecessor wait", || {
        waiting.load(Ordering::Acquire)
    });
    thread::sleep(SHORT_SLEEP);

    let reason = EngineFatalReason::PostDurablePublishFailure;
    sequencer.poison(reason.clone());
    assert_engine_fatal(
        join_result(worker, "US-020 poisoned successor"),
        &reason,
        "successor wait",
    );
    assert!(
        publish_log
            .lock()
            .map_err(|_| Error::Internal("US-020 publish log poisoned".into()))?
            .is_empty(),
        "successor publish closure must not run after poison",
    );
    Ok(())
}

#[test]
fn test_writer_preference_bounds_reader_starvation() -> Result<()> {
    let reader_cycles = us020_writer_preference_bounds_reader_starvation(
        WRITER_PREFERENCE_READERS,
        WRITER_PREFERENCE_TIMEOUT_MS,
    )?;
    assert!(
        reader_cycles >= WRITER_PREFERENCE_READERS as u64,
        "reader pressure must be present before the exclusive waiter succeeds",
    );
    Ok(())
}

#[test]
fn test_close_and_drain_cannot_be_starved_by_new_admits() -> Result<()> {
    let registry = Us020WriterRegistry::new();
    let prime = registry.admit(TEST_NS_ID, DRAIN_TIMEOUT_MS)?;
    let drain_registry = registry.clone();
    let drain = thread::spawn(move || drain_registry.close_and_drain(TEST_NS_ID, DRAIN_TIMEOUT_MS));

    wait_until("close_and_drain did not close the lane", || {
        matches!(
            registry.admit(TEST_NS_ID, ZERO_TIMEOUT_MS),
            Err(Error::WriterBusy)
        )
    });

    let stop = Arc::new(AtomicBool::new(false));
    let hammer_stop = Arc::clone(&stop);
    let hammer_registry = registry.clone();
    let hammer = thread::spawn(move || -> u64 {
        let mut admits_after_close = 0_u64;
        while !hammer_stop.load(Ordering::Acquire) {
            if hammer_registry.admit(TEST_NS_ID, ZERO_TIMEOUT_MS).is_ok() {
                admits_after_close = admits_after_close.saturating_add(1);
            }
            thread::yield_now();
        }
        admits_after_close
    });

    thread::sleep(SHORT_SLEEP);
    drop(prime);
    join_result(drain, "US-020 close_and_drain")?;
    stop.store(true, Ordering::Release);
    let admits_after_close = hammer
        .join()
        .map_err(|_| Error::Internal("US-020 admit hammer panicked".into()))?;
    assert_eq!(
        admits_after_close, 0,
        "new admits must not starve an active close_and_drain",
    );
    Ok(())
}

#[test]
fn test_ddl_drain_completes_after_poisoned_successor_drops_ticket() -> Result<()> {
    let registry = Us020WriterRegistry::new();
    let sequencer = Us020PublishSequencer::new();
    let ticket = registry.admit(TEST_NS_ID, DRAIN_TIMEOUT_MS)?;
    let _predecessor = sequencer.register()?;
    let successor_slot = sequencer.register()?;
    let waiting = Arc::new(AtomicBool::new(false));

    let worker_seq = sequencer.clone();
    let worker_waiting = Arc::clone(&waiting);
    let worker = thread::spawn(move || -> Result<()> {
        let _ticket = ticket;
        let successor_seq = successor_slot.seq()?;
        worker_waiting.store(true, Ordering::Release);
        worker_seq.wait_for_predecessor_or_poison(successor_seq)
    });

    wait_until("poison successor did not enter wait", || {
        waiting.load(Ordering::Acquire)
    });
    thread::sleep(SHORT_SLEEP);

    let drain_registry = registry.clone();
    let drain = thread::spawn(move || drain_registry.close_and_drain(TEST_NS_ID, DRAIN_TIMEOUT_MS));
    thread::sleep(SHORT_SLEEP);

    let reason = EngineFatalReason::PostDurablePublishFailure;
    sequencer.poison(reason.clone());
    assert_engine_fatal(
        join_result(worker, "US-020 poisoned ticket holder"),
        &reason,
        "poisoned ticket holder",
    );
    join_result(drain, "US-020 poisoned close_and_drain")?;
    Ok(())
}

#[test]
fn test_upgrade_loser_backoff_does_not_livelock() -> Result<()> {
    let progress = us020_upgrade_loser_backoff_progress(UPGRADE_RACE_ROUNDS)?;
    assert_eq!(progress.winners, UPGRADE_RACE_ROUNDS as u64);
    assert_eq!(progress.losers, UPGRADE_RACE_ROUNDS as u64);
    Ok(())
}

#[test]
fn test_read_view_uses_live_publish_sequencer_frontier() {
    let pending = VersionEntry {
        start_ts: PENDING_START,
        stop_ts: Ts::MAX,
        txn_id: FOREIGN_TXN_ID,
        state: VersionState::Pending {
            txn_id: FOREIGN_TXN_ID,
        },
        data: VersionData::Inline(PAYLOAD.to_vec()),
        is_tombstone: false,
    };
    let mut source = BTreeMap::new();
    source.insert(KEY.to_vec(), Arc::new(VecDeque::from([pending])));
    let snapshot = ChainSnapshot::new(&source, None);
    assert_eq!(
        snapshot.chain_len(KEY),
        1,
        "ChainSnapshot must clone the foreign Pending entry verbatim",
    );

    let handle = TestFrontierHandle::new(FRONTIER_BEFORE);
    let view = handle.read_view(READ_TS, READER_TXN_ID);

    assert!(
        snapshot.visible_at(KEY, &view).is_none(),
        "foreign Pending must stay hidden while live frontier < start_ts",
    );

    // Advance the live sequencer frontier WITHOUT rebuilding any
    // PublishedEpoch — this is the §10.19 C-1 contract that US-037 locks
    // in. The same `view` must now see the entry because the visibility
    // predicate loads the live frontier on every check.
    handle.advance(FRONTIER_AFTER);

    let visible = snapshot
        .visible_at(KEY, &view)
        .expect("live frontier reached start_ts; foreign Pending is now visible");
    match visible.state {
        VersionState::Pending { txn_id } => assert_eq!(txn_id, FOREIGN_TXN_ID),
        VersionState::Committed | VersionState::Aborted => {
            panic!("foreign entry must remain Pending after frontier advance");
        }
    }
    match &visible.data {
        VersionData::Inline(payload) => assert_eq!(payload.as_slice(), PAYLOAD),
        VersionData::Overflow(_) => panic!("US-037 fixture keeps payload inline"),
    }
}
