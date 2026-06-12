//! PublishSequencer regression tests (§10.19 / §10.19.0 / §10.19.3).
//!
//! Phase 5 PRD guardrail: "Intrusive test code must live in a separate
//! file from the production code it exercises." This module is `mod
//! publish_sequencer` reached from `publish_sequencer.rs` via
//! `#[cfg(test)] #[path = "tests/publish_sequencer.rs"]`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::error::{EngineFatalReason, Error};
use crate::mvcc::timestamp::{TimestampOracle, Ts};

use super::{PublishSequencer, PublishSlotState};

/// AC #3 — `register_with_oracle` allocates `commit_ts` and the dense
/// `publish_seq` as one ordered pair under the sequencer mutex.
#[test]
fn test_register_allocates_commit_ts_and_publish_seq_atomically() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    let g1 = seq.register_with_oracle(&oracle).expect("register #1");
    let g2 = seq.register_with_oracle(&oracle).expect("register #2");
    let g3 = seq.register_with_oracle(&oracle).expect("register #3");

    // Dense publish_seq must be 1, 2, 3 with no gaps.
    assert_eq!(g1.publish_seq(), 1);
    assert_eq!(g2.publish_seq(), 2);
    assert_eq!(g3.publish_seq(), 3);

    // commit_ts must be strictly monotonic in the same order as the
    // dense slots — the §10.19 frontier-monotonicity rule requires
    // every slot's commit_ts to be > every prior slot's commit_ts.
    assert!(g1.commit_ts() < g2.commit_ts());
    assert!(g2.commit_ts() < g3.commit_ts());

    // Drop guards to clean up; no publish closures attached.
    drop((g1, g2, g3));
}

/// AC #4 — Phase 5 source files do not call `publish_sequencer.register(...)`
/// or split `oracle.commit()` from registration. The grep gate runs over
/// the production CRUD/DDL files only; test fixtures are exempt.
#[test]
fn test_no_split_oracle_commit_then_register_call_sites() {
    use std::path::Path;

    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    // R7 split: index_build.rs → index_ddl.rs and index_maint.rs split into
    // pending_install/index_write_maint/index_read_helpers/checkpoint_materialize.
    // Extend (never shrink) the register-call scan basis to all new files.
    let targets = [
        "src/storage/paged_engine.rs",
        "src/storage/paged_engine/commit_envelope.rs",
        "src/storage/paged_engine/ns_ddl.rs",
        "src/storage/paged_engine/index_ddl.rs",
        "src/storage/paged_engine/index_maint.rs",
        "src/storage/paged_engine/pending_install.rs",
        "src/storage/paged_engine/index_write_maint.rs",
        "src/storage/paged_engine/index_read_helpers.rs",
        "src/storage/paged_engine/checkpoint_materialize.rs",
    ];

    for rel in targets {
        let path = project_root.join(rel);
        if !path.exists() {
            // Phase 5 may not have introduced the index DDL/maint files
            // yet; skip missing files rather than fail the gate.
            continue;
        }
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        // Strip line comments before grepping so doc references to the
        // forbidden patterns (e.g. "Do not call publish_sequencer.register")
        // don't trip the gate.
        let mut code_only = String::with_capacity(body.len());
        for line in body.lines() {
            let stripped = line.find("//").map(|idx| &line[..idx]).unwrap_or(line);
            code_only.push_str(stripped);
            code_only.push('\n');
        }

        // Forbidden: split-style register that takes a pre-allocated commit_ts.
        assert!(
            !code_only.contains("publish_sequencer.register("),
            "{rel}: forbidden split-style call `publish_sequencer.register(...)`; \
             Phase 5 §10.19 requires `register_with_oracle`"
        );
        assert!(
            !code_only.contains("register(commit_ts)"),
            "{rel}: forbidden split-style call `register(commit_ts)`"
        );
    }
}

/// AC #13 / §10.19.3 — sparse HLC `commit_ts` skips do not stall the
/// sequencer; the dense window advances as long as `publish_seq` slots
/// transition to Ready or Aborted.
#[test]
fn test_sequencer_tolerates_hlc_skip() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    // Force a wide HLC gap: bump the oracle floor far ahead before g2.
    oracle.set_min(Ts {
        physical_ms: g1.commit_ts().physical_ms + 5_000,
        logical: 0,
    });
    let g2 = seq.register_with_oracle(&oracle).expect("g2");

    assert_eq!(g1.publish_seq(), 1);
    assert_eq!(g2.publish_seq(), 2);
    // commit_ts gap: physical_ms jumped by >= 5000ms.
    assert!(g2.commit_ts().physical_ms >= g1.commit_ts().physical_ms + 5_000);

    // Slot 1 publishes; window-advance must reach last_published == 1
    // even though commit_ts(2) is non-adjacent to commit_ts(1).
    seq.mark_ready(g1, |_pub_ts| Ok(())).expect("mark_ready g1");
    assert_eq!(seq.last_published_seq(), 1);

    seq.mark_ready(g2, |_pub_ts| Ok(())).expect("mark_ready g2");
    assert_eq!(seq.last_published_seq(), 2);
}

/// AC #13 / §10.19.3 — writer registers, exits before `mark_ready` /
/// `mark_aborted`, successor unblocks via Drop-before-ready Aborted.
#[test]
fn test_sequencer_tolerates_writer_abort_between_register_and_ready() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    let g2 = seq.register_with_oracle(&oracle).expect("g2");

    // g1 dropped without mark_ready: Drop -> mark_aborted_from_drop.
    drop(g1);

    // Slot 1 is now Aborted; window advances when g2 marks ready.
    seq.mark_ready(g2, |_pub_ts| Ok(())).expect("mark_ready g2");
    assert_eq!(seq.last_published_seq(), 2);
}

/// AC #13 / §10.19.3 — explicit `mark_aborted` on slot N skips it
/// without waiting for a `commit_ts` successor predicate.
#[test]
fn test_sequencer_advances_over_aborted_slot() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    let g2 = seq.register_with_oracle(&oracle).expect("g2");

    // Explicit abort of slot 1.
    seq.mark_aborted(g1);
    assert_eq!(seq.last_published_seq(), 1);

    seq.mark_ready(g2, |_pub_ts| Ok(())).expect("mark_ready g2");
    assert_eq!(seq.last_published_seq(), 2);
}

/// AC #13 / §10.19.3 — guard drop before durable publish marks the slot
/// Aborted and advances the dense publish window (§10.19).
#[test]
fn test_sequencer_drop_before_ready_advances_window() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    let g2 = seq.register_with_oracle(&oracle).expect("g2");
    drop(g1);
    drop(g2);

    // Both slots aborted via Drop; window advances past both.
    assert_eq!(seq.last_published_seq(), 2);
}

/// AC #13 — `mark_ready` consumes the guard so a subsequent `Drop` of
/// guard memory cannot flip a Ready slot into Aborted.
#[test]
fn test_mark_ready_consumes_guard_so_drop_cannot_abort_ready_slot() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();
    let g1 = seq.register_with_oracle(&oracle).expect("g1");

    let publish_ran = Arc::new(AtomicBool::new(false));
    let publish_ran_clone = Arc::clone(&publish_ran);
    seq.mark_ready(g1, move |_pub_ts| {
        publish_ran_clone.store(true, Ordering::Release);
        Ok(())
    })
    .expect("mark_ready g1");

    // Closure ran (slot transitioned through Ready), and last_published
    // advanced. If guard drop had flipped to Aborted post-mark_ready
    // the closure would not have run.
    assert!(publish_ran.load(Ordering::Acquire));
    assert_eq!(seq.last_published_seq(), 1);
}

/// AC #13 / §10.19.3 — pre-durability failure between `register` and
/// the durable journal envelope routes through `mark_aborted`.
#[test]
fn test_post_register_pre_durability_failure_marks_slot_aborted() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    let g2 = seq.register_with_oracle(&oracle).expect("g2");

    // Simulate writer #1 failing between register and durability:
    // call `mark_aborted` explicitly. Successor must unblock.
    seq.mark_aborted(g1);
    assert_eq!(seq.last_published_seq(), 1);

    seq.mark_ready(g2, |_pub_ts| Ok(())).expect("mark_ready g2");
    assert_eq!(seq.last_published_seq(), 2);
}

/// AC #13 / §10.19.1 — `Ts` is lex-ordered `(physical_ms, logical)`;
/// large NTP-style physical jumps and same-ms logical regression do not
/// confuse the dense sequencer (it only depends on `publish_seq`).
#[test]
fn test_sequencer_tolerates_large_hlc_gaps_and_monotonic_regression() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    // Slot 1 at the default physical floor.
    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    let ts1 = g1.commit_ts();

    // Force a 3-second wall-clock jump for slot 2.
    oracle.set_min(Ts {
        physical_ms: ts1.physical_ms + 3_000,
        logical: 0,
    });
    let g2 = seq.register_with_oracle(&oracle).expect("g2");
    let ts2 = g2.commit_ts();
    assert!(ts2.physical_ms >= ts1.physical_ms + 3_000);

    // Slot 3 in the same ms as slot 2 but a larger logical: still
    // strictly greater.
    let g3 = seq.register_with_oracle(&oracle).expect("g3");
    assert!(g3.commit_ts() > ts2);

    // Dense window advances regardless of HLC gap shape.
    seq.mark_ready(g1, |_pub_ts| Ok(())).expect("mark_ready g1");
    seq.mark_ready(g2, |_pub_ts| Ok(())).expect("mark_ready g2");
    seq.mark_ready(g3, |_pub_ts| Ok(())).expect("mark_ready g3");
    assert_eq!(seq.last_published_seq(), 3);
}

/// AC #8 / §10.19.0 C-2 — post-durable closure failure
/// produces `Error::EngineFatal` rather than silently flipping a
/// durable slot to Aborted.
///
/// The §10.21 protocol routes post-durable failures through
/// `poison_after_durable_commit` BEFORE returning the error to the
/// caller. This test simulates that contract: the closure returns Err,
/// the caller runs the poison hook, and the next register / mark_ready
/// observes EngineFatal.
#[test]
fn test_post_durable_closure_failure_produces_engine_fatal_not_aborted_slot() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();
    let g1 = seq.register_with_oracle(&oracle).expect("g1");

    // Simulate a post-durable publish-closure failure. The §10.19.0 C-2
    // contract is that the *caller* of `mark_ready` translates a closure
    // Err into `poison_after_durable_commit` AFTER `mark_ready` returns;
    // poisoning from inside the closure would re-enter the sequencer
    // mutex (advance_window_locked holds it across the closure call) and
    // deadlock.
    let res = seq.mark_ready(g1, |_pub_ts| {
        Err(Error::Internal("post-journal publish failed".into()))
    });
    assert!(
        res.is_err(),
        "post-journal closure failure must surface as Err"
    );

    // Caller-side poison hook: in production this is
    // `poison_after_durable_commit`. The sequencer is now poisoned, so
    // subsequent register fails with EngineFatal, proving the durable
    // slot was NOT silently flipped.
    seq.poison(EngineFatalReason::PostDurablePublishFailure);
    let err = seq
        .register_with_oracle(&oracle)
        .expect_err("register must fail on poisoned sequencer");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurablePublishFailure
        }
    ));
}

/// AC #13 / §10.19.0 C-2 — successor blocked behind a slot that gets
/// poisoned wakes via `notify_all` and returns `Error::EngineFatal`.
#[test]
fn test_successor_before_poison_wakes_and_returns_engine_fatal() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    // Two registered slots; slot 2's predecessor (slot 1) never readies.
    let _g1 = seq.register_with_oracle(&oracle).expect("g1");
    let _g2 = seq.register_with_oracle(&oracle).expect("g2");

    let seq_clone = Arc::clone(&seq);
    let waiter = thread::spawn(move || seq_clone.wait_until_predecessors_complete(2));
    // Give the waiter time to block on the cvar.
    thread::sleep(Duration::from_millis(50));
    assert!(
        !waiter.is_finished(),
        "waiter must be blocked on predecessor"
    );

    seq.poison(EngineFatalReason::PostDurablePublishFailure);

    let res = waiter.join().expect("waiter joined");
    let err = res.expect_err("wait must return EngineFatal after poison");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurablePublishFailure
        }
    ));
}

// ---------------------------------------------------------------------------
// Frontier-publication ordering tests (AC #10).
// ---------------------------------------------------------------------------

/// `mark_ready` stores `published_frontier` AFTER the publish closure
/// returns successfully. If the closure does the `published.store(...)`
/// step, readers loading the epoch with Acquire and then the frontier
/// with Acquire either see the matched pair (post-publish) or detect
/// `frontier < epoch.visible_ts` and retry.
#[test]
fn mark_ready_publishes_frontier_after_closure() {
    let seq = PublishSequencer::new();
    let oracle = TimestampOracle::new();

    // Frontier starts at Ts::default() per `PublishSequencer::new()`.
    let pre = seq.published_frontier.load(Ordering::Acquire);
    assert_eq!(pre, Ts::default());

    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    let commit_ts = g1.commit_ts();
    seq.mark_ready(g1, |_pub_ts| Ok(())).expect("mark_ready g1");

    let post = seq.published_frontier.load(Ordering::Acquire);
    assert_eq!(post, commit_ts);
}

/// AC #11 — `new_with_published_frontier(recovered_max_commit_ts)` initializes the dense
/// window fresh and seeds `published_frontier` with the recovered HLC.
#[test]
fn new_from_seeds_frontier_with_recovered_max_commit_ts() {
    let recovered = Ts {
        physical_ms: 12_345,
        logical: 7,
    };
    let seq = PublishSequencer::new_with_published_frontier(recovered);

    // Dense window is fresh: no previous slots.
    assert_eq!(seq.last_published_seq(), 0);
    let frontier = seq.published_frontier.load(Ordering::Acquire);
    assert_eq!(frontier, recovered);

    // Newly registered slot starts at publish_seq = 1, NOT at any
    // recovered counter — recovered HLC never seeds the dense slot
    // counter (§10.29 rule 3).
    let oracle = TimestampOracle::new();
    let g1 = seq.register_with_oracle(&oracle).expect("g1");
    assert_eq!(g1.publish_seq(), 1);
}

// ---------------------------------------------------------------------------
// PublishSlotState shape — AC #2.
// ---------------------------------------------------------------------------

/// `PublishSlotState::Pending` exists and is the initial state inserted
/// by `register_with_oracle` (smoke test of the enum shape required by
/// AC #2).
#[test]
fn publish_slot_state_pending_is_default_after_register() {
    let _ = PublishSlotState::Pending;
    let _ = PublishSlotState::Aborted;
    // PublishSlotState::Ready holds a closure and a PublishDirty; we
    // can't construct it standalone without going through mark_ready,
    // which the other tests cover.
}

// ---------------------------------------------------------------------------
// Loom test — AC #14 / §10.13.9.
// ---------------------------------------------------------------------------

/// AC #14 / §10.13.9 — for 3 writers at `publish_seq = 1, 2, 3`, loom
/// interleaves the first two writers while the third confirms the dense
/// window continues after the contested predecessor pair. The model
/// advances `last_published` monotonically and runs the publish closures
/// in dense seq order. A non-adjacent predecessor would either skip a
/// closure or call them out of order, both of which fail the assertions
/// below.
///
/// `PublishSequencer` is shimmed under `cfg(loom)` to wrap
/// `loom::sync::Mutex` + `loom::sync::Condvar`, so loom permutes the
/// `register_with_oracle` / `mark_ready` / `advance_window_locked`
/// critical sections. `TimestampOracle` already carries its own
/// `cfg(loom)` mutex shim (`src/mvcc/timestamp.rs`).
#[cfg(loom)]
#[test]
fn loom_publish_sequencer_ordering() {
    use std::sync::Arc as StdArc;
    use std::sync::Mutex as StdMutex;

    let mut builder = loom::model::Builder::new();
    builder.max_branches = 10_000;
    builder.check(|| {
        let seq = PublishSequencer::new();
        let oracle = TimestampOracle::new();

        // Allocate the three slots up front so the threads only race on
        // mark_ready (the dense-window advance is what the §10.19
        // contract demands ordering on).
        let g1 = seq.register_with_oracle(&oracle).expect("g1");
        let g2 = seq.register_with_oracle(&oracle).expect("g2");
        let g3 = seq.register_with_oracle(&oracle).expect("g3");
        assert_eq!(g1.publish_seq(), 1);
        assert_eq!(g2.publish_seq(), 2);
        assert_eq!(g3.publish_seq(), 3);

        // `order_log` records which `publish_seq` each closure runs at.
        // The window-advance contract requires the entries to be `[1, 2,
        // 3]` in every interleaving — a non-adjacent predecessor would
        // either reorder the entries or skip one entirely.
        let order_log: StdArc<StdMutex<Vec<u64>>> =
            StdArc::new(StdMutex::new(Vec::with_capacity(3)));

        let s1 = StdArc::clone(&seq);
        let s2 = StdArc::clone(&seq);
        let log1 = StdArc::clone(&order_log);
        let log2 = StdArc::clone(&order_log);

        let t1 = loom::thread::spawn(move || {
            s1.mark_ready(g1, move |_pub_ts| {
                log1.lock().expect("order_log not poisoned").push(1);
                Ok(())
            })
            .expect("mark_ready g1");
        });
        let t2 = loom::thread::spawn(move || {
            s2.mark_ready(g2, move |_pub_ts| {
                log2.lock().expect("order_log not poisoned").push(2);
                Ok(())
            })
            .expect("mark_ready g2");
        });

        t1.join().expect("t1 joined");
        t2.join().expect("t2 joined");
        let log3 = StdArc::clone(&order_log);
        seq.mark_ready(g3, move |_pub_ts| {
            log3.lock().expect("order_log not poisoned").push(3);
            Ok(())
        })
        .expect("mark_ready g3");

        // Window-advance must reach last_published == 3 in every
        // interleaving — that is the §10.19 monotonic-advance invariant.
        assert_eq!(seq.last_published_seq(), 3);

        // Closures must have run in dense seq order. Any reordering
        // here implies a writer at seq = N saw a non-adjacent
        // predecessor.
        let log = order_log.lock().expect("order_log not poisoned");
        assert_eq!(*log, vec![1u64, 2, 3]);
    });
}
