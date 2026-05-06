#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Smoke test for the US-002 crash-cut harness.
//!
//! Exercises each helper in `crash_harness` at least once against a known
//! workload, asserting that return values match the known sequence.
//!
//! Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
//! mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
//! (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).

#[path = "crash_harness.rs"]
mod crash_harness;

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, OpenOptions};

// ---------------------------------------------------------------------------
// Smoke test
// ---------------------------------------------------------------------------

/// Verifies each helper in `crash_harness` against a known workload:
///
/// 1. `truncate_journal_to_offset` at the current journal EOF is a no-op;
///    `reopen_inspect` still recovers all frames.
/// 2. `reopen_inspect` returns `legacy_page_frame_count == 0` and
///    `chain_commit_frame_count == N` (one ChainCommit per committed insert).
/// 3. `truncate_journal_before_frame_kind(ChainCommit)` removes the first
///    ChainCommit frame; after reopen `chain_commit_frame_count < N` and
///    `recovered_max_commit_ts` is `<=` the original max (or `None`).
///
/// ## Journal lifetime
///
/// `snapshot_ops::checkpoint` (called by `Client::drop`) flushes the journal
/// but does NOT delete it — the journal accumulates frames across opens until
/// an `emergency_checkpoint` truncates it.  This test writes N inserts in a
/// single open, uses `std::mem::forget` to skip the flush-on-drop, then
/// reopens with `reopen_inspect`.  We assert on the exact ChainCommit count
/// by counting how many frames the recovery loop sees.
#[test]
fn smoke_harness_truncate_and_reopen() {
    const N: usize = 3;

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("smoke.mqlite");

    // -----------------------------------------------------------------------
    // Phase 1: write N inserts in a single open, then mem::forget so the
    // journal is NOT checkpointed/flushed and its frames survive on disk.
    // -----------------------------------------------------------------------
    let expected_chain_count;
    {
        let client = Client::open_with_options(
            &db_path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .expect("open phase1");
        let col = client.database("smokedb").collection::<Document>("col");
        for i in 0..N {
            col.insert_one(&doc! { "_id": i as i32, "v": i as i32 })
                .expect("insert");
        }
        // Leak the client so drop() does not run checkpoint, leaving the
        // journal on disk with all N commit frames intact.
        //
        // Safety: this intentionally leaks the Arc<ClientInner>; the tempdir
        // is removed at test end, freeing all OS-level resources.
        std::mem::forget(client);
    }

    // Verify the journal file exists.
    let jpath = crash_harness::journal_path(&db_path);
    assert!(
        jpath.exists(),
        "journal must exist after un-checkpointed writes"
    );

    // Record journal size before the no-op truncation.
    let journal_size = std::fs::metadata(&jpath).expect("journal metadata").len();

    // -----------------------------------------------------------------------
    // Phase 2: no-op truncation (at journal EOF) + reopen_inspect
    // -----------------------------------------------------------------------
    crash_harness::truncate_journal_to_offset(&db_path, journal_size)
        .expect("no-op truncate at EOF");

    let (_client1, report1) = crash_harness::reopen_inspect(&db_path).expect("reopen_inspect 1");

    assert_eq!(
        report1.legacy_page_frame_count, 0,
        "Phase 6 ordinary CRUD recovery must not process retired page frames"
    );

    // The recovery loop sees exactly as many ChainCommit frames as were
    // written.  Since we started with a fresh DB and did N inserts in one
    // session, exactly N ChainCommit frames are in the journal.
    expected_chain_count = report1.chain_commit_frame_count;
    assert_eq!(
        expected_chain_count, N as u64,
        "expected chain_commit_frame_count == {N} (one per insert), got {}",
        expected_chain_count
    );

    let original_max_ts = report1.recovered_max_commit_ts;
    assert!(
        original_max_ts.is_some(),
        "recovered_max_commit_ts must be Some after {N} committed inserts"
    );

    // -----------------------------------------------------------------------
    // Phase 3: truncate before the first ChainCommit frame.
    //
    // reopen_inspect from Phase 2 already reset counters and opened the DB;
    // the journal was replayed, which means the main file is now up-to-date.
    // We need to drop _client1 (which will try to checkpoint, but the journal
    // may already be truncated/replayed), then set up a fresh journal again
    // for the truncation test.
    // -----------------------------------------------------------------------
    drop(_client1);

    // Open again and write N more inserts (different collection to avoid _id
    // collisions), then forget so the journal survives.
    {
        let client = Client::open_with_options(
            &db_path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .expect("open phase3");
        let col = client.database("smokedb").collection::<Document>("col2");
        for i in 0..N {
            col.insert_one(&doc! { "_id": i as i32, "w": i as i32 })
                .expect("insert phase3");
        }
        std::mem::forget(client);
    }

    assert!(
        jpath.exists(),
        "journal must exist before phase3 truncation test"
    );

    // Truncate before the first ChainCommit frame.
    let cut_offset = crash_harness::truncate_journal_before_frame_kind(
        &db_path,
        crash_harness::FrameKind::ChainCommit,
    )
    .expect("truncate_journal_before_frame_kind");

    assert!(
        cut_offset.is_some(),
        "expected at least one ChainCommit frame in the journal before truncation"
    );

    // Reopen and check that chain_commit_frame_count decreased.
    let (_client2, report2) = crash_harness::reopen_inspect(&db_path).expect("reopen_inspect 2");

    assert!(
        report2.chain_commit_frame_count < N as u64,
        "expected chain_commit_frame_count < {N} after truncating the first \
         ChainCommit frame, got {}",
        report2.chain_commit_frame_count
    );

    // If any ChainCommit frames survived, the recovered HLC floor must be <=
    // the original max (truncation can only lower it, never raise it).
    if let Some(new_ts) = report2.recovered_max_commit_ts {
        let orig = original_max_ts.expect("original_max_ts is Some");
        assert!(
            new_ts <= orig,
            "recovered_max_commit_ts {new_ts:?} after truncation must be <= \
             original {orig:?}"
        );
    }
    // None means all ChainCommit frames were truncated, which is strictly
    // lower than Some(orig) — the predicate holds.
}
