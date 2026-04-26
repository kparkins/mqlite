#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Contract 3.4 — durable timestamp-floor recovery.
//!
//! After reopen, the engine must not issue a `commit_ts` <= any durable
//! pre-reopen commit's `commit_ts`. Corresponds to
//! docs/STORAGE-CONTRACTS-FROZEN.md Contract 3.4.
//!
//! # Observable source
//!
//! We sample `Client::__oracle_now()` — a `#[doc(hidden)]` test-only accessor
//! that returns `(physical_ms, logical)` from the timestamp oracle — after each
//! committed insert.  Because the oracle is monotone and every commit advances
//! it, the value sampled immediately after the Nth insert is >= that commit's
//! `commit_ts`.  We track the maximum across all N inserts as `max_pre_ts`.
//!
//! Critically, we use `std::mem::forget` to abandon the client without running
//! Drop (which would call checkpoint and flush the journal to main storage,
//! destroying the ChainCommit frames). This leaves ChainCommit frames durable
//! in the journal, exactly as they would be after a crash.
//!
//! We then use `scan_chain_commits` (from crash_harness.rs) to verify the
//! journal contains N ChainCommit frames before reopening. This proves the
//! test is not vacuous — recovery will actually fold them through the HLC-floor
//! code path.
//!
//! After reopen we perform one additional insert and sample the oracle again.
//! The post-reopen oracle value must be strictly greater than `max_pre_ts`,
//! which proves that the engine floored itself above every durable pre-reopen
//! `commit_ts` as required by Contract 3.4.

// Pull in crash_harness helpers (scan_chain_commits, reopen_inspect).
#[path = "crash_harness.rs"]
mod crash_harness;

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, OpenOptions};

#[test]
fn recovery_restores_commit_ts_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("floor_test.mqlite");

    const N_INSERTS: i32 = 7;

    // --- Step 1-2: perform N committed writes and capture the running max
    // oracle value after each commit.
    //
    // We use FullSync so every insert is durable before insert_one returns,
    // ensuring the journal has a ChainCommit frame for each write.
    //
    // IMPORTANT: we do NOT call client.close() — that would checkpoint the
    // journal (flush to main storage and reset), leaving no ChainCommit frames
    // for recovery to process. Instead we use std::mem::forget to abandon the
    // client without running Drop, preserving all ChainCommit frames in the
    // journal exactly as a crash would.
    let max_pre_ts: (u64, u32) = {
        let client = Client::open_with_options(
            &path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .unwrap();

        let col = client.database("tsfloor").collection::<Document>("items");

        let mut running_max: (u64, u32) = (0, 0);
        for i in 0..N_INSERTS {
            col.insert_one(&doc! { "_id": i, "v": i }).unwrap();
            // Sample oracle immediately after each committed write. The oracle
            // is >= commit_ts because commit() advances it before returning.
            let sampled = client.__oracle_now();
            if sampled > running_max {
                running_max = sampled;
            }
        }

        // Abandon the client WITHOUT running Drop. Drop would call checkpoint,
        // which flushes the journal to main storage and resets the journal —
        // removing the ChainCommit frames that recovery needs to process.
        // mem::forget preserves those frames, simulating a crash.
        std::mem::forget(client);
        running_max
    };

    // Sanity: we must have seen at least one non-zero timestamp.
    assert!(
        max_pre_ts.0 > 0 || max_pre_ts.1 > 0,
        "pre-reopen oracle never advanced above zero — oracle is broken"
    );

    // --- Verify journal is not empty: scan ChainCommit frames before reopen.
    //
    // This confirms the test is non-vacuous: recovery will actually process
    // these frames through the HLC-floor code path.
    let chain_commits = crash_harness::scan_chain_commits(&path)
        .expect("scan_chain_commits failed — journal may be missing or corrupt");

    assert!(
        !chain_commits.is_empty(),
        "journal contains no ChainCommit frames before reopen — \
         the test is vacuous (recovery has nothing to fold)"
    );

    let journal_frame_count = chain_commits.len();

    // The max commit_ts from the journal scan should be consistent with our
    // oracle samples (both are >= the actual commit timestamps).
    let max_journal_ts = chain_commits
        .iter()
        .map(|&(_, ts)| ts)
        .max()
        .expect("chain_commits is non-empty");

    // --- Step 3: reopen (via reopen_inspect to also capture recovery metrics).
    let (client, report) = crash_harness::reopen_inspect(&path).expect("reopen_inspect failed");

    // Confirm recovery actually processed ChainCommit frames (not vacuous).
    assert!(
        report.chain_commit_frame_count > 0,
        "recovery processed zero ChainCommit frames — HLC-floor code path was not exercised \
         (journal had {} frames before reopen but recovery saw none)",
        journal_frame_count
    );

    assert_eq!(
        report.chain_commit_frame_count as usize, journal_frame_count,
        "recovery processed {} ChainCommit frames but journal had {} — mismatch",
        report.chain_commit_frame_count, journal_frame_count
    );

    // The recovered_max_commit_ts must equal the max we read from the journal.
    assert_eq!(
        report.recovered_max_commit_ts,
        Some(max_journal_ts),
        "recovered_max_commit_ts {:?} does not match journal scan max {:?}",
        report.recovered_max_commit_ts,
        Some(max_journal_ts)
    );

    // --- Step 4: perform one new commit.
    // --- Step 5: assert new oracle value > max_pre_ts.
    //
    // Contract 3.4 (src/storage/paged_engine/state.rs, src/journal/recovery.rs):
    // on reopen the engine reads `recovered_max_commit_ts` from the journal scan
    // and floors the TimestampOracle via `oracle.set_min(max_ts.successor())`.
    // The oracle must therefore be strictly above every pre-reopen ChainCommit
    // commit_ts before the first post-reopen commit executes.
    let col = client.database("tsfloor").collection::<Document>("items");
    col.insert_one(&doc! { "_id": 99i32, "v": "post-reopen" })
        .unwrap();
    let post_ts = client.__oracle_now();

    assert!(
        post_ts > max_pre_ts,
        "Contract 3.4 violated: post-reopen oracle ({post_ts:?}) must be \
         strictly greater than pre-shutdown max oracle value ({max_pre_ts:?}). \
         The HLC floor was not restored correctly after reopen. \
         Journal had {journal_frame_count} ChainCommit frames; \
         recovery processed {}.",
        report.chain_commit_frame_count
    );
}
