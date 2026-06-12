//! Regression test: close-time checkpoint cost must be bounded (roughly linear
//! in document count), not quadratic — secondary-index variant.
//!
//! # Bug description
//!
//! Dropping the last `Client` handle triggers a checkpoint via
//! `src/client/handle.rs:148-158` → `ClientInner::checkpoint` →
//! `snapshot_ops::checkpoint`. In addition to the primary tree
//! (`materialize_primary_deltas_for_checkpoint`), checkpoint also folds every
//! ready secondary index via
//! `materialize_ready_secondary_deltas_for_checkpoint`
//! (`src/storage/paged_engine/index_maint.rs:937-954`). That secondary path has
//! the *identical* quadratic shape as the primary path: each structural leaf
//! read deep-clones the leaf frame's entire resident delta map and then
//! discards it, so the work done during close is proportional to the *square*
//! of the documents inserted since open.
//!
//! This test is the secondary-index twin of
//! `tests/close_checkpoint_bounded.rs`: same document count, document shape,
//! payload size, batch size, and assert bound. The only difference is that a
//! single secondary index is created on the payload-bearing collection before
//! the bulk insert so the quadratic secondary materialize path is exercised at
//! close.
//!
//! # Measured quadratic scaling (release build, single namespace, 256B payload)
//!
//! | docs  | close wall-clock |
//! |-------|-----------------|
//! |  4 000 | ~4 s            |
//! |  8 000 | ~16 s           |
//! | 20 000 | ~102 s          |
//! | 40 000 | ~422 s          |
//!
//! Fit: t ≈ 4.4 s × (docs / 4 000)²
//!
//! Durability mode is irrelevant — `DurabilityMode::None` and
//! `DurabilityMode::Interval(50ms)` produce identical close times.
//!
//! # Calibration rationale for the 20 s assert bound
//!
//! * Expected post-fix behavior (linear close): ~2–4 s for 20 000 docs.
//! * Current broken behavior (quadratic): ~102 s for 20 000 docs.
//! * The 20 s bound is deliberately placed at ~5× over expected post-fix
//!   performance (generous slack for slow CI machines) and ~5× under the
//!   current broken value (robust failure signal).
//! * The test will pass once the quadratic materialization bug is fixed and
//!   fail on every tree where the bug is present.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use std::time::{Duration, Instant};

use bson::doc;
use mqlite::{Client, DurabilityMode, IndexModel, OpenOptions};

/// Total documents to insert.  Must be large enough to expose quadratic
/// scaling (~102 s under the bug) while keeping the insert phase fast
/// (~1-2 s single-threaded with batches of 100).
const TOTAL_DOCS: usize = 20_000;

/// Batch size mirrors the canonical perf_matrix write-axis shape.
const BATCH_SIZE: usize = 100;

/// Payload size mirrors `PAYLOAD_BYTES` in `benches/perf/perf_matrix.rs`.
const PAYLOAD_BYTES: usize = 256;

/// Close-time wall-clock limit.
///
/// * ~5× above expected post-fix linear close (~2–4 s)
/// * ~5× below current quadratic close (~102 s)
///
/// Any machine that cannot close 20 000 docs in 20 s after the bug is
/// fixed would already be failing the existing perf-baseline contract
/// tests, so this bound is appropriate even for slow CI.
const CLOSE_DEADLINE: Duration = Duration::from_secs(20);

#[test]
fn close_checkpoint_is_bounded_after_bulk_insert_with_secondary_index() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("close_checkpoint_bounded_secondary.mqlite");

    // DurabilityMode::None — per measurements, durability mode is irrelevant
    // to the close-time bug.  Using None removes fsync noise so the only
    // variable is the checkpoint materialization cost itself.
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::None),
    )
    .expect("open db");

    let payload = "x".repeat(PAYLOAD_BYTES);

    // All sub-handles (Database, Collection) are dropped inside this block
    // before we time close.  Each of Database and Collection holds its own
    // Arc<ClientInner> clone, so they must not be alive when drop(client)
    // runs — otherwise Arc::strong_count > 1 and the checkpoint is skipped.
    let insert_elapsed = {
        // Pre-create the namespace so the first batch does not pay bootstrap
        // DDL overhead inside the timed insert window.
        client
            .database("bench_db")
            .create_collection("docs")
            .expect("create collection");

        let col = client
            .database("bench_db")
            .collection::<bson::Document>("docs");

        // Create ONE secondary index on the payload-bearing collection before
        // the bulk insert. This is the only difference from
        // `close_checkpoint_bounded.rs`: it forces every committed insert to
        // also produce a resident secondary delta, so the quadratic secondary
        // materialize path (`materialize_ready_secondary_deltas_for_checkpoint`)
        // is exercised at close.
        col.create_index(IndexModel::builder().keys(doc! { "seq": 1i32 }).build())
            .expect("create secondary index");

        // --- Insert phase (not timed for the assertion, but printed for diagnosis) ---
        let insert_start = Instant::now();

        let total_batches = TOTAL_DOCS / BATCH_SIZE;
        for batch_idx in 0..total_batches {
            let base = (batch_idx * BATCH_SIZE) as i64;
            let docs: Vec<bson::Document> = (0..BATCH_SIZE as i64)
                .map(|i| {
                    doc! {
                        "_id":     base + i,
                        "writer":  0i32,
                        "seq":     base + i,
                        "payload": &payload,
                    }
                })
                .collect();
            col.insert_many(&docs)
                .run()
                .unwrap_or_else(|err| panic!("insert_many batch {batch_idx} failed: {err}"));
        }

        // col (and the inner Database handle) are dropped here, returning the
        // Arc<ClientInner> refcount to 1 (held only by `client`).
        insert_start.elapsed()
    };

    eprintln!(
        "[close_checkpoint_bounded_secondary] insert phase: {TOTAL_DOCS} docs in {insert_elapsed:.2?} \
         ({:.0} docs/s)",
        TOTAL_DOCS as f64 / insert_elapsed.as_secs_f64()
    );

    // --- Close phase (this is the measurement) ---
    //
    // At this point Arc::strong_count(&client.inner) == 1.
    // drop(client) triggers Client::drop → ClientInner::checkpoint().
    // On the buggy tree this takes ~102 s; post-fix it should take ~2–4 s.
    let close_start = Instant::now();
    drop(client);
    let close_elapsed = close_start.elapsed();

    eprintln!(
        "[close_checkpoint_bounded_secondary] close (checkpoint) elapsed: {close_elapsed:.2?}  \
         (limit: {CLOSE_DEADLINE:.2?})"
    );

    assert!(
        close_elapsed < CLOSE_DEADLINE,
        "close-time checkpoint after {TOTAL_DOCS} docs (with one secondary index) took \
         {close_elapsed:.2?}, which exceeds the {CLOSE_DEADLINE:.2?} bound.\n\
         \n\
         This indicates the QUADRATIC checkpoint-materialization bug is present in \
         `materialize_ready_secondary_deltas_for_checkpoint`: the online CRUD path defers \
         all leaf splits to checkpoint time and every structural leaf read deep-clones \
         the resident delta map only to discard it, making close cost O(n²) in document \
         count.\n\
         \n\
         Measured scaling: 4k docs→~4s, 8k→~16s, 20k→~102s, 40k→~422s.\n\
         Post-fix linear close should take ~2–4s for {TOTAL_DOCS} docs.\n\
         Insert phase took: {insert_elapsed:.2?}",
    );
}
