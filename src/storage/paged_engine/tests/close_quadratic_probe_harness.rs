//! #18 RESIDUAL-n² PROBE — two-scale close-time counter harness (`#[ignore]`d).
//!
//! BUG-CLOSE killed the original O(n²) close (per-rebuild-read deep-clone of the
//! resident delta map). A residual ~60× smaller quadratic term remains in
//! close-time checkpoint (fits `0.0038s × (docs/1k)²`). This harness identifies
//! WHICH site is quadratic by counting work — not wall time — at two document
//! scales and dividing.
//!
//! Methodology (the one that worked for BUG-CLOSE): counters are
//! machine-drift-immune. A LINEAR counter ~doubles when docs double; the
//! QUADRATIC culprit ~quadruples.
//!
//! The counters (`crate::storage::close_quadratic_probe`) are reset immediately
//! before `drop(client)` (the close checkpoint) and snapshotted immediately
//! after, so they capture ONLY the close-time materialize → rebuild →
//! leaf-split → chain-migration window, not the insert phase.
//!
//! RUN (prints the table; nothing is asserted that can flake on CI timing):
//! ```text
//! cargo test --profile release-test \
//!   close_quadratic_probe_harness::residual_close_quadratic_counter_growth \
//!   -- --ignored --nocapture
//! ```
//! For the secondary-index variant, the same harness with a `{seq:1}` index:
//! ```text
//! cargo test --profile release-test \
//!   close_quadratic_probe_harness::residual_close_quadratic_counter_growth_secondary \
//!   -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "ignored diagnostic harness uses assertion-style unwraps"
)]

use std::time::Instant;

use bson::doc;

use crate::client::Client;
use crate::options::{DurabilityMode, OpenOptions};
use crate::storage::close_quadratic_probe::{self, ProbeSnapshot};
use crate::IndexModel;

const BATCH_SIZE: usize = 100;
const PAYLOAD_BYTES: usize = 256;

/// Bulk-insert `total_docs` rows, then time + probe the close checkpoint.
///
/// Returns the close-window counter snapshot and the close wall-clock seconds.
fn measure_close(total_docs: usize, with_secondary_index: bool) -> (ProbeSnapshot, f64) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().join("close_quadratic_probe.mqlite");

    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::None),
    )
    .expect("open db");

    {
        client
            .database("bench_db")
            .create_collection("docs")
            .expect("create collection");

        let col = client
            .database("bench_db")
            .collection::<bson::Document>("docs");

        if with_secondary_index {
            col.create_index(IndexModel::builder().keys(doc! { "seq": 1i32 }).build())
                .expect("create secondary index");
        }

        let payload = "x".repeat(PAYLOAD_BYTES);
        let total_batches = total_docs / BATCH_SIZE;
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
        // col + inner Database handle drop here → Arc::strong_count(inner) == 1.
    }

    // Capture ONLY the close-time checkpoint window.
    close_quadratic_probe::reset_all();
    let close_start = Instant::now();
    drop(client);
    let close_secs = close_start.elapsed().as_secs_f64();
    let snap = close_quadratic_probe::snapshot();

    (snap, close_secs)
}

fn ratio(small: u64, large: u64) -> f64 {
    if small == 0 {
        f64::NAN
    } else {
        large as f64 / small as f64
    }
}

fn report(label: &str, small_docs: usize, large_docs: usize, with_secondary: bool) {
    let (s, s_secs) = measure_close(small_docs, with_secondary);
    let (l, l_secs) = measure_close(large_docs, with_secondary);

    // Doc scale doubled, so: ~2.0 = linear, ~4.0 = quadratic.
    eprintln!("\n================ {label} ================");
    eprintln!(
        "scales: {small_docs} docs (close {s_secs:.3}s) vs {large_docs} docs (close {l_secs:.3}s)  \
         wall-ratio {:.2}",
        l_secs / s_secs.max(1e-9)
    );
    eprintln!(
        "{:<28} {:>14} {:>14} {:>8}",
        "counter", format!("@{small_docs}"), format!("@{large_docs}"), "ratio"
    );
    let rows: [(&str, u64, u64); 7] = [
        ("materialize_delta_ops", s.materialize_delta_ops, l.materialize_delta_ops),
        ("descent_internal_reads", s.descent_internal_reads, l.descent_internal_reads),
        ("leaf_splits", s.leaf_splits, l.leaf_splits),
        ("leaf_cells_parsed", s.leaf_cells_parsed, l.leaf_cells_parsed),
        ("chain_drain_calls", s.chain_drain_calls, l.chain_drain_calls),
        ("chain_drain_entries", s.chain_drain_entries, l.chain_drain_entries),
        ("chain_rehome_ops", s.chain_rehome_ops, l.chain_rehome_ops),
    ];
    for (name, sv, lv) in rows {
        eprintln!("{name:<28} {sv:>14} {lv:>14} {:>8.2}", ratio(sv, lv));
    }
    eprintln!("(ratio ~2.0 ⇒ linear, ~4.0 ⇒ quadratic; scale doubled)\n");
}

#[test]
#[ignore = "diagnostic n^2 probe harness — run explicitly with --ignored --nocapture"]
fn residual_close_quadratic_counter_growth() {
    report("PRIMARY close (single namespace)", 10_000, 20_000, false);
}

#[test]
#[ignore = "diagnostic n^2 probe harness — run explicitly with --ignored --nocapture"]
fn residual_close_quadratic_counter_growth_secondary() {
    report("SECONDARY close (one {seq:1} index)", 10_000, 20_000, true);
}
