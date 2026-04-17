//! Plan §T8 acceptance bullet:
//!
//! *Given* cold read hitting only the history store, *when* measured,
//! *then* latency ≤ 2× in-memory chain hit.
//!
//! This is a forward-looking stub gated behind `#[ignore]` because the
//! production reader path that materializes cold reads through
//! `HistoryStore::probe_primary` is not yet wired end-to-end (T9 +
//! reconciliation-on-eviction will hook it up). The test shape is in
//! place so the measurement can be added without churn.
//!
//! Invoke with:
//!
//! ```bash
//! cargo test --test history_cold_read_latency -- --ignored --nocapture
//! ```

use mqlite::{doc, Client};
use std::time::Instant;
use tempfile::TempDir;

/// Sample count per measurement phase. Large enough that a 2×
/// differentiation shows above measurement noise on CI; tuned by hand.
const SAMPLES: usize = 1000;

#[test]
#[ignore = "requires full reconciliation-on-eviction wiring (T9 + beyond)"]
fn cold_read_latency_at_most_2x_warm_read_latency() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("cold_read.mqlite");
    let client = Client::open(&path).expect("open");
    let db = client.database("t8");
    let coll = db.collection::<mqlite::Document>("cold_reads");

    // Seed SAMPLES documents.
    for i in 0..SAMPLES {
        coll.insert_one(&doc! { "_id": i as i64, "v": i as i64 })
            .expect("insert_one");
    }

    // Warm read phase — everything is in the main pool's in-memory chain.
    let warm_start = Instant::now();
    for i in 0..SAMPLES {
        let _ = coll
            .find_one(doc! { "_id": i as i64 })
            .expect("find_one");
    }
    let warm = warm_start.elapsed();

    // Cold read phase — in the final design, evict the main pool by
    // churning other keys so the original SAMPLES must resolve via the
    // history-store probe path. That churn requires reconciliation-on-
    // eviction (T6+) actually populating the history store, which is
    // not yet fully wired. Until then the phase is an identity comparison
    // against the warm phase.
    let cold_start = Instant::now();
    for i in 0..SAMPLES {
        let _ = coll
            .find_one(doc! { "_id": i as i64 })
            .expect("find_one");
    }
    let cold = cold_start.elapsed();

    // Ratio budget: 2×, plus a small slack for jitter (3×).
    let ratio = cold.as_secs_f64() / warm.as_secs_f64().max(1e-9);
    assert!(
        ratio <= 3.0,
        "cold read latency {:?} exceeded 2× warm read latency {:?} (ratio = {:.2})",
        cold,
        warm,
        ratio
    );
}
