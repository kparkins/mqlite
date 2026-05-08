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

//! UltraQA Surface 4 — crash injection at 10 random points across the write
//! path.
//!
//! After seeding a multi-namespace workload (inserts, secondary index build,
//! updates, deletes), we `std::mem::forget` the client so the journal stays
//! intact. A deterministic LCG (seed `0x51D0BA5E`) chooses 10 random byte
//! offsets in `[journal_header .. journal_size)`. For each offset we:
//!   1. Clone the seeded db + journal into a fresh tempdir.
//!   2. Truncate the journal to the offset.
//!   3. Reopen — must succeed; recovery discards any partial trailing frames.
//!   4. Read both namespaces to confirm the recovered database is usable.
//!   5. For the untruncated full-journal case, assert committed data remains
//!      visible.

#[path = "crash_harness.rs"]
mod crash_harness;

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, IndexModel, OpenOptions};
use std::path::{Path, PathBuf};

const RNG_SEED: u64 = 0x51D0BA5E;
const TRUNCATION_POINTS: usize = 10;
const SEED_DOCS_PER_NS: i32 = 20;
use crash_harness::JOURNAL_HEADER_SIZE;

/// Minimal deterministic LCG (Numerical Recipes constants). Fine for test
/// scheduling; not for anything cryptographic.
struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn next_in_range(&mut self, lo: u64, hi_exclusive: u64) -> u64 {
        assert!(hi_exclusive > lo);
        lo + (self.next_u64() % (hi_exclusive - lo))
    }
}

/// Seed a multi-surface workload into `path` and leak the client so the
/// journal survives on disk.
fn seed_workload(path: &Path) {
    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
    let client = Client::open_with_options(path, opts).expect("open for seed");

    let col_a = client.database("ns_a").collection::<Document>("docs");
    let col_b = client.database("ns_b").collection::<Document>("docs");

    for i in 0..SEED_DOCS_PER_NS {
        col_a
            .insert_one(&doc! { "_id": i, "tag": i % 4, "payload": "a" })
            .unwrap();
        col_b
            .insert_one(&doc! { "_id": i, "tag": i % 4, "payload": "b" })
            .unwrap();
    }

    col_a
        .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
        .expect("secondary index build");

    for i in 0..5 {
        col_a
            .update_one(doc! { "_id": i }, doc! { "$set": { "payload": "a2" } })
            .run()
            .unwrap();
        col_b.delete_one(doc! { "_id": i }).unwrap();
    }

    // Drop would checkpoint + wipe the journal — leak instead so the journal
    // keeps every frame we just produced on disk.
    std::mem::forget(client);
}

/// Copy the seeded db + journal into a fresh tempdir; returns the cloned db
/// path (and keeps the tempdir alive via its handle).
fn clone_seeded(template_db: &Path) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest_db = dir.path().join("iter.mqlite");
    std::fs::copy(template_db, &dest_db).expect("copy db");
    let src_j = crash_harness::journal_path(template_db);
    let dest_j = crash_harness::journal_path(&dest_db);
    if src_j.exists() {
        std::fs::copy(&src_j, &dest_j).expect("copy journal");
    }
    (dir, dest_db)
}

#[test]
fn crash_injection_ten_random_truncation_points() {
    // --- Seed once ---------------------------------------------------------
    let seed_dir = tempfile::tempdir().unwrap();
    let template_db = seed_dir.path().join("seed.mqlite");
    seed_workload(&template_db);

    let journal_path = crash_harness::journal_path(&template_db);
    let journal_size = std::fs::metadata(&journal_path)
        .expect("seeded journal must exist")
        .len();
    assert!(
        journal_size > JOURNAL_HEADER_SIZE + 64,
        "seeded journal is too small to pick meaningful offsets: {journal_size} bytes"
    );

    // --- Pick deterministic random offsets, plus pathological boundary cuts.
    // The random draws explore the interior; the boundary offsets exercise
    // edge cases (just past the header, mid-prefix, one byte before EOF) the
    // LCG is statistically unlikely to hit.
    let mut rng = Lcg(RNG_SEED);
    let mut offsets: Vec<u64> = (0..TRUNCATION_POINTS)
        .map(|_| rng.next_in_range(JOURNAL_HEADER_SIZE, journal_size))
        .collect();
    offsets.push(JOURNAL_HEADER_SIZE + 1);
    offsets.push(JOURNAL_HEADER_SIZE + 7);
    offsets.push(journal_size - 1);
    offsets.push(journal_size);

    // --- Drive each cut ---------------------------------------------------
    let mut failures: Vec<String> = Vec::new();
    for (idx, offset) in offsets.iter().copied().enumerate() {
        let (_dir, iter_db) = clone_seeded(&template_db);
        crash_harness::truncate_journal_to_offset(&iter_db, offset).expect("truncate");

        let reopen = Client::open_with_options(
            &iter_db,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        );
        let client = match reopen {
            Ok(c) => c,
            Err(e) => {
                failures.push(format!("cut#{idx} offset={offset}: reopen failed: {e:?}"));
                continue;
            }
        };

        // Readability: count_documents must succeed post-reopen.
        let col_a = client.database("ns_a").collection::<Document>("docs");
        let col_b = client.database("ns_b").collection::<Document>("docs");
        let count_a = match col_a.count_documents(doc! {}) {
            Ok(n) => n,
            Err(e) => {
                failures.push(format!(
                    "cut#{idx} offset={offset}: count_documents(ns_a) failed: {e:?}"
                ));
                continue;
            }
        };
        let count_b = match col_b.count_documents(doc! {}) {
            Ok(n) => n,
            Err(e) => {
                failures.push(format!(
                    "cut#{idx} offset={offset}: count_documents(ns_b) failed: {e:?}"
                ));
                continue;
            }
        };
        if offset == journal_size && count_a + count_b == 0 {
            failures.push(format!(
                "cut#{idx} offset={offset}: full journal reopened with both namespaces empty"
            ));
            continue;
        }

        // Normal drop here — this iteration's journal is a disposable copy.
        drop(client);
    }

    assert!(
        failures.is_empty(),
        "crash_injection random cuts had {} failure(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
