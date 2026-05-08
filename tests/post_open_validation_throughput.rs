//! Phase 1 US-017 / §10.8 #29 — post-open validation throughput
//! gate.
//!
//! Populates a database with a large number of logical frames spread
//! across many collections and verifies that reopen + the first post-
//! recovery validation pass completes within a documented wall-clock
//! budget. Guards against an O(N^2) regression in Phase 2 §5.2
//! post-open validation.
//!
//! Reference runner (from the PRD): 8-core x86_64, 32 GB RAM,
//! NVMe SSD — 500ms budget. Developer laptops may be slower; the
//! env var `CARGO_VALIDATION_BUDGET_MULT` multiplies the budget
//! (default 1). CI should keep it at 1; a developer rerunning
//! locally can set it to 2-3 while tuning.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};

const NUM_COLLECTIONS: usize = 100;
const LOGICAL_FRAMES: usize = 10_000;
const BUDGET_MS: u64 = 500;
const JOURNAL_HEADER_BYTES: u64 = 32;
const CHECKPOINT_APPLIED_LSN_OFFSET: usize = 102;
const CHECKPOINT_APPLIED_LSN_BYTES: usize = 8;

fn journal_path(db_path: &Path) -> PathBuf {
    let mut path = db_path.as_os_str().to_owned();
    path.push("-journal");
    PathBuf::from(path)
}

fn read_checkpoint_applied_lsn(db_path: &Path) -> u64 {
    let bytes = std::fs::read(db_path).expect("read database header");
    let end = CHECKPOINT_APPLIED_LSN_OFFSET + CHECKPOINT_APPLIED_LSN_BYTES;
    u64::from_le_bytes(
        bytes[CHECKPOINT_APPLIED_LSN_OFFSET..end]
            .try_into()
            .expect("checkpoint_applied_lsn bytes"),
    )
}

fn budget() -> Duration {
    let mult: u64 = env::var("CARGO_VALIDATION_BUDGET_MULT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    Duration::from_millis(BUDGET_MS * mult)
}

/// §10.8 #29: populate the DB with `LOGICAL_FRAMES` writes spread
/// across `NUM_COLLECTIONS` collections, close, reopen, and time the
/// reopen path. Asserts the open operation completes under the
/// budget.
#[test]
fn test_post_open_validation_throughput_at_10k_logical_frames() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("post_open.mqlite");

    // Phase 1: populate. Use FullSync to ensure every commit
    // produces a durable ChainCommit frame.
    {
        let client = Client::open_with_options(
            &path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .unwrap();
        let db = client.database("pov");
        for i in 0..NUM_COLLECTIONS {
            db.create_collection(&format!("c{}", i))
                .expect("create_collection");
        }
        let per_coll = LOGICAL_FRAMES / NUM_COLLECTIONS;
        for i in 0..NUM_COLLECTIONS {
            let col = db.collection::<Document>(&format!("c{}", i));
            for j in 0..per_coll {
                col.insert_one(&doc! {
                    "_id": (i * per_coll + j) as i32,
                    "v": j as i32,
                })
                .expect("insert");
            }
        }
        // Checkpoint so the journal is compacted before the reopen.
        // A reopen against a full journal would measure journal
        // recovery time rather than post-open validation.
        client.checkpoint().expect("checkpoint");
    }

    // Phase 2: time the reopen.
    let start = Instant::now();
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("reopen must succeed");
    let elapsed = start.elapsed();
    // Keep the client alive across the measurement to avoid
    // early-close cost leaking in.
    let _keepalive = client;

    let budget = budget();
    assert!(
        elapsed < budget,
        "§10.8 #29: post-open validation completed in {:?}, budget {:?} \
         (CARGO_VALIDATION_BUDGET_MULT controls the multiplier; reference \
         runner is 8-core x86_64, 32GB RAM, NVMe SSD with a 500ms base)",
        elapsed,
        budget
    );
    eprintln!(
        "[post_open_validation_throughput] elapsed={:?} budget={:?} \
         collections={} frames={}",
        elapsed, budget, NUM_COLLECTIONS, LOGICAL_FRAMES
    );
}

#[test]
fn checkpoint_marks_fully_materialized_logical_tail_applied() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("checkpoint_tail.mqlite");
    let journal = journal_path(&path);

    {
        let client = Client::open_with_options(
            &path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .unwrap();
        let col = client.database("pov").collection::<Document>("tail");
        for id in 0..8 {
            col.insert_one(&doc! { "_id": id, "v": id })
                .expect("insert");
        }

        let pre_checkpoint_len = std::fs::metadata(&journal).unwrap().len();
        assert!(
            pre_checkpoint_len > JOURNAL_HEADER_BYTES,
            "logical writes should leave journal frames before checkpoint"
        );

        client.checkpoint().expect("checkpoint");
        let post_checkpoint_len = std::fs::metadata(&journal).unwrap().len();
        let checkpoint_applied_lsn = read_checkpoint_applied_lsn(&path);
        assert_eq!(
            checkpoint_applied_lsn, pre_checkpoint_len,
            "checkpoint should mark the fully materialized logical tail as applied"
        );
        assert!(
            post_checkpoint_len >= checkpoint_applied_lsn,
            "Phase 8 retains valid prefix log bytes for recovery skip instead of \
             physically trimming them"
        );
    }

    let reopened = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .unwrap();
    let col = reopened.database("pov").collection::<Document>("tail");
    assert_eq!(col.count_documents(doc! {}).unwrap(), 8);
}
