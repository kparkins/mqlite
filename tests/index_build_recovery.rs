#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![doc = "Integration tests requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

mod crash_harness;

use std::path::Path;
use std::sync::Mutex;

use bson::{doc, Document};
use mqlite::{Client, IndexModel, Result};

const DOC_COUNT: i32 = 240;
const TAG_MATCH_COUNT: usize = 48;
static CRASH_RECOVERY_TEST_LOCK: Mutex<()> = Mutex::new(());

fn seed_collection(client: &Client) -> mqlite::Collection<Document> {
    let collection = client.database("db").collection::<Document>("docs");
    for id in 0..DOC_COUNT {
        collection
            .insert_one(&doc! {
                "_id": id,
                "tag": format!("tag-{}", id % 5),
                "payload": format!("payload-{id:04}"),
            })
            .expect("seed document");
    }
    collection
}

fn assert_ready_index_covers_all_docs(client: &Client) {
    let collection = client.database("db").collection::<Document>("docs");
    let cursor = collection
        .find(doc! { "tag": "tag-3" })
        .run()
        .expect("find via rebuilt index");
    let explain = cursor.explain().expect("explain");
    let docs = cursor.collect::<Result<Vec<_>>>().expect("collect docs");

    assert_eq!(
        explain.index_used.as_deref(),
        Some("tag_1"),
        "planner must use the Ready index after reopen"
    );
    assert!(!explain.full_scan, "query must not fall back to COLLSCAN");
    assert_eq!(
        docs.len(),
        TAG_MATCH_COUNT,
        "rebuilt index must cover every matching seeded document"
    );
}

fn assert_no_legacy_ready_transition(path: &Path) {
    let legacy_frames =
        crash_harness::scan_legacy_commit_frames(path).expect("scan legacy commit frames");
    assert!(
        legacy_frames.is_empty(),
        "Phase 6 create_index must not write retired legacy commit frames; found {legacy_frames:?}",
    );
}

#[test]
fn test_class_b_b_index_build_reopens_without_legacy_ready_commit() {
    let _guard = CRASH_RECOVERY_TEST_LOCK.lock().expect("lock recovery test");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us018c_b_b.mqlite");

    let client = crash_harness::open_fullsync(&path).expect("open");
    let collection = seed_collection(&client);
    collection
        .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
        .expect("create index");
    assert_no_legacy_ready_transition(&path);
    std::mem::forget(client);

    let (reopened, _recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    assert_ready_index_covers_all_docs(&reopened);
}

#[test]
fn test_class_b_c_ready_durable_index_intact() {
    let _guard = CRASH_RECOVERY_TEST_LOCK.lock().expect("lock recovery test");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us018c_b_c.mqlite");

    {
        let client = crash_harness::open_fullsync(&path).expect("open");
        let collection = seed_collection(&client);
        collection
            .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
            .expect("create index");
        std::mem::forget(client);
    }

    let (reopened, _recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    assert_ready_index_covers_all_docs(&reopened);
}
