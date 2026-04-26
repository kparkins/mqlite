//! Phase 1 US-012 — §10.10 observability counters.
//!
//! Four counters make the §4.1 / §10.3 publish-decision table
//! observable:
//!   - `read_epoch_publish_count` — every `publish_commit` call.
//!   - `published_catalog_rebuild_count` — only when `dirty.published_catalog_dirty`.
//!   - `catalog_header_sync_count` — only when `dirty.catalog_header_dirty`.
//!   - `root_neutral_commit_count` — only when both flags are clear.
//!
//! Also covers §10.8 #21 (root-neutral workload does not rebuild) and
//! §10.8 #22 (building-index iteration does not publish).

use bson::{doc, Document};
use mqlite::mvcc::metrics::{
    catalog_header_sync_count_snapshot, published_catalog_rebuild_count_snapshot,
    read_epoch_publish_count_snapshot, reset_catalog_header_sync_count,
    reset_published_catalog_rebuild_count, reset_read_epoch_publish_count,
    reset_root_neutral_commit_count, root_neutral_commit_count_snapshot,
};
use mqlite::{Client, IndexModel};
use std::sync::Mutex;

static COUNTER_SERIAL: Mutex<()> = Mutex::new(());

fn reset_all() {
    reset_read_epoch_publish_count();
    reset_published_catalog_rebuild_count();
    reset_catalog_header_sync_count();
    reset_root_neutral_commit_count();
}

fn new_client(name: &str) -> (Client, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{}.mqlite", name));
    let client = Client::open(&path).unwrap();
    (client, dir)
}

#[test]
fn publish_count_ticks_once_per_publish() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("pc_pub");
    // Prime.
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 0i32 })
        .unwrap();

    reset_all();
    // Do N arbitrary CRUD commits — each runs publish_commit exactly once.
    let col = client.database("db").collection::<Document>("c");
    const N: i32 = 7;
    for i in 1..=N {
        col.insert_one(&doc! { "_id": i }).unwrap();
    }
    let pubs = read_epoch_publish_count_snapshot();
    assert_eq!(
        pubs, N as u64,
        "read_epoch_publish_count must rise by exactly N={} after N CRUD commits; saw {}",
        N, pubs
    );
}

#[test]
fn rebuild_count_stays_flat_under_root_neutral_workload() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("pc_rebuild_flat");
    // Prime the namespace.
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 0i32 })
        .unwrap();

    reset_all();
    // §10.8 #21: N root-neutral inserts should NOT rebuild the catalog.
    let col = client.database("db").collection::<Document>("c");
    const N: i32 = 12;
    for i in 1..=N {
        col.insert_one(&doc! { "_id": i }).unwrap();
    }
    assert_eq!(
        published_catalog_rebuild_count_snapshot(),
        0,
        "§10.8 #21: root-neutral workload must not rebuild catalog"
    );
    assert_eq!(
        read_epoch_publish_count_snapshot(),
        N as u64,
        "§10.8 #21: publish count must rise linearly with commits"
    );
}

#[test]
fn header_sync_count_ticks_for_header_only_commit() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("pc_hdr");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "tags": "scalar" })
        .unwrap();
    col.create_index(IndexModel::builder().keys(doc! { "tags": 1 }).build())
        .unwrap();

    reset_all();
    // multikey flip on a Ready index: sets ONLY catalog_header_dirty.
    col.insert_one(&doc! { "_id": 1i32, "tags": ["a", "b"] })
        .unwrap();
    assert_eq!(
        catalog_header_sync_count_snapshot(),
        1,
        "multikey flip must tick catalog_header_sync_count exactly once"
    );
    assert_eq!(
        published_catalog_rebuild_count_snapshot(),
        0,
        "multikey flip must NOT rebuild catalog"
    );
}

#[test]
fn root_neutral_commit_count_ticks_when_both_flags_clear() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("pc_root_neutral");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32 }).unwrap();

    reset_all();
    const N: i32 = 5;
    for i in 1..=N {
        col.insert_one(&doc! { "_id": i }).unwrap();
    }
    assert_eq!(
        root_neutral_commit_count_snapshot(),
        N as u64,
        "every root-neutral commit must tick root_neutral_commit_count"
    );
}

#[test]
fn building_index_iteration_does_not_publish() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    // §10.8 #22: during create_index_build iteration (the body loop
    // that scans the data tree and populates the index), no publishes
    // occur. The reserve step publishes once (Building entry becomes
    // visible), then iteration runs without publishing, then the
    // commit step publishes once (Building→Ready). Total publishes for
    // a create_index call: 2.
    let (client, _dir) = new_client("pc_building");
    let col = client.database("db").collection::<Document>("c");
    // Seed enough documents to make the build iterate meaningfully.
    for i in 0..50 {
        col.insert_one(&doc! { "_id": i, "x": i }).unwrap();
    }

    reset_all();
    col.create_index(IndexModel::builder().keys(doc! { "x": 1 }).build())
        .unwrap();
    let pubs = read_epoch_publish_count_snapshot();
    assert_eq!(
        pubs, 2,
        "create_index must publish exactly 2x (reserve + commit); iteration \
         itself contributes 0 publishes; saw {}",
        pubs
    );
    let rebuilds = published_catalog_rebuild_count_snapshot();
    assert_eq!(
        rebuilds, 2,
        "create_index must rebuild catalog Arc exactly 2x (Building entry \
         appear + Building→Ready); saw {}",
        rebuilds
    );
}
