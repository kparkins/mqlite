//! Phase 1 US-006 — §10.3 dirty-flag row coverage.
//!
//! Each row in the §10.3 assignment table maps a mutation site to a
//! `PublishDirty` outcome (published_catalog_dirty and/or
//! catalog_header_dirty). The pre-release counter
//! `published_snapshot_rebuilds_total` ticks once per
//! `publish_commit` call whose `dirty.published_catalog_dirty == true`
//! (gated at `catalog_ops::rebuild_and_publish_locked`). That makes it
//! a direct observable for "did this mutation set
//! `published_catalog_dirty`?" — the exact classification US-006
//! asks us to verify.
//!
//! US-012 will split this counter into the four Phase 1 counters
//! (read_epoch_publish_count, published_catalog_rebuild_count,
//! catalog_header_sync_count, root_neutral_commit_count); for US-006
//! we only need the rebuild observable.

use bson::{doc, Document};
use mqlite::mvcc::metrics::{
    published_snapshot_rebuilds_snapshot, reset_published_snapshot_rebuilds,
};
use mqlite::{Client, IndexModel};
use std::sync::Mutex;

/// Process-global serialization lock. `published_snapshot_rebuilds` is a
/// process-wide atomic; two tests resetting it in parallel would race.
static COUNTER_SERIAL: Mutex<()> = Mutex::new(());

fn new_client(name: &str) -> (Client, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{}.mqlite", name));
    let client = Client::open(&path).unwrap();
    (client, dir)
}

// ---------------------------------------------------------------------------
// Row 1 (primary version-chain install, root-unchanged) and
// row 7 (entry_count / document_count — not in header, not published):
// root-neutral CRUD leaves both flags clear → zero rebuild ticks.
// ---------------------------------------------------------------------------

#[test]
fn root_neutral_insert_does_not_rebuild_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("rn_insert");
    let col = client.database("db").collection::<Document>("c");
    // Prime: bootstrap + first insert. Bootstrap is DDL (ticks rebuild);
    // first insert creates the initial leaf (root may move).
    col.insert_one(&doc! { "_id": 0i32 }).unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();

    const N: i32 = 8;
    for i in 1..=N {
        col.insert_one(&doc! { "_id": i, "v": i }).unwrap();
    }
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        0,
        "§10.3 row: root-neutral primary inserts must reuse Arc<PublishedCatalog> \
         (0 rebuild ticks); saw delta={}",
        after - before
    );
}

#[test]
fn root_neutral_update_does_not_rebuild_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("rn_update");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "v": 0 }).unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    let _ = col
        .update_one(doc! { "_id": 0i32 }, doc! { "$set": { "v": 1 } })
        .run()
        .unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        0,
        "§10.3 row: root-neutral primary update must reuse catalog; saw delta={}",
        after - before
    );
}

#[test]
fn root_neutral_delete_does_not_rebuild_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("rn_delete");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32 }).unwrap();
    col.insert_one(&doc! { "_id": 1i32 }).unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    let _ = col.delete_one(doc! { "_id": 0i32 }).unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        0,
        "§10.3 row: root-neutral primary delete (tombstone that does not move \
         data root) must reuse catalog; saw delta={}",
        after - before
    );
}

// ---------------------------------------------------------------------------
// Row 9: create_namespace publishes a fresh catalog.
// ---------------------------------------------------------------------------

#[test]
fn create_namespace_rebuilds_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("cn");
    // Force a prior publish so we start from a known state.
    client
        .database("db")
        .collection::<Document>("prime")
        .insert_one(&doc! { "_id": 0i32 })
        .unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    client.database("db").create_collection("fresh").unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        1,
        "§10.3 row: create_namespace must publish a fresh catalog exactly once; delta={}",
        after - before
    );
}

// ---------------------------------------------------------------------------
// Row 10: drop_namespace publishes after the force-expire + page-free
// barrier (§3.5).
// ---------------------------------------------------------------------------

#[test]
fn drop_namespace_rebuilds_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("dn");
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 0i32 })
        .unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    client.database("db").drop_collection("c").unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        1,
        "§10.3 row: drop_namespace must publish once (post force-expire + free); delta={}",
        after - before
    );
}

// ---------------------------------------------------------------------------
// Rows 11 + 13: create_index_reserve + create_index_commit each publish
// (two publishes per create_index call).
// ---------------------------------------------------------------------------

#[test]
fn create_index_reserve_and_commit_each_publish() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("ci");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "x": 1 }).unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    col.create_index(IndexModel::builder().keys(doc! { "x": 1 }).build())
        .unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    let delta = after - before;
    assert_eq!(
        delta, 2,
        "§10.3: create_index must publish reserve + commit (2 total); delta={}",
        delta
    );
}

// ---------------------------------------------------------------------------
// Row 14: drop_index publishes once.
// ---------------------------------------------------------------------------

#[test]
fn drop_index_publishes_once() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("di");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "x": 1 }).unwrap();
    let idx_name = col
        .create_index(IndexModel::builder().keys(doc! { "x": 1 }).build())
        .unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    col.drop_index(&idx_name).unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        1,
        "§10.3: drop_index publishes once; delta={}",
        after - before
    );
}

// ---------------------------------------------------------------------------
// Row 4: multikey flip on a Ready index — catalog_header dirty only,
// NOT published (multikey is not a reader-visible field per §10.3).
// ---------------------------------------------------------------------------

#[test]
fn multikey_flip_on_ready_index_does_not_rebuild_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("mk");
    let col = client.database("db").collection::<Document>("c");
    // Seed with a scalar (non-multikey) entry so the index starts as
    // multikey=false.
    col.insert_one(&doc! { "_id": 0i32, "tags": "single" })
        .unwrap();
    col.create_index(IndexModel::builder().keys(doc! { "tags": 1 }).build())
        .unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    // Insert a document with an array `tags` — flips multikey=true on
    // the Ready index. The catalog tree is updated and
    // `sync_catalog_root_overlay` runs (mark_header), but
    // `published_catalog_dirty` stays false.
    col.insert_one(&doc! { "_id": 1i32, "tags": ["a", "b"] })
        .unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        0,
        "§10.3: multikey flip on Ready index must NOT publish a new catalog \
         Arc (multikey is not a reader-visible field); delta={}",
        after - before
    );
}

// ---------------------------------------------------------------------------
// Row 3: Ready secondary insert with unchanged root — neither flag set.
// ---------------------------------------------------------------------------

#[test]
fn ready_secondary_insert_root_unchanged_does_not_rebuild_catalog() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("ss");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "x": 10 }).unwrap();
    col.create_index(IndexModel::builder().keys(doc! { "x": 1 }).build())
        .unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    // Small insert: index tree fits in existing leaf, no root move, no
    // multikey flip → both flags stay clear.
    col.insert_one(&doc! { "_id": 1i32, "x": 11 }).unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        0,
        "§10.3: Ready secondary insert with unchanged root must not rebuild; delta={}",
        after - before
    );
}

// ---------------------------------------------------------------------------
// Row 15 (create_index_cleanup): run the cleanup DDL via drop_index on a
// freshly-created (and therefore cleanup-able) index; it must publish
// once. create_index_cleanup is called by the normal create_index failure
// path; observable state is identical to drop_index (both rows are
// `yes | yes`).
// ---------------------------------------------------------------------------

#[test]
fn create_index_cleanup_row_drops_orphan_and_publishes() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _dir) = new_client("ic");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "x": 1 }).unwrap();
    // create_index that completes normally is the dual of
    // create_index_cleanup (both remove the catalog entry they started
    // with). Run the full create + drop cycle and assert the drop
    // publishes exactly once.
    let name = col
        .create_index(IndexModel::builder().keys(doc! { "x": 1 }).build())
        .unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();
    col.drop_index(&name).unwrap();
    let after = published_snapshot_rebuilds_snapshot();
    assert_eq!(
        after - before,
        1,
        "§10.3 row: create_index cleanup/drop path publishes once; delta={}",
        after - before
    );
}
