#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Phase 1 US-011 / §4.4 — header-dirty commits do NOT rebuild the
//! published catalog.
//!
//! §4.4 invariant: `catalog_header_dirty` and `published_catalog_dirty`
//! are decoupled. A commit may need `sync_catalog_root_overlay`
//! (because the on-disk catalog tree changed) while still reusing the
//! existing published catalog. The §10.3 "multikey flip on a
//! Ready index" row is exactly this shape:
//!   - published_catalog_dirty = false (multikey is not published)
//!   - catalog_header_dirty    = true  (catalog B-tree + header updated)

use bson::{doc, Document};
use mqlite::mvcc::metrics::{
    catalog_header_sync_count_snapshot, published_catalog_rebuild_count_snapshot,
    read_epoch_publish_count_snapshot, reset_catalog_header_sync_count,
    reset_published_catalog_rebuild_count, reset_read_epoch_publish_count,
};
use mqlite::{Client, IndexModel};
use std::sync::Mutex;

static COUNTER_SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn multikey_flip_sets_header_dirty_only_and_reuses_catalog_arc() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hdr_only.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("db").collection::<Document>("c");

    // Seed a scalar-only doc so the Ready index starts multikey=false.
    col.insert_one(&doc! { "_id": 0i32, "tags": "scalar" })
        .unwrap();
    col.create_index(IndexModel::builder().keys(doc! { "tags": 1 }).build())
        .unwrap();

    // Capture the pre-commit catalog generation.
    let pre_gen = client.__published_catalog_gen();

    reset_read_epoch_publish_count();
    reset_published_catalog_rebuild_count();
    reset_catalog_header_sync_count();

    // Insert a doc with an array `tags` — flips multikey=true on the
    // Ready index. §10.3: `published_catalog_dirty=false`,
    // `catalog_header_dirty=true`. Expected counter deltas:
    //   - read_epoch_publish_count: +1 (every CRUD commit publishes)
    //   - published_catalog_rebuild_count: +0 (generation unchanged)
    //   - catalog_header_sync_count: +1 (sync_catalog_root_overlay ran)
    col.insert_one(&doc! { "_id": 1i32, "tags": ["a", "b"] })
        .unwrap();

    let post_gen = client.__published_catalog_gen();
    assert_eq!(
        pre_gen, post_gen,
        "§4.4: published-catalog generation must be preserved across a \
         header-dirty-only commit; pre={} post={}",
        pre_gen, post_gen
    );
    assert_eq!(
        published_catalog_rebuild_count_snapshot(),
        0,
        "§4.4: published_catalog_rebuild_count must NOT increment \
         when only catalog_header_dirty is set"
    );
    assert_eq!(
        catalog_header_sync_count_snapshot(),
        1,
        "§4.4: catalog_header_sync_count must rise by exactly 1 for the \
         multikey-flip commit"
    );
    assert_eq!(
        read_epoch_publish_count_snapshot(),
        1,
        "§4.4: every commit still invokes publish_commit exactly once"
    );
}
