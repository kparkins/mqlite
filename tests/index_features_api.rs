//! Black-box functional tests for partial indexes (`partialFilterExpression`).
//!
//! Each test opens a temp-file-backed `Client` and drives the public API
//! end-to-end: index creation + `list_indexes` roundtrip, on-disk persistence
//! across a reopen, write-path maintenance (insert / delete / the four update
//! transitions), unique-partial semantics, an index build over existing mixed
//! data, and the create-time restrictions (partial + sparse, partial on `_id`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test targets use assertion-style panics and setup unwraps"
)]

use std::time::{SystemTime, UNIX_EPOCH};

use mqlite::{doc, Bson, Client, DateTime, Document, Hint, IndexModel, IndexOptions};
use tempfile::TempDir;

/// Current Unix milliseconds.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as i64
}

/// A BSON `DateTime` `seconds` seconds in the past relative to now.
fn seconds_ago(seconds: i64) -> DateTime {
    DateTime::from_millis(now_millis() - seconds * 1000)
}

/// A BSON `DateTime` `seconds` seconds in the future relative to now.
fn seconds_from_now(seconds: i64) -> DateTime {
    DateTime::from_millis(now_millis() + seconds * 1000)
}

/// Open a temp-file-backed collection for the given namespace.
fn open_collection(tempdir: &TempDir, db: &str, coll: &str) -> mqlite::Collection<Document> {
    let client = Client::open(tempdir.path().join("db.mqlite")).expect("open");
    client.database(db).collection::<Document>(coll)
}

/// Run a `find(filter)` and return matched `_id`s (as i32), sorted ascending.
fn matched_ids(col: &mqlite::Collection<Document>, filter: Document) -> Vec<i32> {
    let docs: Vec<Document> = col
        .find(filter)
        .run()
        .expect("find")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");
    let mut ids: Vec<i32> = docs
        .iter()
        .map(|d| d.get_i32("_id").expect("_id is int32"))
        .collect();
    ids.sort_unstable();
    ids
}

/// Run a `find(filter).hint(hint)` and return matched `_id`s, sorted ascending.
fn matched_ids_hinted(
    col: &mqlite::Collection<Document>,
    filter: Document,
    hint: Hint,
) -> Vec<i32> {
    let docs: Vec<Document> = col
        .find(filter)
        .hint(hint)
        .run()
        .expect("find hinted")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");
    let mut ids: Vec<i32> = docs
        .iter()
        .map(|d| d.get_i32("_id").expect("_id is int32"))
        .collect();
    ids.sort_unstable();
    ids
}

// ---------------------------------------------------------------------------
// listIndexes roundtrip
// ---------------------------------------------------------------------------

#[test]
fn create_partial_index_roundtrips_through_list_indexes() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    let pfe = doc! { "qty": { "$gt": 10i32 } };

    col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .partial_filter_expression(pfe.clone())
            .build(),
    )
    .expect("create partial index");

    let infos = col.list_indexes().expect("list indexes");
    let info = infos
        .iter()
        .find(|i| i.name == "qty_1")
        .expect("partial index present");
    assert_eq!(info.partial_filter_expression.as_ref(), Some(&pfe));
}

// ---------------------------------------------------------------------------
// On-disk persistence across reopen
// ---------------------------------------------------------------------------

#[test]
fn partial_index_persists_across_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("persist.mqlite");
    let pfe = doc! { "active": true };

    {
        let client = Client::open(&db_path).expect("open new");
        let col = client.database("test").collection::<Document>("users");
        col.create_index(
            IndexModel::builder()
                .keys(doc! { "name": 1 })
                .partial_filter_expression(pfe.clone())
                .build(),
        )
        .expect("create partial index");
        col.insert_one(&doc! { "_id": 1i32, "name": "alice", "active": true })
            .expect("insert matching");
        col.insert_one(&doc! { "_id": 2i32, "name": "bob", "active": false })
            .expect("insert non-matching");
    }

    {
        let client = Client::open(&db_path).expect("reopen");
        let col = client.database("test").collection::<Document>("users");
        let infos = col.list_indexes().expect("list after reopen");
        let info = infos
            .iter()
            .find(|i| i.name == "name_1")
            .expect("partial index survives reopen");
        assert_eq!(
            info.partial_filter_expression.as_ref(),
            Some(&pfe),
            "PFE must survive reopen"
        );

        // A covering query still returns the right documents after reopen.
        assert_eq!(matched_ids(&col, doc! { "active": true, "name": "alice" }), vec![1]);
    }
}

// ---------------------------------------------------------------------------
// Write-path: insert maintenance + covering query uses the index correctly
// ---------------------------------------------------------------------------

#[test]
fn covering_query_returns_only_matching_docs() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .partial_filter_expression(doc! { "qty": { "$gt": 10i32 } })
            .build(),
    )
    .expect("create partial index");

    col.insert_one(&doc! { "_id": 1i32, "qty": 50i32 }).unwrap();
    col.insert_one(&doc! { "_id": 2i32, "qty": 5i32 }).unwrap();
    col.insert_one(&doc! { "_id": 3i32, "qty": 11i32 }).unwrap();

    // Covering query (uses the partial index): qty>10 -> docs 1 and 3.
    let via_index = matched_ids(&col, doc! { "qty": { "$gt": 10i32 } });
    // Full-scan comparison forcing $natural.
    let via_scan = matched_ids_hinted(
        &col,
        doc! { "qty": { "$gt": 10i32 } },
        Hint::Keys(doc! { "$natural": 1i32 }),
    );
    assert_eq!(via_index, vec![1, 3]);
    assert_eq!(via_index, via_scan, "index and full-scan must agree");
}

// ---------------------------------------------------------------------------
// Write-path: the four update transitions keep the index consistent
// ---------------------------------------------------------------------------

#[test]
fn update_transitions_keep_partial_index_consistent() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .partial_filter_expression(doc! { "qty": { "$gt": 10i32 } })
            .build(),
    )
    .expect("create partial index");

    // Seed: 1 matches, 2 does not.
    col.insert_one(&doc! { "_id": 1i32, "qty": 50i32 }).unwrap();
    col.insert_one(&doc! { "_id": 2i32, "qty": 5i32 }).unwrap();

    // stayed-in: 1 stays > 10.
    col.replace_one(doc! { "_id": 1i32 }, &doc! { "qty": 30i32 })
        .run()
        .unwrap();
    // entered: 2 crosses into the PFE.
    col.replace_one(doc! { "_id": 2i32 }, &doc! { "qty": 99i32 })
        .run()
        .unwrap();
    // left: a new doc that matches, then drops out.
    col.insert_one(&doc! { "_id": 3i32, "qty": 40i32 }).unwrap();
    col.replace_one(doc! { "_id": 3i32 }, &doc! { "qty": 1i32 })
        .run()
        .unwrap();
    // stayed-out: a doc that never matches and is updated within the out-region.
    col.insert_one(&doc! { "_id": 4i32, "qty": 2i32 }).unwrap();
    col.replace_one(doc! { "_id": 4i32 }, &doc! { "qty": 3i32 })
        .run()
        .unwrap();

    // Expected membership after all transitions: 1 (30) and 2 (99).
    let via_index = matched_ids(&col, doc! { "qty": { "$gt": 10i32 } });
    let via_scan = matched_ids_hinted(
        &col,
        doc! { "qty": { "$gt": 10i32 } },
        Hint::Keys(doc! { "$natural": 1i32 }),
    );
    assert_eq!(via_index, vec![1, 2]);
    assert_eq!(
        via_index, via_scan,
        "index membership must match full-scan after update transitions"
    );
}

// ---------------------------------------------------------------------------
// Delete maintenance
// ---------------------------------------------------------------------------

#[test]
fn delete_keeps_partial_index_consistent() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .partial_filter_expression(doc! { "qty": { "$gt": 10i32 } })
            .build(),
    )
    .expect("create partial index");

    col.insert_one(&doc! { "_id": 1i32, "qty": 50i32 }).unwrap();
    col.insert_one(&doc! { "_id": 2i32, "qty": 5i32 }).unwrap();
    col.insert_one(&doc! { "_id": 3i32, "qty": 20i32 }).unwrap();

    // Delete a matching doc and a non-matching doc.
    col.delete_one(doc! { "_id": 1i32 }).unwrap();
    col.delete_one(doc! { "_id": 2i32 }).unwrap();

    let via_index = matched_ids(&col, doc! { "qty": { "$gt": 10i32 } });
    assert_eq!(via_index, vec![3]);
}

// ---------------------------------------------------------------------------
// Unique partial: duplicates allowed outside PFE, blocked inside
// ---------------------------------------------------------------------------

#[test]
fn unique_partial_allows_outside_blocks_inside() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "email": 1 })
            .options(IndexOptions::new().unique(true))
            .partial_filter_expression(doc! { "verified": true })
            .build(),
    )
    .expect("create unique partial index");

    // Two unverified docs may share an email freely (outside the PFE).
    col.insert_one(&doc! { "_id": 1i32, "email": "a@x.com", "verified": false })
        .expect("first unverified insert");
    col.insert_one(&doc! { "_id": 2i32, "email": "a@x.com", "verified": false })
        .expect("duplicate email allowed outside PFE");

    // First verified doc with a fresh email succeeds.
    col.insert_one(&doc! { "_id": 3i32, "email": "b@x.com", "verified": true })
        .expect("first verified insert");
    // A second verified doc with the same email is a duplicate inside the PFE.
    let dup = col.insert_one(&doc! { "_id": 4i32, "email": "b@x.com", "verified": true });
    assert!(
        dup.is_err(),
        "duplicate verified email must be rejected inside the PFE"
    );
}

// ---------------------------------------------------------------------------
// Index build over existing mixed data
// ---------------------------------------------------------------------------

#[test]
fn build_partial_index_over_existing_mixed_data() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");

    // Insert a mix BEFORE the index exists.
    for (id, qty) in [(1i32, 50i32), (2, 5), (3, 11), (4, 10), (5, 100)] {
        col.insert_one(&doc! { "_id": id, "qty": qty }).unwrap();
    }

    col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .partial_filter_expression(doc! { "qty": { "$gt": 10i32 } })
            .build(),
    )
    .expect("build partial index over existing data");

    // Only qty>10 (1, 3, 5) are indexed; a covering query must match exactly
    // those, and agree with the full scan.
    let via_index = matched_ids(&col, doc! { "qty": { "$gt": 10i32 } });
    let via_scan = matched_ids_hinted(
        &col,
        doc! { "qty": { "$gt": 10i32 } },
        Hint::Keys(doc! { "$natural": 1i32 }),
    );
    assert_eq!(via_index, vec![1, 3, 5]);
    assert_eq!(via_index, via_scan);
}

// ---------------------------------------------------------------------------
// Create-time restrictions
// ---------------------------------------------------------------------------

#[test]
fn partial_plus_sparse_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .options(IndexOptions::new().sparse(true))
            .partial_filter_expression(doc! { "qty": { "$gt": 1i32 } })
            .build(),
    );
    assert!(result.is_err(), "partial + sparse must be rejected");
}

#[test]
fn partial_on_id_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "_id": 1 })
            .partial_filter_expression(doc! { "_id": { "$gt": 1i32 } })
            .build(),
    );
    assert!(result.is_err(), "partial index on _id must be rejected");
}

#[test]
fn empty_partial_filter_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "inv");
    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "qty": 1 })
            .partial_filter_expression(doc! {})
            .build(),
    );
    assert!(result.is_err(), "empty partialFilterExpression must be rejected");
}

// ===========================================================================
// TTL indexes (expireAfterSeconds)
// ===========================================================================

// ---------------------------------------------------------------------------
// listIndexes roundtrip + reopen persistence
// ---------------------------------------------------------------------------

#[test]
fn create_ttl_index_roundtrips_through_list_indexes() {
    let dir = TempDir::new().expect("tempdir");
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let col = client.database("test").collection::<Document>("events");

    col.create_index(
        IndexModel::builder()
            .keys(doc! { "createdAt": 1 })
            .expire_after_seconds(3600)
            .build(),
    )
    .expect("create ttl index");

    let infos = col.list_indexes().expect("list indexes");
    let info = infos
        .iter()
        .find(|i| i.name == "createdAt_1")
        .expect("ttl index present");
    assert_eq!(info.expire_after_seconds, Some(3600));
}

#[test]
fn ttl_index_persists_across_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("ttl_persist.mqlite");

    {
        let client = Client::open(&db_path).expect("open new");
        let col = client.database("test").collection::<Document>("events");
        col.create_index(
            IndexModel::builder()
                .keys(doc! { "createdAt": 1 })
                .expire_after_seconds(120)
                .build(),
        )
        .expect("create ttl index");
    }

    {
        let client = Client::open(&db_path).expect("reopen");
        let col = client.database("test").collection::<Document>("events");
        let infos = col.list_indexes().expect("list after reopen");
        let info = infos
            .iter()
            .find(|i| i.name == "createdAt_1")
            .expect("ttl index survives reopen");
        assert_eq!(
            info.expire_after_seconds,
            Some(120),
            "expireAfterSeconds must survive reopen"
        );
    }
}

// ---------------------------------------------------------------------------
// Create-time validation
// ---------------------------------------------------------------------------

#[test]
fn ttl_on_compound_index_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "events");
    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "a": 1, "b": 1 })
            .expire_after_seconds(60)
            .build(),
    );
    assert!(
        result.is_err(),
        "TTL on a compound index must be rejected"
    );
}

#[test]
fn ttl_with_negative_seconds_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "events");
    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "createdAt": 1 })
            .expire_after_seconds(-5)
            .build(),
    );
    assert!(result.is_err(), "negative expireAfterSeconds must be rejected");
}

#[test]
fn ttl_on_id_is_rejected() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "events");
    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "_id": 1 })
            .expire_after_seconds(60)
            .build(),
    );
    assert!(result.is_err(), "TTL on _id must be rejected");
}

// ---------------------------------------------------------------------------
// Sweep correctness
// ---------------------------------------------------------------------------

#[test]
fn sweep_deletes_past_dates_keeps_future_and_nondate_and_missing() {
    let dir = TempDir::new().expect("tempdir");
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let col = client.database("test").collection::<Document>("events");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "expireAt": 1 })
            .expire_after_seconds(0)
            .build(),
    )
    .expect("create ttl index");

    // 1: past date -> expires. 2: future date -> survives.
    col.insert_one(&doc! { "_id": 1i32, "expireAt": seconds_ago(3600) })
        .unwrap();
    col.insert_one(&doc! { "_id": 2i32, "expireAt": seconds_from_now(3600) })
        .unwrap();
    // 3: non-date value in the indexed field -> never expires.
    col.insert_one(&doc! { "_id": 3i32, "expireAt": "not-a-date" })
        .unwrap();
    // 4: missing indexed field -> never expires.
    col.insert_one(&doc! { "_id": 4i32, "other": 1i32 }).unwrap();

    let deleted = client.sweep_expired().expect("sweep");
    assert_eq!(deleted, 1, "only the past-date document expires");

    let survivors = matched_ids(&col, doc! {});
    assert_eq!(survivors, vec![2, 3, 4]);
}

#[test]
fn sweep_array_of_dates_expires_when_any_element_is_past() {
    let dir = TempDir::new().expect("tempdir");
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let col = client.database("test").collection::<Document>("events");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "dates": 1 })
            .expire_after_seconds(0)
            .build(),
    )
    .expect("create ttl index");

    // 1: array containing one past date (plus a future one) -> expires.
    col.insert_one(&doc! {
        "_id": 1i32,
        "dates": Bson::Array(vec![
            Bson::DateTime(seconds_from_now(3600)),
            Bson::DateTime(seconds_ago(3600)),
        ]),
    })
    .unwrap();
    // 2: array with only future dates -> survives.
    col.insert_one(&doc! {
        "_id": 2i32,
        "dates": Bson::Array(vec![
            Bson::DateTime(seconds_from_now(3600)),
            Bson::DateTime(seconds_from_now(7200)),
        ]),
    })
    .unwrap();
    // 3: array of non-date values -> survives.
    col.insert_one(&doc! {
        "_id": 3i32,
        "dates": Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]),
    })
    .unwrap();

    let deleted = client.sweep_expired().expect("sweep");
    assert_eq!(deleted, 1, "only the array with a past element expires");

    let survivors = matched_ids(&col, doc! {});
    assert_eq!(survivors, vec![2, 3]);
}

#[test]
fn sweep_partial_ttl_only_expires_pfe_matching_docs() {
    let dir = TempDir::new().expect("tempdir");
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let col = client.database("test").collection::<Document>("events");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "expireAt": 1 })
            .partial_filter_expression(doc! { "archived": true })
            .expire_after_seconds(0)
            .build(),
    )
    .expect("create partial ttl index");

    // 1: past date AND archived -> expires (matches PFE).
    col.insert_one(&doc! { "_id": 1i32, "expireAt": seconds_ago(3600), "archived": true })
        .unwrap();
    // 2: past date but NOT archived -> survives (outside PFE).
    col.insert_one(&doc! { "_id": 2i32, "expireAt": seconds_ago(3600), "archived": false })
        .unwrap();
    // 3: past date, no archived field -> survives (outside PFE).
    col.insert_one(&doc! { "_id": 3i32, "expireAt": seconds_ago(3600) })
        .unwrap();

    let deleted = client.sweep_expired().expect("sweep");
    assert_eq!(deleted, 1, "only the PFE-matching past document expires");

    let survivors = matched_ids(&col, doc! {});
    assert_eq!(survivors, vec![2, 3]);
}

#[test]
fn sweep_returns_total_deleted_count() {
    let dir = TempDir::new().expect("tempdir");
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let col = client.database("test").collection::<Document>("events");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "expireAt": 1 })
            .expire_after_seconds(0)
            .build(),
    )
    .expect("create ttl index");

    for id in 1..=5i32 {
        col.insert_one(&doc! { "_id": id, "expireAt": seconds_ago(100) })
            .unwrap();
    }
    // One survivor in the future.
    col.insert_one(&doc! { "_id": 99i32, "expireAt": seconds_from_now(3600) })
        .unwrap();

    let deleted = client.sweep_expired().expect("sweep");
    assert_eq!(deleted, 5, "sweep_expired returns the deleted count");
    assert_eq!(matched_ids(&col, doc! {}), vec![99]);

    // A second sweep with nothing newly expired deletes zero.
    let deleted_again = client.sweep_expired().expect("second sweep");
    assert_eq!(deleted_again, 0);
}

// ---------------------------------------------------------------------------
// TTL + unique: index entries are removed when the doc is swept
// ---------------------------------------------------------------------------

#[test]
fn sweep_clears_unique_index_entries_of_deleted_docs() {
    let dir = TempDir::new().expect("tempdir");
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let col = client.database("test").collection::<Document>("sessions");
    // Unique TTL index on `token`, expiring on the `token` date value.
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "token": 1 })
            .options(IndexOptions::new().unique(true))
            .expire_after_seconds(0)
            .build(),
    )
    .expect("create unique ttl index");

    // Insert a doc whose unique `token` is a past date.
    let token = seconds_ago(3600);
    col.insert_one(&doc! { "_id": 1i32, "token": token })
        .unwrap();

    let deleted = client.sweep_expired().expect("sweep");
    assert_eq!(deleted, 1);

    // The unique value is now free: reinserting the same token must succeed,
    // proving the swept document's unique-index entry was removed.
    col.insert_one(&doc! { "_id": 2i32, "token": seconds_from_now(3600) })
        .expect("reinsert reusing the unique token value must succeed");
    assert_eq!(matched_ids(&col, doc! {}), vec![2]);
}

// ---------------------------------------------------------------------------
// Sweep on reopen (open-time sweep)
// ---------------------------------------------------------------------------

#[test]
fn open_time_sweep_deletes_expired_docs() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("open_sweep.mqlite");

    {
        let client = Client::open(&db_path).expect("open new");
        let col = client.database("test").collection::<Document>("events");
        col.create_index(
            IndexModel::builder()
                .keys(doc! { "expireAt": 1 })
                .expire_after_seconds(0)
                .build(),
        )
        .expect("create ttl index");
        col.insert_one(&doc! { "_id": 1i32, "expireAt": seconds_ago(3600) })
            .unwrap();
        col.insert_one(&doc! { "_id": 2i32, "expireAt": seconds_from_now(3600) })
            .unwrap();
        client.close().expect("close");
    }

    {
        // Reopen: the open-time sweep runs after recovery, so the expired
        // document is already gone. Assert via document absence (the open-sweep
        // count is internal and not observable through the public API).
        let client = Client::open(&db_path).expect("reopen");
        let col = client.database("test").collection::<Document>("events");
        let survivors = matched_ids(&col, doc! {});
        assert_eq!(
            survivors, vec![2],
            "open-time sweep removes the expired document on reopen"
        );
    }
}
