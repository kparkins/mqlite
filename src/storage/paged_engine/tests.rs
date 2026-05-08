use super::btree_ops::btree_collscan;
use super::*;
use crate::mvcc::read_view::ReadView;
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use bson::doc;

fn engine() -> PagedEngine {
    let (e, _io) = buffered_engine();
    e
}

#[test]
fn insert_and_find_one() {
    let e = engine();
    e.insert("test.users", doc! { "name": "Alice", "age": 30 })
        .unwrap();
    let found = e.find_one("test.users", &doc! { "name": "Alice" }).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().get_str("name").unwrap(), "Alice");
}

#[test]
fn insert_missing_namespace_returns_empty_find() {
    let e = engine();
    let (found, _) = e
        .find("test.users", &doc! {}, &FindOptions::default())
        .unwrap();
    assert!(found.is_empty());
}

#[test]
fn insert_multiple_and_count() {
    let e = engine();
    for i in 0..10i32 {
        e.insert("test.c", doc! { "i": i }).unwrap();
    }
    let count = e.count("test.c", &doc! {}).unwrap();
    assert_eq!(count, 10);
}

#[test]
fn delete_one_removes_single_document() {
    let e = engine();
    e.insert("test.c", doc! { "x": 1 }).unwrap();
    e.insert("test.c", doc! { "x": 2 }).unwrap();
    let r = e.delete("test.c", &doc! { "x": 1 }, false).unwrap();
    assert_eq!(r.deleted_count, 1);
    assert_eq!(e.count("test.c", &doc! {}).unwrap(), 1);
}

#[test]
fn delete_many_removes_all_matching() {
    let e = engine();
    for i in 0..5i32 {
        e.insert("test.c", doc! { "v": i }).unwrap();
    }
    let r = e
        .delete("test.c", &doc! { "v": { "$gt": 2 } }, true)
        .unwrap();
    assert_eq!(r.deleted_count, 2); // v=3 and v=4
}

#[test]
fn update_one_modifies_field() {
    let e = engine();
    e.insert("test.c", doc! { "name": "Alice", "age": 30 })
        .unwrap();
    let r = e
        .update(
            "test.c",
            &doc! { "name": "Alice" },
            &doc! { "$set": { "age": 31 } },
            &UpdateOptions::default(),
            false,
        )
        .unwrap();
    assert_eq!(r.matched_count, 1);
    assert_eq!(r.modified_count, 1);
    let found = e
        .find_one("test.c", &doc! { "name": "Alice" })
        .unwrap()
        .unwrap();
    assert_eq!(found.get_i32("age").unwrap(), 31);
}

#[test]
fn add_to_set_duplicate_only_does_not_modify_document() {
    let e = engine();
    e.insert("test.c", doc! { "tags": ["a", "b"] }).unwrap();

    let r = e
        .update(
            "test.c",
            &doc! {},
            &doc! { "$addToSet": { "tags": "b" } },
            &UpdateOptions::default(),
            false,
        )
        .unwrap();

    assert_eq!(r.matched_count, 1);
    assert_eq!(r.modified_count, 0);
    let found = e.find_one("test.c", &doc! {}).unwrap().unwrap();
    assert_eq!(found.get_array("tags").unwrap().len(), 2);
}

#[test]
fn add_to_set_empty_each_does_not_modify_document() {
    let e = engine();
    e.insert("test.c", doc! { "tags": ["a", "b"] }).unwrap();

    let r = e
        .update(
            "test.c",
            &doc! {},
            &doc! { "$addToSet": { "tags": { "$each": [] } } },
            &UpdateOptions::default(),
            false,
        )
        .unwrap();

    assert_eq!(r.matched_count, 1);
    assert_eq!(r.modified_count, 0);
    let found = e.find_one("test.c", &doc! {}).unwrap().unwrap();
    assert_eq!(found.get_array("tags").unwrap().len(), 2);
}

#[test]
fn find_with_sort_and_limit() {
    let e = engine();
    for i in [3i32, 1, 2] {
        e.insert("test.c", doc! { "v": i }).unwrap();
    }
    let mut opts = FindOptions::default();
    opts.sort = Some(doc! { "v": 1 });
    opts.limit = Some(2);
    let (results, _) = e.find("test.c", &doc! {}, &opts).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get_i32("v").unwrap(), 1);
    assert_eq!(results[1].get_i32("v").unwrap(), 2);
}

#[test]
fn create_namespace_then_insert() {
    let e = engine();
    e.create_namespace("test.c").unwrap();
    e.insert("test.c", doc! { "k": "v" }).unwrap();
    assert_eq!(e.count("test.c", &doc! {}).unwrap(), 1);
}

#[test]
fn drop_namespace_removes_documents() {
    let e = engine();
    e.insert("test.c", doc! { "x": 1 }).unwrap();
    e.drop_namespace("test.c").unwrap();
    assert_eq!(e.count("test.c", &doc! {}).unwrap(), 0);
}

#[test]
fn create_and_list_indexes() {
    let e = engine();
    e.create_namespace("test.c").unwrap();
    let model = IndexModel::builder().keys(doc! { "email": 1 }).build();
    let name = e.create_index("test.c", &model).unwrap();
    assert_eq!(name, "email_1");
    let indexes = e.list_indexes("test.c").unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name, "email_1");
}

#[test]
fn upsert_creates_document_when_no_match() {
    let e = engine();
    let r = e
        .update(
            "test.c",
            &doc! { "email": "a@b.com" },
            &doc! { "$set": { "name": "Alice" } },
            &UpdateOptions { upsert: true },
            false,
        )
        .unwrap();
    assert!(r.upserted_id.is_some());
    let doc = e
        .find_one("test.c", &doc! { "email": "a@b.com" })
        .unwrap()
        .unwrap();
    assert_eq!(doc.get_str("name").unwrap(), "Alice");
}

#[test]
fn find_one_and_delete_returns_doc() {
    let e = engine();
    e.insert("test.c", doc! { "x": 42 }).unwrap();
    let d = e
        .find_one_and_delete(
            "test.c",
            &doc! { "x": 42 },
            &FindOneAndDeleteOptions::default(),
        )
        .unwrap();
    assert!(d.is_some());
    assert_eq!(e.count("test.c", &doc! {}).unwrap(), 0);
}

// -----------------------------------------------------------------------
// Buffered-mode (catalog namespace registry) tests
//
// These tests exercise PagedEngine in buffered mode, using
// an in-memory mock I/O layer so they remain hermetic and fast.
// -----------------------------------------------------------------------

use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

/// Create a buffered `PagedEngine` backed by an in-memory `MockIo`.
///
/// Returns `(engine, io)` so callers can inspect or re-use the backing store.
fn buffered_engine() -> (PagedEngine, Arc<MockIo>) {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    let engine = PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine");
    (engine, io)
}

/// Reconstruct a buffered engine by reading back the catalog root from
/// the mock I/O layer.  Simulates closing and reopening a database file.
///
/// Reads page 0 (the file header) from `io`, extracts the persisted
/// `catalog_root_page` and `catalog_root_level`, and opens a new engine.
fn reopen_engine(io: &Arc<MockIo>) -> PagedEngine {
    // Read the header page from backing store.
    let pages = io.pages.lock().unwrap();
    let hdr_bytes = pages
        .get(&0)
        .expect("header page 0 must have been flushed")
        .clone();
    drop(pages); // release lock before creating new pool

    use crate::storage::header::HEADER_PAGE_SIZE;
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    let n = buf.len().min(hdr_bytes.len());
    buf[..n].copy_from_slice(&hdr_bytes[..n]);
    let header = FileHeader::from_bytes(&buf).expect("parse header");

    let catalog_root_page = header.catalog_root_page;
    let catalog_root_level = header.catalog_root_level;

    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(io))),
    ));
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, catalog_root_page, catalog_root_level)
        .expect("reopen buffered engine")
}

// --- create_namespace wires into catalog ---

#[test]
fn buffered_create_namespace_appears_in_list() {
    let (e, _io) = buffered_engine();
    e.create_namespace("mydb.users").unwrap();
    e.create_namespace("mydb.orders").unwrap();

    let mut ns = e.list_namespaces().unwrap();
    ns.sort();
    assert_eq!(ns, ["mydb.orders", "mydb.users"]);
}

#[test]
fn buffered_create_namespace_rejects_duplicate_name() {
    let (e, _io) = buffered_engine();
    e.create_namespace("mydb.users").unwrap();
    let err = e
        .create_namespace("mydb.users")
        .expect_err("duplicate namespace create must fail");
    assert!(matches!(err, Error::DuplicateKey { .. }));
    assert_eq!(e.list_namespaces().unwrap().len(), 1);
}

#[test]
fn buffered_namespace_supports_db_dot_coll_format() {
    let (e, _io) = buffered_engine();
    // Namespace keys MUST be in 'db.collection' multi-database format.
    e.create_namespace("analytics.events").unwrap();
    e.create_namespace("billing.invoices").unwrap();

    let mut ns = e.list_namespaces().unwrap();
    ns.sort();
    assert!(ns.contains(&"analytics.events".to_owned()));
    assert!(ns.contains(&"billing.invoices".to_owned()));
}

// --- drop_namespace removes catalog entries AND frees pages ---

#[test]
fn buffered_drop_namespace_removes_from_catalog() {
    let (e, _io) = buffered_engine();
    e.create_namespace("mydb.users").unwrap();
    e.create_namespace("mydb.orders").unwrap();

    e.drop_namespace("mydb.users").unwrap();

    let ns = e.list_namespaces().unwrap();
    assert!(!ns.contains(&"mydb.users".to_owned()));
    assert!(ns.contains(&"mydb.orders".to_owned()));
}

#[test]
fn buffered_drop_namespace_frees_pages_for_reuse() {
    let (e, _io) = buffered_engine();
    e.create_namespace("mydb.users").unwrap();

    // Insert enough docs to allocate multiple leaf pages.
    for i in 0..20i32 {
        e.insert("mydb.users", doc! { "i": i }).unwrap();
    }

    // Checkpoint so allocator state is stable.
    e.checkpoint().unwrap();

    // Record total page count before drop.
    let total_before = {
        e.shared
            .handle
            .allocator()
            .with_header(|h| h.total_page_count)
            .unwrap()
    };

    e.drop_namespace("mydb.users").unwrap();

    // Free page count should have increased (pages returned to free list).
    let free_after = {
        e.shared
            .handle
            .allocator()
            .with_header(|h| h.free_page_count_32k + h.free_page_count_4k)
            .unwrap()
    };

    // After drop the free page count must be > 0 (at least the data leaf
    // and _id-index leaf were reclaimed).
    assert!(
        free_after > 0,
        "pages must be returned to free list after drop; total_before={total_before}, free_after={free_after}"
    );
}

#[test]
fn buffered_drop_nonexistent_namespace_is_ok() {
    let (e, _io) = buffered_engine();
    // Dropping a namespace that never existed must not panic or error.
    e.drop_namespace("mydb.ghost").unwrap();
}

// --- list_namespaces reads from catalog ---

#[test]
fn buffered_list_namespaces_empty_on_new_database() {
    let (e, _io) = buffered_engine();
    assert!(e.list_namespaces().unwrap().is_empty());
}

#[test]
fn buffered_list_namespaces_returns_all() {
    let (e, _io) = buffered_engine();
    for name in &["a.x", "a.y", "b.z"] {
        e.create_namespace(name).unwrap();
    }
    let mut ns = e.list_namespaces().unwrap();
    ns.sort();
    assert_eq!(ns, ["a.x", "a.y", "b.z"]);
}

// --- on-open: catalog discovery ---

#[test]
fn buffered_catalog_survives_reopen() {
    let (e, io) = buffered_engine();

    e.create_namespace("prod.users").unwrap();
    e.create_namespace("prod.orders").unwrap();
    e.insert("prod.users", doc! { "name": "Alice" }).unwrap();

    // Flush the catalog + data to the mock backing store.
    e.checkpoint().unwrap();
    drop(e);

    // Reopen using the persisted catalog root from the header.
    let e2 = reopen_engine(&io);

    // list_namespaces must discover the collections created in the first
    // engine lifetime by reading the catalog from disk.
    let mut ns = e2.list_namespaces().unwrap();
    ns.sort();
    assert_eq!(
        ns,
        ["prod.orders", "prod.users"],
        "catalog must survive close/reopen"
    );
}

#[test]
fn buffered_data_survives_reopen() {
    let (e, io) = buffered_engine();

    e.create_namespace("prod.users").unwrap();
    e.insert("prod.users", doc! { "name": "Bob", "age": 42 })
        .unwrap();
    e.checkpoint().unwrap();
    drop(e);

    let e2 = reopen_engine(&io);
    let found = e2.find_one("prod.users", &doc! { "name": "Bob" }).unwrap();
    assert!(
        found.is_some(),
        "document inserted before checkpoint must be visible after reopen"
    );
    assert_eq!(found.unwrap().get_i32("age").unwrap(), 42);
}

#[test]
fn buffered_drop_and_create_reuses_pages() {
    let (e, _io) = buffered_engine();

    e.create_namespace("test.c").unwrap();
    for i in 0..10i32 {
        e.insert("test.c", doc! { "i": i }).unwrap();
    }
    e.checkpoint().unwrap();

    let page_count_after_create = {
        e.shared
            .handle
            .allocator()
            .with_header(|h| h.total_page_count)
            .unwrap()
    };

    e.drop_namespace("test.c").unwrap();

    // Create the namespace again and insert the same data.
    e.create_namespace("test.c").unwrap();
    for i in 0..10i32 {
        e.insert("test.c", doc! { "i": i }).unwrap();
    }
    e.checkpoint().unwrap();

    let page_count_after_recreate = {
        e.shared
            .handle
            .allocator()
            .with_header(|h| h.total_page_count)
            .unwrap()
    };

    // After drop + recreate, pages should be recycled — total page count
    // must not keep growing without bound.
    assert!(
        page_count_after_recreate <= page_count_after_create + 4,
        "pages should be recycled after drop; before={page_count_after_create} after={page_count_after_recreate}"
    );
}

// -----------------------------------------------------------------------
// Secondary index maintenance + index scan tests (buffered mode)
// -----------------------------------------------------------------------

/// Verify that `create_index` builds the secondary B+ tree from existing
/// documents ("online" index build).
#[test]
fn buffered_create_index_builds_from_existing_docs() {
    let (e, _io) = buffered_engine();

    // Insert documents BEFORE creating the index.
    e.insert("test.items", doc! { "sku": "A", "price": 10i32 })
        .unwrap();
    e.insert("test.items", doc! { "sku": "B", "price": 20i32 })
        .unwrap();
    e.insert("test.items", doc! { "sku": "C", "price": 30i32 })
        .unwrap();

    // Create an index on "sku".
    let idx = IndexModel::builder().keys(doc! { "sku": 1 }).build();
    let name = e.create_index("test.items", &idx).unwrap();
    assert_eq!(name, "sku_1");

    // Query using the indexed field; must return the correct document.
    let found = e
        .find_one("test.items", &doc! { "sku": "B" })
        .unwrap()
        .expect("document B must be found via index");
    assert_eq!(found.get_i32("price").unwrap(), 20);
}

/// Verify that the index is maintained when new documents are inserted
/// after the index was created.
#[test]
fn buffered_index_maintained_on_insert() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "email": 1 }).build();
    e.create_index("test.users", &idx).unwrap();

    // Insert after index creation.
    e.insert(
        "test.users",
        doc! { "email": "alice@test.com", "role": "admin" },
    )
    .unwrap();
    e.insert(
        "test.users",
        doc! { "email": "bob@test.com", "role": "user" },
    )
    .unwrap();

    // Both documents must be found via the index.
    let alice = e
        .find_one("test.users", &doc! { "email": "alice@test.com" })
        .unwrap()
        .expect("alice must be found");
    assert_eq!(alice.get_str("role").unwrap(), "admin");

    let bob = e
        .find_one("test.users", &doc! { "email": "bob@test.com" })
        .unwrap()
        .expect("bob must be found");
    assert_eq!(bob.get_str("role").unwrap(), "user");
}

/// Verify that deleting a document removes its secondary index entry,
/// so subsequent queries no longer find it.
#[test]
fn buffered_index_maintained_on_delete() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "email": 1 }).build();
    e.create_index("test.users", &idx).unwrap();

    e.insert("test.users", doc! { "email": "charlie@test.com" })
        .unwrap();

    // Delete the document.
    let r = e
        .delete("test.users", &doc! { "email": "charlie@test.com" }, false)
        .unwrap();
    assert_eq!(r.deleted_count, 1);

    // Must not be found via index scan.
    let found = e
        .find_one("test.users", &doc! { "email": "charlie@test.com" })
        .unwrap();
    assert!(found.is_none(), "deleted doc must not be returned");
}

#[test]
fn buffered_index_maintained_on_find_one_and_delete() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "email": 1 }).build();
    e.create_index("test.users", &idx).unwrap();

    e.insert(
        "test.users",
        doc! { "email": "dana@test.com", "role": "admin" },
    )
    .unwrap();

    let deleted = e
        .find_one_and_delete(
            "test.users",
            &doc! { "email": "dana@test.com" },
            &FindOneAndDeleteOptions::default(),
        )
        .unwrap()
        .expect("indexed document should be deleted");
    assert_eq!(deleted.get_str("role").unwrap(), "admin");

    let found = e
        .find_one("test.users", &doc! { "email": "dana@test.com" })
        .unwrap();
    assert!(found.is_none(), "deleted doc must not be returned");
}

/// Verify that updating a document replaces its old secondary index entry
/// with a new one.
#[test]
fn buffered_index_maintained_on_update() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "email": 1 }).build();
    e.create_index("test.users", &idx).unwrap();

    e.insert("test.users", doc! { "email": "old@test.com" })
        .unwrap();

    // Update the indexed field.
    e.update(
        "test.users",
        &doc! { "email": "old@test.com" },
        &doc! { "$set": { "email": "new@test.com" } },
        &UpdateOptions::default(),
        false,
    )
    .unwrap();

    // Old entry must be gone.
    assert!(
        e.find_one("test.users", &doc! { "email": "old@test.com" })
            .unwrap()
            .is_none(),
        "old email must not be found after update"
    );
    // New entry must be present.
    assert!(
        e.find_one("test.users", &doc! { "email": "new@test.com" })
            .unwrap()
            .is_some(),
        "new email must be found after update"
    );
}

/// Verify that the index scan finds documents using a range condition.
#[test]
fn buffered_index_scan_range_gt() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "score": 1 }).build();
    e.create_index("test.players", &idx).unwrap();

    for i in 0i32..10 {
        e.insert("test.players", doc! { "name": format!("p{i}"), "score": i })
            .unwrap();
    }

    // Use $gt — only scores > 7 should be returned.
    let (results, _) = e
        .find(
            "test.players",
            &doc! { "score": { "$gt": 7i32 } },
            &FindOptions::default(),
        )
        .unwrap();
    assert_eq!(results.len(), 2, "scores 8 and 9 should match");
    for d in &results {
        assert!(d.get_i32("score").unwrap() > 7);
    }
}

/// Verify that the index scan handles `$in` queries correctly.
#[test]
fn buffered_index_scan_in_query() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "status": 1 }).build();
    e.create_index("test.orders", &idx).unwrap();

    e.insert("test.orders", doc! { "status": "pending", "amount": 10i32 })
        .unwrap();
    e.insert("test.orders", doc! { "status": "active",  "amount": 20i32 })
        .unwrap();
    e.insert("test.orders", doc! { "status": "closed",  "amount": 30i32 })
        .unwrap();

    let (results, _) = e
        .find(
            "test.orders",
            &doc! { "status": { "$in": ["pending", "active"] } },
            &FindOptions::default(),
        )
        .unwrap();
    assert_eq!(results.len(), 2);
    for d in &results {
        let s = d.get_str("status").unwrap();
        assert!(s == "pending" || s == "active");
    }
}

/// Verify that a unique secondary index rejects duplicate values.
#[test]
fn buffered_unique_secondary_index_rejects_duplicates() {
    let (e, _io) = buffered_engine();

    use crate::options::IndexOptions;
    let idx = IndexModel::builder()
        .keys(doc! { "email": 1 })
        .options(IndexOptions::new().unique(true))
        .build();
    e.create_index("test.users", &idx).unwrap();

    e.insert("test.users", doc! { "email": "dup@test.com" })
        .unwrap();
    let result = e.insert("test.users", doc! { "email": "dup@test.com" });
    assert!(
        matches!(result, Err(Error::DuplicateKey { .. })),
        "unique index must reject duplicate email"
    );
}

/// Verify that a compound index can be created and used for lookups.
#[test]
fn buffered_compound_index_lookup() {
    let (e, _io) = buffered_engine();

    let idx = IndexModel::builder()
        .keys(doc! { "category": 1, "price": 1 })
        .build();
    e.create_index("test.products", &idx).unwrap();

    e.insert(
        "test.products",
        doc! { "category": "books", "price": 15i32, "title": "Rust Programming" },
    )
    .unwrap();
    e.insert(
        "test.products",
        doc! { "category": "books", "price": 25i32, "title": "Database Design" },
    )
    .unwrap();
    e.insert(
        "test.products",
        doc! { "category": "tools", "price": 50i32, "title": "Hammer" },
    )
    .unwrap();

    // Equality on the leftmost field — planner selects the compound index.
    let (results, _) = e
        .find(
            "test.products",
            &doc! { "category": "books" },
            &FindOptions::default(),
        )
        .unwrap();
    assert_eq!(results.len(), 2, "two books should be found");
    for d in &results {
        assert_eq!(d.get_str("category").unwrap(), "books");
    }
}

/// Verify that an index survives a checkpoint + reopen cycle.
#[test]
fn buffered_index_survives_reopen() {
    let (e, io) = buffered_engine();

    let idx = IndexModel::builder().keys(doc! { "username": 1 }).build();
    e.create_index("test.accounts", &idx).unwrap();

    e.insert("test.accounts", doc! { "username": "alice" })
        .unwrap();
    e.insert("test.accounts", doc! { "username": "bob" })
        .unwrap();

    e.checkpoint().unwrap();
    drop(e);

    let e2 = reopen_engine(&io);

    // After reopen, index scan must still work.
    let found = e2
        .find_one("test.accounts", &doc! { "username": "alice" })
        .unwrap();
    assert!(
        found.is_some(),
        "alice must be found via index after reopen"
    );
}

#[test]
fn buffered_index_checkpoint_persists_materialized_split_root() {
    const DOC_COUNT: i32 = 240;
    const TARGET_INDEX: i32 = DOC_COUNT - 1;
    const KEY_PAD_LEN: usize = 256;

    let (e, io) = buffered_engine();
    let idx = IndexModel::builder().keys(doc! { "username": 1 }).build();
    e.create_index("test.accounts", &idx).unwrap();

    let padding = "x".repeat(KEY_PAD_LEN);
    for i in 0..DOC_COUNT {
        e.insert(
            "test.accounts",
            doc! { "username": format!("user-{i:04}-{padding}") },
        )
        .unwrap();
    }

    e.checkpoint().unwrap();
    drop(e);

    let e2 = reopen_engine(&io);
    let target = format!("user-{TARGET_INDEX:04}-{padding}");
    let found = e2
        .find_one("test.accounts", &doc! { "username": target })
        .unwrap();
    assert!(
        found.is_some(),
        "right-side secondary index entry must survive checkpoint root split"
    );
}

// -----------------------------------------------------------------------
// SWMR concurrency tests
//
// Verify that multiple concurrent readers do not block each other, and
// that readers run concurrently with writers (writers take an exclusive
// write lock; readers take a shared read lock).
// -----------------------------------------------------------------------

/// Verify that many concurrent reader threads can all see committed data
/// without blocking each other.
#[test]
fn swmr_concurrent_readers_do_not_block() {
    use std::sync::Arc;
    use std::thread;

    let e = Arc::new(engine());
    // Insert documents under the single writer lock.
    for i in 0..20i32 {
        e.insert("test.c", doc! { "i": i }).unwrap();
    }

    // Spawn many reader threads that all query concurrently.
    let handles: Vec<_> = (0..16)
        .map(|_| {
            let e = Arc::clone(&e);
            thread::spawn(move || {
                let opts = FindOptions::default();
                let (docs, _) = e.find("test.c", &doc! {}, &opts).unwrap();
                assert_eq!(
                    docs.len(),
                    20,
                    "all 20 docs must be visible to every reader"
                );
            })
        })
        .collect();

    for h in handles {
        h.join().expect("reader thread panicked");
    }
}

/// Verify that a reader can observe a consistent snapshot while a
/// concurrent writer is modifying the collection.
///
/// Readers load a `PublishedEpoch` without taking the engine mutex.
/// A reader that loaded the snapshot before the writer commits will still
/// see the pre-write state because `publish_ts` pins the `ReadView` at
/// that timestamp.
#[test]
fn swmr_reader_sees_snapshot_isolation() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let e = Arc::new(engine());
    // Insert an initial document.
    e.insert("test.snap", doc! { "status": "before" }).unwrap();

    // Barrier: reader loads snapshot, signals writer; writer commits,
    // then signals reader to finish.
    let barrier = Arc::new(Barrier::new(2));

    let e_reader = Arc::clone(&e);
    let barrier_reader = Arc::clone(&barrier);
    let reader = thread::spawn(move || {
        // Load the published snapshot BEFORE the writer commits.
        let snap = e_reader.shared.published.load_full();
        let publish_ts = snap.visible_ts;

        // Tell the writer we have our snapshot.
        barrier_reader.wait();

        // Scan using the snapshot's root pages and publish_ts (no mutex).
        let matched = if let Some(ns_snap) = snap.catalog.get_by_name("test.snap") {
            let store = BufferPoolPageStore::new(Arc::clone(&e_reader.shared.handle));
            let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
            let txn_id = e_reader.shared.txn_counter.fetch_add(1, Ordering::Relaxed);
            let view = ReadView::open(
                Arc::clone(e_reader.shared.handle.read_view_registry()),
                publish_ts,
                txn_id,
            );
            btree_collscan(&tree, &doc! {}, &view, None).unwrap()
        } else {
            Vec::new()
        };
        matched
    });

    // Writer: wait for the reader to capture its snapshot, then write.
    barrier.wait();
    e.insert("test.snap", doc! { "status": "after" }).unwrap();

    let matched = reader.join().expect("reader panicked");
    // The reader's snapshot was taken before the write, so it sees exactly 1 doc.
    assert_eq!(
        matched.len(),
        1,
        "reader must see snapshot before writer committed"
    );
}

/// Verify that the in-process writer lock (in client.rs) respects the
/// busy_timeout: concurrent writers should queue up and eventually all
/// succeed (or get WriterBusy on zero-timeout paths).
///
/// This test uses the PagedEngine directly (not through Client) so it
/// only exercises the RwLock inside the engine, not the client-level
/// writer_lock.  Engine-level writes are serialized by the write-lock.
#[test]
fn swmr_concurrent_writers_serialize() {
    use std::sync::Arc;
    use std::thread;

    let e = Arc::new(engine());

    // Spawn 8 writer threads — each inserts 10 documents.
    let handles: Vec<_> = (0..8u32)
        .map(|worker| {
            let e = Arc::clone(&e);
            thread::spawn(move || {
                for j in 0..10u32 {
                    e.insert(
                        "test.concurrent",
                        doc! { "worker": worker as i32, "j": j as i32 },
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("writer thread panicked");
    }

    // After all writers complete, total doc count must be 8 * 10 = 80.
    let count = e.count("test.concurrent", &doc! {}).unwrap();
    assert_eq!(
        count, 80,
        "all 80 documents must be present after concurrent writes"
    );
}

// ---------------------------------------------------------------------------
// Phase 1 §10.2 / US-005 — publish_commit unit tests
// ---------------------------------------------------------------------------

/// §10.2 / US-005: calling `publish_commit` with `published_catalog_dirty=false`
/// reuses the previous `Arc<PublishedCatalog>` — same allocation, testable
/// with `Arc::ptr_eq`.
#[test]
fn publish_commit_root_neutral_reuses_catalog_arc() {
    use super::publish::{publish_commit, PublishDirty};

    let (e, _io) = buffered_engine();
    e.create_namespace("test.ptr_eq").unwrap();

    // Capture the current epoch.
    let prev = e.shared.published.load_full();
    let prev_catalog_arc = Arc::clone(&prev.catalog);

    // Force a publish with no published-catalog dirty. `publish_commit`
    // must clone the previous catalog Arc rather than build a new one.
    let _md_r = e.metadata.read().unwrap();
    let next_ts = e.shared.oracle.commit().unwrap();
    let new_epoch = {
        let cat = e.metadata_state.catalog.lock().unwrap();
        publish_commit(&e.shared, &cat, next_ts, PublishDirty::default(), None).unwrap()
    };

    assert!(
        Arc::ptr_eq(&prev_catalog_arc, &new_epoch.catalog),
        "root-neutral publish must reuse the previous catalog Arc (Phase 1 §4.2)"
    );
    assert!(
        new_epoch.visible_ts > prev.visible_ts,
        "visible_ts must advance on every publish"
    );
}

/// §10.2 / US-005: calling `publish_commit` with `published_catalog_dirty=true`
/// always builds a fresh `Arc<PublishedCatalog>` — the new epoch's catalog
/// must be a distinct allocation from the prior epoch's catalog.
#[test]
fn publish_commit_dirty_path_builds_new_catalog_arc() {
    use super::publish::{publish_commit, PublishDirty};

    let (e, _io) = buffered_engine();
    e.create_namespace("test.rebuild").unwrap();

    let prev = e.shared.published.load_full();
    let prev_catalog_arc = Arc::clone(&prev.catalog);

    let _md_r = e.metadata.read().unwrap();
    let next_ts = e.shared.oracle.commit().unwrap();
    let dirty = PublishDirty {
        published_catalog_dirty: true,
        catalog_header_dirty: false,
    };
    let new_epoch = {
        let cat = e.metadata_state.catalog.lock().unwrap();
        // DDL-style publish: pass an explicit reservation so the new
        // epoch's catalog_generation advances per §10.17.1 / US-006.
        let prev_gen = e.shared.published.load_full().catalog_generation;
        publish_commit(&e.shared, &cat, next_ts, dirty, Some(prev_gen + 1)).unwrap()
    };

    assert!(
        !Arc::ptr_eq(&prev_catalog_arc, &new_epoch.catalog),
        "dirty publish must build a fresh catalog Arc (Phase 1 §4.1)"
    );
}

/// Phase 1 §6.3 / US-007: the `debug_assert!` in `publish_commit`
/// rejects a stale `visible_ts`. This guards against a regression where
/// a caller hands in `oracle.now()` and two metadata-only commits in
/// the same millisecond would land equal visible_ts. Gated on
/// `debug_assertions` because `debug_assert!` is a no-op in release.
#[cfg(debug_assertions)]
#[test]
#[should_panic(
    expected = "visible_ts must be strictly monotonic; caller must use commit_ts or oracle.commit()"
)]
fn publish_commit_rejects_stale_visible_ts() {
    use super::publish::{publish_commit, PublishDirty};

    let (e, _io) = buffered_engine();
    e.create_namespace("test.stale").unwrap();
    let prev = e.shared.published.load_full();
    // Inject a stale Ts (equal to the current one) — must panic via
    // the monotonicity debug_assert.
    let _md_r = e.metadata.read().unwrap();
    let cat = e.metadata_state.catalog.lock().unwrap();
    let _ = publish_commit(
        &e.shared,
        &cat,
        prev.visible_ts,
        PublishDirty::default(),
        None,
    );
}

// ---------------------------------------------------------------------------
// Phase 1 §10.5 / US-008 + US-009 — ReadOpScope single-load discipline
// ---------------------------------------------------------------------------

/// §10.8 #15 — `find_one` performs exactly one epoch load.
#[test]
fn find_one_performs_one_epoch_load() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.find_one").unwrap();
    e.insert("test.find_one", bson::doc! { "_id": 1, "v": "a" })
        .unwrap();

    let _scope = ReadOpScope::new(1);
    let _ = e
        .find_one("test.find_one", &bson::doc! { "_id": 1 })
        .unwrap();
}

/// §10.8 #16 — `find` (range scan) performs exactly one epoch load.
#[test]
fn find_performs_one_epoch_load() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.find_range").unwrap();
    for i in 0..5 {
        e.insert("test.find_range", bson::doc! { "_id": i, "v": i })
            .unwrap();
    }

    let _scope = ReadOpScope::new(1);
    let _ = e
        .find(
            "test.find_range",
            &bson::doc! {},
            &crate::options::FindOptions::default(),
        )
        .unwrap();
}

/// §10.8 #18 — `count` performs exactly one epoch load.
#[test]
fn count_performs_one_epoch_load() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.count_scope").unwrap();
    e.insert("test.count_scope", bson::doc! { "_id": 1 })
        .unwrap();

    let _scope = ReadOpScope::new(1);
    let _ = e.count("test.count_scope", &bson::doc! {}).unwrap();
}

/// §10.8 #15 / US-008: the missing-namespace read path also performs
/// exactly one epoch load (returns empty without re-loading).
#[test]
fn find_on_missing_namespace_performs_one_epoch_load() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    // No namespace; read must not re-load the epoch looking for one.
    let _scope = ReadOpScope::new(1);
    let _ = e
        .find(
            "test.absent",
            &bson::doc! {},
            &crate::options::FindOptions::default(),
        )
        .unwrap();
}

/// US-008 sanity check: two reads in a row consume two load_published
/// calls, but each wrapped in its own `ReadOpScope::new(1)` stays
/// within the limit.
#[test]
fn consecutive_reads_each_within_scope() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.consec").unwrap();
    e.insert("test.consec", bson::doc! { "_id": 1 }).unwrap();

    {
        let _scope = ReadOpScope::new(1);
        let _ = e.find_one("test.consec", &bson::doc! { "_id": 1 }).unwrap();
    }
    {
        let _scope = ReadOpScope::new(1);
        let _ = e.count("test.consec", &bson::doc! {}).unwrap();
    }
}

/// §10.8 #19 — `publish_commit` (the `published.store` step) runs
/// strictly AFTER `commit_txn`. Unit test (not integration) because
/// the publish-pause rendezvous hook is `#[cfg(test)]`-gated
/// (§11 #10: no new `Arc` / `Mutex` in production builds).
///
/// The test installs a 2-party `Barrier` that the writer waits on
/// between `commit_txn` and `publish_commit`. The reader observes
/// the pre-publish `PublishedEpoch` while the writer is pinned at the
/// barrier, then releases it and observes the new epoch.
#[test]
fn publish_happens_strictly_after_commit_txn() {
    use super::hidden_accessors::install_publish_pause;
    use std::sync::Barrier;

    let (engine_raw, _io) = buffered_engine();
    let engine: Arc<PagedEngine> = Arc::new(engine_raw);
    engine.create_namespace("test.rdv").unwrap();
    engine
        .insert("test.rdv", bson::doc! { "_id": 0i32 })
        .unwrap();
    let pre_ts = engine.shared.published.load_full().visible_ts;

    let gate = Arc::new(Barrier::new(2));
    let _guard = install_publish_pause(&engine.shared, Arc::clone(&gate));

    let writer_engine = Arc::clone(&engine);
    let writer = std::thread::spawn(move || {
        writer_engine
            .insert("test.rdv", bson::doc! { "_id": 1i32 })
            .unwrap();
    });

    // Spin briefly so the writer has time to reach the gate. Even if
    // it hasn't, any load before the writer publishes sees pre_ts.
    for _ in 0..200 {
        std::thread::yield_now();
    }
    let observed_paused = engine.shared.published.load_full().visible_ts;
    assert_eq!(
        observed_paused, pre_ts,
        "§10.8 #19: reader must observe pre-publish visible_ts while writer \
         is paused between commit_txn and publish_commit"
    );

    gate.wait();
    writer.join().expect("writer thread panicked");

    let post_ts = engine.shared.published.load_full().visible_ts;
    assert!(
        post_ts > pre_ts,
        "§10.8 #19: after release the published visible_ts must advance"
    );
}

// ---------------------------------------------------------------------------
// US-009 — additional ReadOpScope coverage for compound read ops
// ---------------------------------------------------------------------------

/// `update` performs one published load for filter planning and one for the
/// US-008 writer visibility context.
#[test]
fn update_read_phase_plus_write_visibility_performs_two_epoch_loads() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.u_scope").unwrap();
    e.insert("test.u_scope", bson::doc! { "_id": 1, "v": 0 })
        .unwrap();

    let _scope = ReadOpScope::new(2);
    let _ = e
        .update(
            "test.u_scope",
            &bson::doc! { "_id": 1 },
            &bson::doc! { "$set": { "v": 1 } },
            &UpdateOptions::default(),
            false,
        )
        .unwrap();
}

/// `delete` performs one published load for target selection and one for the
/// US-008 writer visibility context.
#[test]
fn delete_read_phase_plus_write_visibility_performs_two_epoch_loads() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.d_scope").unwrap();
    e.insert("test.d_scope", bson::doc! { "_id": 1 }).unwrap();
    e.insert("test.d_scope", bson::doc! { "_id": 2 }).unwrap();

    let _scope = ReadOpScope::new(2);
    let _ = e
        .delete("test.d_scope", &bson::doc! { "_id": 1 }, false)
        .unwrap();
}

/// `find_one_and_delete` performs one published load for target selection and
/// one for the US-008 writer visibility context.
#[test]
fn find_one_and_delete_read_phase_plus_write_visibility_performs_two_loads() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.foad_scope").unwrap();
    e.insert("test.foad_scope", bson::doc! { "_id": 1 })
        .unwrap();

    let _scope = ReadOpScope::new(2);
    let _ = e
        .find_one_and_delete(
            "test.foad_scope",
            &bson::doc! { "_id": 1 },
            &FindOneAndDeleteOptions::default(),
        )
        .unwrap();
}

/// `list_indexes` (src/storage/paged_engine/index_maint.rs:433-449) is
/// a pure read-path op — exactly one published load.
#[test]
fn list_indexes_performs_one_epoch_load() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.li_scope").unwrap();
    let _scope = ReadOpScope::new(1);
    let _ = e.list_indexes("test.li_scope").unwrap();
}

/// `list_namespaces` (src/storage/paged_engine.rs) — pure read path,
/// one published load.
#[test]
fn list_namespaces_performs_one_epoch_load() {
    use super::state::ReadOpScope;
    let (e, _io) = buffered_engine();
    e.create_namespace("test.ln_a").unwrap();
    e.create_namespace("test.ln_b").unwrap();

    let _scope = ReadOpScope::new(1);
    let names = <PagedEngine as StorageEngine>::list_namespaces(&e).unwrap();
    assert!(names.len() >= 2);
}

/// §10.1 — `catalog_gen` is strictly monotonic across two
/// rebuild-inducing commits and unchanged across an epoch-only
/// (root-neutral) publish. The Phase 5 §10.17.1 / §10.21 CV-5 revalidation
/// path in the sequencer depends on this contract.
#[test]
fn catalog_gen_advances_on_rebuild_and_holds_on_reuse() {
    let (e, _io) = buffered_engine();

    // Two DDL commits → each advances catalog_gen.
    let g0 = e.shared.load_published().catalog_generation;
    e.create_namespace("test.cg1").unwrap();
    let g1 = e.shared.load_published().catalog_generation;
    e.create_namespace("test.cg2").unwrap();
    let g2 = e.shared.load_published().catalog_generation;
    assert!(g1 > g0, "first DDL publish must advance catalog_gen");
    assert!(g2 > g1, "second DDL publish must advance catalog_gen");

    // Prime namespace so the next inserts are root-neutral CRUD.
    e.insert("test.cg1", bson::doc! { "_id": 0 }).unwrap();
    let g3 = e.shared.load_published().catalog_generation;
    // Root-neutral CRUD does NOT rebuild, so catalog_gen holds.
    for i in 1..=5 {
        e.insert("test.cg1", bson::doc! { "_id": i }).unwrap();
    }
    let g4 = e.shared.load_published().catalog_generation;
    assert_eq!(
        g4, g3,
        "5 root-neutral CRUD publishes must NOT advance catalog_gen"
    );
}
