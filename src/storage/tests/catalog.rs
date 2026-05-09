use super::*;
use crate::storage::btree::MemPageStore;
use bson::doc;

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn make_catalog() -> Catalog<MemPageStore> {
    Catalog::create(MemPageStore::new()).expect("create catalog")
}

fn index_model(keys: Document) -> IndexModel {
    IndexModel::builder().keys(keys).build()
}

/// Phase 1 §10.7 — allocate a durable namespace id and create a
/// collection in one call. Used by tests that don't need to observe
/// the allocated id directly.
fn create_coll(cat: &mut Catalog<MemPageStore>, name: &str) -> Result<u32> {
    let id = cat.allocate_namespace_id();
    cat.create_collection(name, id, doc! {}, now())
}

/// Phase 1 §10.7 — allocate a durable index id and create an index.
fn create_idx(
    cat: &mut Catalog<MemPageStore>,
    collection: &str,
    keys: Document,
    index_name: &str,
) -> Result<u32> {
    let id = cat.allocate_index_id();
    cat.create_index(collection, id, &index_model(keys), index_name)
}

// -----------------------------------------------------------------------
// Key encoding
// -----------------------------------------------------------------------

#[test]
fn collection_key_has_prefix_0x01() {
    let k = collection_key("users");
    assert_eq!(k[0], KEY_TYPE_COLLECTION);
    assert_eq!(&k[1..], b"users");
}

#[test]
fn index_key_has_prefix_0x02_and_null_sep() {
    let k = index_key("users", "email_1");
    assert_eq!(k[0], KEY_TYPE_INDEX);
    let sep_pos = 1 + b"users".len();
    assert_eq!(k[sep_pos], INDEX_KEY_SEP);
    assert_eq!(&k[sep_pos + 1..], b"email_1");
}

#[test]
fn index_keys_sort_after_collection_keys() {
    let ck = collection_key("zzzz");
    let ik = index_key("aaaa", "_id_");
    // 0x02 > 0x01 → index keys always sort after collection keys
    assert!(ik > ck, "index keys must sort after collection keys");
}

#[test]
fn index_keys_for_same_collection_group_together() {
    let k1 = index_key("users", "_id_");
    let k2 = index_key("users", "email_1");
    let k_other = index_key("widgets", "_id_");
    assert!(k1 < k_other);
    assert!(k2 < k_other);
}

// -----------------------------------------------------------------------
// CollectionEntry round-trip
// -----------------------------------------------------------------------

#[test]
fn collection_entry_roundtrip() {
    let entry = CollectionEntry {
        id: 7,
        name: "orders".to_owned(),
        data_root_page: 42,
        data_root_level: 1,
        document_count: 1000,
        avg_doc_size: 256,
        created_at: now(),
        options: doc! {},
    };
    let bytes = entry.to_bson_bytes().unwrap();
    let decoded = CollectionEntry::from_bson_bytes(&bytes).unwrap();
    assert_eq!(decoded, entry);
    assert_eq!(decoded.id, 7);
}

// -----------------------------------------------------------------------
// IndexEntry round-trip
// -----------------------------------------------------------------------

#[test]
fn index_entry_roundtrip() {
    let entry = IndexEntry {
        id: 11,
        name: "email_1".to_owned(),
        collection: "users".to_owned(),
        root_page: 99,
        root_level: 0,
        key_pattern: doc! { "email": 1 },
        unique: true,
        sparse: false,
        multikey: false,
        entry_count: 5000,
        state: IndexState::Ready,
    };
    let bytes = entry.to_bson_bytes().unwrap();
    let decoded = IndexEntry::from_bson_bytes(&bytes).unwrap();
    assert_eq!(decoded, entry);
    assert_eq!(decoded.id, 11);
}

// -----------------------------------------------------------------------
// Durable id allocation (Phase 1 §10.7, US-002)
// -----------------------------------------------------------------------

#[test]
fn allocate_namespace_id_is_monotonic_starting_at_one() {
    let mut cat = make_catalog();
    assert_eq!(cat.allocate_namespace_id(), 1);
    assert_eq!(cat.allocate_namespace_id(), 2);
    assert_eq!(cat.allocate_namespace_id(), 3);
}

#[test]
fn allocate_index_id_is_monotonic_starting_at_one() {
    let mut cat = make_catalog();
    assert_eq!(cat.allocate_index_id(), 1);
    assert_eq!(cat.allocate_index_id(), 2);
    assert_eq!(cat.allocate_index_id(), 3);
}

#[test]
fn allocate_ns_and_index_counters_are_independent() {
    let mut cat = make_catalog();
    assert_eq!(cat.allocate_namespace_id(), 1);
    assert_eq!(cat.allocate_index_id(), 1);
    assert_eq!(cat.allocate_namespace_id(), 2);
    assert_eq!(cat.allocate_index_id(), 2);
}

#[test]
fn created_collection_carries_monotonic_id() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "alpha").unwrap();
    create_coll(&mut cat, "beta").unwrap();
    let a = cat.get_collection("alpha").unwrap().unwrap();
    let b = cat.get_collection("beta").unwrap().unwrap();
    assert!(a.id >= 1);
    assert!(b.id > a.id);
}

#[test]
fn created_index_carries_monotonic_id() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    create_idx(&mut cat, "users", doc! { "age": 1 }, "age_1").unwrap();
    let e = cat.get_index("users", "email_1").unwrap().unwrap();
    let a = cat.get_index("users", "age_1").unwrap().unwrap();
    assert!(e.id >= 1);
    assert!(a.id > e.id);
}

// -----------------------------------------------------------------------
// find_collection_by_id / find_index_by_id (Phase 1 §10.7, US-003)
// -----------------------------------------------------------------------

#[test]
fn find_collection_by_id_returns_entry_for_present_id() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "alpha").unwrap();
    create_coll(&mut cat, "beta").unwrap();
    create_coll(&mut cat, "gamma").unwrap();
    let a = cat.get_collection("alpha").unwrap().unwrap();
    let found = cat.find_collection_by_id(a.id).unwrap().unwrap();
    assert_eq!(found.name, "alpha");
    assert_eq!(found.id, a.id);
}

#[test]
fn find_collection_by_id_returns_none_for_reserved_zero() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "alpha").unwrap();
    assert!(cat.find_collection_by_id(0).unwrap().is_none());
}

#[test]
fn find_collection_by_id_returns_none_for_missing_id() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "alpha").unwrap();
    assert!(cat.find_collection_by_id(i64::MAX).unwrap().is_none());
}

#[test]
fn find_index_by_id_returns_entry_for_present_id() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    let idx = cat.get_index("users", "email_1").unwrap().unwrap();
    let (coll, found) = cat.find_index_by_id(idx.id).unwrap().unwrap();
    assert_eq!(coll.name, "users");
    assert_eq!(found.name, "email_1");
    assert_eq!(found.id, idx.id);
}

#[test]
fn find_index_by_id_returns_none_for_reserved_zero() {
    let cat = make_catalog();
    assert!(cat.find_index_by_id(0).unwrap().is_none());
}

#[test]
fn find_index_by_id_returns_none_for_missing_id() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    assert!(cat.find_index_by_id(i64::MAX).unwrap().is_none());
}

// -----------------------------------------------------------------------
// create_collection / get_collection
// -----------------------------------------------------------------------

#[test]
fn create_and_get_collection() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();

    let entry = cat.get_collection("users").unwrap().expect("should exist");
    assert_eq!(entry.name, "users");
    assert_eq!(entry.document_count, 0);
}

#[test]
fn create_collection_allocates_data_root_page() {
    let mut cat = make_catalog();
    let data_page = create_coll(&mut cat, "users").unwrap();
    assert!(data_page > 0, "data root page must be > 0");
}

#[test]
fn create_collection_does_not_create_id_index_entry() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();

    let idx = cat.get_index("users", "_id_").unwrap();
    assert!(idx.is_none(), "_id_ index must not exist");
}

#[test]
fn create_collection_duplicate_returns_error() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    let result = create_coll(&mut cat, "users");
    assert!(matches!(result, Err(Error::DuplicateKey { .. })));
}

// -----------------------------------------------------------------------
// list_collections
// -----------------------------------------------------------------------

#[test]
fn list_collections_empty() {
    let cat = make_catalog();
    assert!(cat.list_collections().unwrap().is_empty());
}

#[test]
fn list_collections_returns_all() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "alpha").unwrap();
    create_coll(&mut cat, "beta").unwrap();
    create_coll(&mut cat, "gamma").unwrap();

    let names: Vec<String> = cat
        .list_collections()
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["alpha", "beta", "gamma"]);
}

// -----------------------------------------------------------------------
// drop_collection
// -----------------------------------------------------------------------

#[test]
fn drop_collection_removes_collection_and_indexes() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();

    let removed = cat.drop_collection("users").unwrap();
    assert!(removed);

    assert!(cat.get_collection("users").unwrap().is_none());
    assert!(cat.list_indexes("users").unwrap().is_empty());
}

#[test]
fn drop_collection_nonexistent_returns_false() {
    let mut cat = make_catalog();
    assert!(!cat.drop_collection("nonexistent").unwrap());
}

// -----------------------------------------------------------------------
// create_index / list_indexes / drop_index
// -----------------------------------------------------------------------

#[test]
fn create_index_requires_collection() {
    let mut cat = make_catalog();
    let result = create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1");
    assert!(matches!(result, Err(Error::CollectionNotFound { .. })));
}

#[test]
fn create_index_allocates_root_page() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    let page = create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    assert!(page > 0);
}

#[test]
fn create_index_duplicate_returns_error() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    let result = create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1");
    assert!(matches!(result, Err(Error::DuplicateKey { .. })));
}

#[test]
fn list_indexes_returns_only_user_indexes() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    create_idx(&mut cat, "users", doc! { "age": -1 }, "age_-1").unwrap();

    let names: Vec<String> = cat
        .list_indexes("users")
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert!(names.contains(&"email_1".to_owned()));
    assert!(names.contains(&"age_-1".to_owned()));
    assert_eq!(names.len(), 2);
}

#[test]
fn list_indexes_empty_for_unknown_collection() {
    let cat = make_catalog();
    assert!(cat.list_indexes("ghost").unwrap().is_empty());
}

#[test]
fn indexes_from_different_collections_dont_leak() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_coll(&mut cat, "orders").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    create_idx(&mut cat, "orders", doc! { "total": 1 }, "total_1").unwrap();

    let user_idxs = cat.list_indexes("users").unwrap();
    assert!(user_idxs.iter().all(|e| e.collection == "users"));

    let order_idxs = cat.list_indexes("orders").unwrap();
    assert!(order_idxs.iter().all(|e| e.collection == "orders"));
}

#[test]
fn drop_index_removes_only_target_index() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    create_idx(&mut cat, "users", doc! { "age": 1 }, "age_1").unwrap();

    let removed = cat.drop_index("users", "email_1").unwrap();
    assert!(removed);

    assert!(cat.get_index("users", "email_1").unwrap().is_none());
    assert!(cat.get_index("users", "age_1").unwrap().is_some());
}

#[test]
fn drop_index_nonexistent_returns_false() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    assert!(!cat.drop_index("users", "ghost").unwrap());
}

// -----------------------------------------------------------------------
// update_collection / update_index
// -----------------------------------------------------------------------

#[test]
fn update_collection_changes_document_count() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    let mut entry = cat.get_collection("users").unwrap().unwrap();
    entry.document_count = 42;
    let updated = cat.update_collection(&entry).unwrap();
    assert!(updated);

    let fetched = cat.get_collection("users").unwrap().unwrap();
    assert_eq!(fetched.document_count, 42);
}

#[test]
fn update_index_changes_entry_count() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    create_idx(&mut cat, "users", doc! { "email": 1 }, "email_1").unwrap();
    let mut entry = cat.get_index("users", "email_1").unwrap().unwrap();
    entry.entry_count = 777;
    let updated = cat.update_index(&entry).unwrap();
    assert!(updated);

    let fetched = cat.get_index("users", "email_1").unwrap().unwrap();
    assert_eq!(fetched.entry_count, 777);
}

// -----------------------------------------------------------------------
// Root page tracking
// -----------------------------------------------------------------------

#[test]
fn root_page_is_nonzero_after_create() {
    let cat = make_catalog();
    assert!(cat.root_page() > 0);
}

#[test]
fn root_page_may_change_after_inserts() {
    let mut cat = make_catalog();
    let initial_root = cat.root_page();
    // Insert enough collections to potentially trigger a root split.
    // A single leaf page holds dozens of entries; 30 should be enough.
    for i in 0..30 {
        create_coll(&mut cat, &format!("coll_{i:03}")).unwrap();
    }
    // We don't assert a specific root page; just that the method is accessible.
    let _ = cat.root_page();
    let _ = initial_root;
}

// -----------------------------------------------------------------------
// open_with_fallback
// -----------------------------------------------------------------------

#[test]
fn open_with_fallback_new_db_creates_empty_catalog() {
    let store = MemPageStore::new();
    let (cat, used_backup) = open_with_fallback(store, 0, 0, 0, 0, 1, 1, |_| true).unwrap();
    assert!(!used_backup);
    assert!(cat.list_collections().unwrap().is_empty());
}

#[test]
fn open_with_fallback_uses_backup_when_primary_fails() {
    // Build a real catalog first to get a valid root page.
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    let backup_root = cat.root_page();
    let backup_level = cat.root_level();

    // Simulate: primary root is corrupt (page checker returns false),
    // backup root is healthy.
    let store = MemPageStore::new();
    let corrupt_primary = 999u32;
    let (opened, used_backup) = open_with_fallback(
        store,
        corrupt_primary,
        0,
        backup_root,
        backup_level,
        1,
        1,
        |page| page != corrupt_primary,
    )
    .unwrap();
    assert!(used_backup, "should have fallen back to backup");
    assert_eq!(opened.root_page(), backup_root);
}

#[test]
fn open_with_fallback_both_corrupt_returns_error() {
    let store = MemPageStore::new();
    let result = open_with_fallback(store, 1, 0, 2, 0, 1, 1, |_| false);
    assert!(matches!(result, Err(Error::CorruptDatabase { .. })));
}

// -----------------------------------------------------------------------
// Catalog hardening: multiple collections + drop stress
// -----------------------------------------------------------------------

#[test]
fn create_drop_create_same_collection() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "users").unwrap();
    cat.drop_collection("users").unwrap();
    // Must succeed (not duplicate-key) after drop.
    create_coll(&mut cat, "users").unwrap();
    assert!(cat.get_collection("users").unwrap().is_some());
}

#[test]
fn dropping_one_collection_leaves_others_intact() {
    let mut cat = make_catalog();
    create_coll(&mut cat, "alpha").unwrap();
    create_coll(&mut cat, "beta").unwrap();
    create_coll(&mut cat, "gamma").unwrap();

    cat.drop_collection("beta").unwrap();

    let names: Vec<String> = cat
        .list_collections()
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, ["alpha", "gamma"]);
}
