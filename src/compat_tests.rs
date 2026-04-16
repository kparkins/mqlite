//! Native API compatibility and persistence tests.
//!
//! Implements the 6 test suites specified in `integration.md` Phase A:
//!
//! 1. `insert_many` behavioral contract — ordered and unordered modes with
//!    unique-index violation at `doc[2]`.
//! 2. `find_one_and_update` behavioral contract — pre/post modification
//!    return semantics.
//! 3. Upsert behavioral contract — `update_one` with `upsert: true` on an
//!    empty collection.
//! 4. Persistence round-trip test — open, insert 1000 docs, create an email
//!    index, close, reopen, assert count / index / query integrity.
//! 5. Index-vs-scan consistency — `$ne` operator (the only operator from the
//!    required set that the planner always collapses to COLLSCAN, regardless
//!    of whether an index exists).
//! 6. Error code verification — each error condition returns the MongoDB-
//!    compatible error code defined in `error::codes`.
//!
//! Tests 1–3 and 5–6 use a `tempfile`-backed `Client` for speed.
//! Test 4 uses a real on-disk file via `tempfile`.
//!
//! # Organisational note
//!
//! Index-vs-scan consistency for index-eligible operators ($eq, $gt, $gte,
//! $lt, $lte, $in, $all, $elemMatch, $regex) is already covered by the
//! engine-level tests in `src/engine.rs`.  This module adds the `$ne` case
//! (always COLLSCAN, with or without an index) and verifies consistency
//! through the public `Client`/`Database`/`Collection` API.

#[cfg(test)]
mod tests {
    use crate::{
        client::Client,
        doc,
        error::{codes, Error},
        options::{FindOneAndUpdateOptions, InsertManyOptions, ReturnDocument, UpdateOptions},
        IndexModel, IndexOptions,
    };
    use bson::{Bson, Document};
    use tempfile::TempDir;

    // =========================================================================
    // Suite 1 — insert_many behavioral contract
    // =========================================================================

    /// ordered=true: 5 docs where doc[2] violates a pre-existing unique index.
    ///
    /// Asserts:
    /// - `inserted_ids` contains exactly keys 0 and 1.
    /// - `errors[0].index == 2` and `errors[0].code == 11000`.
    /// - Docs at logical positions 3 and 4 are absent from the DB (ordered
    ///   mode stops at first error).
    #[test]
    fn insert_many_ordered_behavioral_contract() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("items");

        // Create unique index on "x".
        let model = IndexModel::builder()
            .keys(doc! { "x": 1i32 })
            .options(IndexOptions::new().unique(true))
            .build()
            .unwrap();
        col.create_index(model).unwrap();

        // Pre-insert a document so doc[2] will collide.
        col.insert_one(&doc! { "x": "dup" }).unwrap();

        // Five documents; doc[2] has "x": "dup" which duplicates the index.
        let docs = vec![
            doc! { "x": "a", "label": "doc0" },
            doc! { "x": "b", "label": "doc1" },
            doc! { "x": "dup", "label": "doc2" }, // <- violates unique index
            doc! { "x": "c", "label": "doc3" },
            doc! { "x": "d", "label": "doc4" },
        ];

        let res = col
            .insert_many_with_options(&docs, InsertManyOptions::new().ordered(true))
            .unwrap();

        // Exactly docs 0 and 1 were inserted.
        assert_eq!(
            res.inserted_ids.len(),
            2,
            "ordered=true: expected 2 inserted docs, got {}",
            res.inserted_ids.len()
        );
        assert!(
            res.inserted_ids.contains_key(&0),
            "inserted_ids must contain index 0"
        );
        assert!(
            res.inserted_ids.contains_key(&1),
            "inserted_ids must contain index 1"
        );

        // Exactly one error at index 2 with DuplicateKey code.
        assert_eq!(
            res.errors.len(),
            1,
            "ordered=true: expected 1 error, got {}",
            res.errors.len()
        );
        assert_eq!(
            res.errors[0].index, 2,
            "error must be at index 2, got {}",
            res.errors[0].index
        );
        assert_eq!(
            res.errors[0].code,
            codes::DUPLICATE_KEY,
            "error code must be {} (DuplicateKey), got {}",
            codes::DUPLICATE_KEY,
            res.errors[0].code
        );

        // Docs 3 and 4 must be absent (ordered mode stopped at index 2).
        let doc3 = col.find_one(doc! { "x": "c" }).unwrap();
        let doc4 = col.find_one(doc! { "x": "d" }).unwrap();
        assert!(
            doc3.is_none(),
            "doc3 (x=c) must not be in DB after ordered stop"
        );
        assert!(
            doc4.is_none(),
            "doc4 (x=d) must not be in DB after ordered stop"
        );
    }

    /// ordered=false: same 5-doc batch with the same unique-index violation.
    ///
    /// Asserts:
    /// - Docs 0, 1, 3, and 4 are inserted.
    /// - Doc 2 is absent.
    /// - `errors` has exactly 1 entry with index 2 and code 11000.
    #[test]
    fn insert_many_unordered_behavioral_contract() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("items");

        let model = IndexModel::builder()
            .keys(doc! { "x": 1i32 })
            .options(IndexOptions::new().unique(true))
            .build()
            .unwrap();
        col.create_index(model).unwrap();

        // Pre-insert the duplicate value.
        col.insert_one(&doc! { "x": "dup" }).unwrap();

        let docs = vec![
            doc! { "x": "a", "label": "doc0" },
            doc! { "x": "b", "label": "doc1" },
            doc! { "x": "dup", "label": "doc2" }, // <- violates unique index
            doc! { "x": "c", "label": "doc3" },
            doc! { "x": "d", "label": "doc4" },
        ];

        let res = col
            .insert_many_with_options(&docs, InsertManyOptions::new().ordered(false))
            .unwrap();

        // Docs 0, 1, 3, 4 inserted; doc 2 absent.
        assert_eq!(
            res.inserted_ids.len(),
            4,
            "ordered=false: expected 4 inserted docs, got {}",
            res.inserted_ids.len()
        );
        assert!(
            res.inserted_ids.contains_key(&0),
            "index 0 must be in inserted_ids"
        );
        assert!(
            res.inserted_ids.contains_key(&1),
            "index 1 must be in inserted_ids"
        );
        assert!(
            !res.inserted_ids.contains_key(&2),
            "index 2 must NOT be in inserted_ids (it failed)"
        );
        assert!(
            res.inserted_ids.contains_key(&3),
            "index 3 must be in inserted_ids"
        );
        assert!(
            res.inserted_ids.contains_key(&4),
            "index 4 must be in inserted_ids"
        );

        // Exactly one error at index 2.
        assert_eq!(
            res.errors.len(),
            1,
            "ordered=false: expected 1 error, got {}",
            res.errors.len()
        );
        assert_eq!(res.errors[0].index, 2);
        assert_eq!(res.errors[0].code, codes::DUPLICATE_KEY);

        // Doc 2 absent, others present.
        assert!(col
            .find_one(doc! { "x": "dup", "label": "doc2" })
            .unwrap()
            .is_none());
        assert!(col.find_one(doc! { "x": "a" }).unwrap().is_some());
        assert!(col.find_one(doc! { "x": "b" }).unwrap().is_some());
        assert!(col.find_one(doc! { "x": "c" }).unwrap().is_some());
        assert!(col.find_one(doc! { "x": "d" }).unwrap().is_some());
    }

    // =========================================================================
    // Suite 2 — find_one_and_update behavioral contract
    // =========================================================================

    /// Default return policy (`Before`): returned doc reflects pre-update state;
    /// DB is updated.
    #[test]
    fn find_one_and_update_returns_pre_modification() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("docs");

        col.insert_one(&doc! { "a": 1i32 }).unwrap();

        // Default: return the document *before* the update.
        let returned: Option<Document> = col
            .find_one_and_update(doc! { "a": 1i32 }, doc! { "$set": { "a": 2i32 } })
            .unwrap();

        let returned_doc = returned.expect("must return the pre-update document");
        assert_eq!(
            returned_doc.get_i32("a").unwrap(),
            1,
            "returned doc must reflect pre-update value a=1"
        );

        // DB must now contain a=2.
        let db_doc: Option<Document> = col.find_one(doc! {}).unwrap();
        let db_doc = db_doc.expect("document must still exist in DB");
        assert_eq!(
            db_doc.get_i32("a").unwrap(),
            2,
            "DB document must be updated to a=2"
        );
    }

    /// `return_document=After`: returned doc reflects post-update state.
    #[test]
    fn find_one_and_update_return_document_after() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("docs");

        col.insert_one(&doc! { "b": 1i32 }).unwrap();

        let returned: Option<Document> = col
            .find_one_and_update_with_options(
                doc! { "b": 1i32 },
                doc! { "$set": { "b": 2i32 } },
                FindOneAndUpdateOptions::new().return_document(ReturnDocument::After),
            )
            .unwrap();

        let returned_doc = returned.expect("must return the post-update document");
        assert_eq!(
            returned_doc.get_i32("b").unwrap(),
            2,
            "with return_document=After, returned doc must have b=2"
        );
    }

    // =========================================================================
    // Suite 3 — Upsert behavioral contract
    // =========================================================================

    /// `update_one` with `upsert: true` on an empty collection inserts a new
    /// document and returns a non-null `upserted_id`.
    ///
    /// Asserts:
    /// - `upserted_id` is `Some(_)`.
    /// - A subsequent `find_one({email: "a@b.com"})` returns a document with
    ///   both `email: "a@b.com"` and `name: "Alice"`.
    #[test]
    fn upsert_behavioral_contract() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("users");

        // Collection is empty — upsert must insert.
        let res = col
            .update_one_with_options(
                doc! { "email": "a@b.com" },
                doc! { "$set": { "name": "Alice" } },
                UpdateOptions::new().upsert(true),
            )
            .unwrap();

        // upserted_id must be non-null.
        assert!(
            res.upserted_id.is_some(),
            "upsert on empty collection must set upserted_id"
        );
        assert_eq!(res.matched_count, 0, "no existing doc should have matched");
        assert_eq!(res.modified_count, 0, "no existing doc was modified");

        // find_one must return the upserted document with both fields.
        let found: Option<Document> = col.find_one(doc! { "email": "a@b.com" }).unwrap();
        let found_doc = found.expect("upserted doc must be findable by email filter");
        assert_eq!(
            found_doc.get_str("email").unwrap(),
            "a@b.com",
            "upserted doc must have email field"
        );
        assert_eq!(
            found_doc.get_str("name").unwrap(),
            "Alice",
            "upserted doc must have name field set by $set"
        );
    }

    // =========================================================================
    // Suite 4 — Persistence round-trip test (REQUIRED for Phase 1 complete)
    // =========================================================================

    /// Open a file-backed database, insert 1 000 documents, create an email
    /// index, query via the index, then close the handle and reopen from the
    /// same path.  Assert that the document count, index metadata, and
    /// indexed query results survive the reopen.
    #[test]
    fn persistence_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("round_trip.mqlite");

        // ---- Phase 1: populate -------------------------------------------
        let expected_count = 1_000u64;
        let reference_email = "user42@example.com";

        {
            let client = crate::client::Client::open(&db_path).expect("open new database");
            let db = client.database("app");
            let col = db.collection::<Document>("users");

            // Insert 1 000 documents.
            let docs: Vec<Document> = (0..expected_count as i32)
                .map(|i| {
                    doc! {
                        "email": format!("user{}@example.com", i),
                        "index": i,
                    }
                })
                .collect();
            for doc in &docs {
                col.insert_one(doc).expect("insert_one");
            }

            // Create a unique index on `email`.
            let model = IndexModel::builder()
                .keys(doc! { "email": 1i32 })
                .options(IndexOptions::new().unique(true).name("email_1".to_string()))
                .build()
                .unwrap();
            col.create_index(model).expect("create email index");

            // Verify indexed query before close.
            let before_doc: Option<Document> = col
                .find_one(doc! { "email": reference_email })
                .expect("find_one before close");
            assert!(
                before_doc.is_some(),
                "reference document must be findable before close"
            );

            // Explicit close triggers a checkpoint (snapshot write).
            db.close().expect("close database");
        }

        // ---- Phase 2: reopen and verify ----------------------------------
        {
            let client = crate::client::Client::open(&db_path).expect("reopen database");
            let db = client.database("app");
            let col = db.collection::<Document>("users");

            // Assert count == 1 000.
            let count = col.count_documents(doc! {}).expect("count_documents");
            assert_eq!(
                count, expected_count,
                "document count must survive reopen: expected {expected_count}, got {count}"
            );

            // Assert email index is present.
            let indexes = col.list_indexes().expect("list_indexes");
            let email_idx = indexes.iter().find(|idx| idx.name == "email_1");
            assert!(
                email_idx.is_some(),
                "email_1 index must survive reopen; found indexes: {:?}",
                indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
            );
            let email_idx = email_idx.unwrap();
            assert!(email_idx.unique, "email_1 index must be unique");

            // Assert indexed query returns the same document.
            let after_doc: Option<Document> = col
                .find_one(doc! { "email": reference_email })
                .expect("find_one after reopen");
            assert!(
                after_doc.is_some(),
                "reference document must be findable after reopen"
            );
            let after_doc = after_doc.unwrap();
            assert_eq!(
                after_doc.get_str("email").unwrap(),
                reference_email,
                "email field must match after reopen"
            );
            assert_eq!(
                after_doc.get_i32("index").unwrap(),
                42,
                "index field must match after reopen"
            );
        }
    }

    // =========================================================================
    // Suite 5 — Index-vs-scan consistency ($ne operator)
    // =========================================================================

    /// `$ne` is not index-eligible (the planner always chooses COLLSCAN).
    /// Verify that results are identical whether or not an index exists on the
    /// queried field.
    #[test]
    fn index_vs_scan_consistency_ne() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("scores");

        // Insert 10 documents with scores 0–9.
        for i in 0..10i32 {
            col.insert_one(&doc! { "score": i }).unwrap();
        }

        // Create an index on "score".
        let model = IndexModel::builder()
            .keys(doc! { "score": 1i32 })
            .build()
            .unwrap();
        let idx_name = col.create_index(model).unwrap();

        // Query: score != 5  →  should return 9 documents.
        let filter = doc! { "score": { "$ne": 5i32 } };

        // Run with index present (planner still picks COLLSCAN for $ne).
        let with_index: Vec<Document> = col
            .find(filter.clone())
            .unwrap()
            .collect::<crate::error::Result<_>>()
            .unwrap();

        // Drop index so the next query is definitely a COLLSCAN.
        col.drop_index(&idx_name).unwrap();

        let without_index: Vec<Document> = col
            .find(filter)
            .unwrap()
            .collect::<crate::error::Result<_>>()
            .unwrap();

        assert_eq!(
            with_index.len(),
            9,
            "$ne should return 9 documents (all except score=5)"
        );
        assert_eq!(
            without_index.len(),
            9,
            "$ne without index should also return 9 documents"
        );

        // Same document _ids in both result sets.
        let ids = |docs: &[Document]| -> std::collections::HashSet<Vec<u8>> {
            use crate::key_encoding::encode_key;
            docs.iter()
                .filter_map(|d| d.get("_id"))
                .map(encode_key)
                .collect()
        };
        assert_eq!(
            ids(&with_index),
            ids(&without_index),
            "$ne results must be identical with and without an index"
        );
    }

    // =========================================================================
    // Suite 6 — Error code verification
    // =========================================================================

    /// DuplicateKey: inserting a document that violates a unique index must
    /// return error code 11000.
    #[test]
    fn error_code_duplicate_key() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("u");

        let model = IndexModel::builder()
            .keys(doc! { "email": 1i32 })
            .options(IndexOptions::new().unique(true))
            .build()
            .unwrap();
        col.create_index(model).unwrap();

        col.insert_one(&doc! { "email": "alice@example.com" })
            .unwrap();
        let err = col
            .insert_one(&doc! { "email": "alice@example.com" })
            .unwrap_err();

        assert!(
            matches!(err, Error::DuplicateKey { .. }),
            "expected DuplicateKey, got: {:?}",
            err
        );
        assert_eq!(
            err.code(),
            Some(codes::DUPLICATE_KEY),
            "DuplicateKey must carry error code {}",
            codes::DUPLICATE_KEY
        );
    }

    /// UnsupportedOperator: using `$where` (not in Phase 1 operator set) must
    /// return error code 9.
    #[test]
    fn error_code_unsupported_operator() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("u");
        col.insert_one(&doc! { "x": 1i32 }).unwrap();

        // $where is explicitly excluded from Phase 1.
        // Use `.err().expect()` since `Cursor<T>` doesn't implement `Debug`.
        let err = col
            .find(doc! { "$where": "this.x == 1" })
            .err()
            .expect("find with $where must return Err");

        assert!(
            matches!(err, Error::UnsupportedOperator { .. }),
            "expected UnsupportedOperator, got: {:?}",
            err
        );
        assert_eq!(
            err.code(),
            Some(codes::UNSUPPORTED_OPERATOR),
            "UnsupportedOperator must carry error code {}",
            codes::UNSUPPORTED_OPERATOR
        );
    }

    /// UnsupportedIndexOption: requesting an unsupported index type (e.g.,
    /// `text`) must return error code 67.
    #[test]
    fn error_code_unsupported_index_option() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("u");

        // A text index is not supported in Phase 1.
        let model = IndexModel::builder()
            .keys(doc! { "description": "text" })
            .build()
            .unwrap();
        let err = col.create_index(model).unwrap_err();

        assert!(
            matches!(err, Error::UnsupportedIndexOption { .. }),
            "expected UnsupportedIndexOption, got: {:?}",
            err
        );
        assert_eq!(
            err.code(),
            Some(codes::CANNOT_CREATE_INDEX),
            "UnsupportedIndexOption must carry error code {}",
            codes::CANNOT_CREATE_INDEX
        );
    }

    /// DocumentTooLarge: inserting a document > 16 MiB must return error code
    /// 10334.
    #[test]
    fn error_code_document_too_large() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("u");

        // Build a document that exceeds the 16 MB limit.
        // 16 MB + 1 byte of payload in the "data" field.
        let big_string = "x".repeat(16 * 1024 * 1024 + 1);
        let big_doc = doc! { "data": big_string };

        let err = col.insert_one(&big_doc).unwrap_err();

        assert!(
            matches!(err, Error::DocumentTooLarge { .. }),
            "expected DocumentTooLarge, got: {:?}",
            err
        );
        assert_eq!(
            err.code(),
            Some(codes::DOCUMENT_TOO_LARGE),
            "DocumentTooLarge must carry error code {}",
            codes::DOCUMENT_TOO_LARGE
        );
    }

    /// SymlinkRejected: opening a path that is a symlink must return error code
    /// 2 (BAD_VALUE).
    #[test]
    #[cfg(unix)]
    fn error_code_symlink_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        std::fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        let err = crate::client::Client::open(&symlink_path)
            .err()
            .expect("opening symlink must return Err");

        assert!(
            matches!(err, Error::SymlinkRejected { .. }),
            "expected SymlinkRejected, got: {:?}",
            err
        );
        assert_eq!(
            err.code(),
            Some(codes::BAD_VALUE),
            "SymlinkRejected must carry error code {}",
            codes::BAD_VALUE
        );
    }

    /// CollectionNotFound: accessing a non-existent collection returns empty
    /// results (not an error) for `find`, but `count_documents` returns 0.
    ///
    /// This is consistent with MongoDB 8.0 behaviour: querying a missing
    /// collection is not an error.
    #[test]
    fn collection_not_found_returns_empty() {
        let _tempdir = TempDir::new().expect("tempdir");
        let client = Client::open(_tempdir.path().join("db.mqlite")).expect("open");
        let db = client.database("test");
        let col = db.collection::<Document>("nonexistent");

        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(count, 0, "empty collection count must be 0");

        let found: Option<Document> = col.find_one(doc! {}).unwrap();
        assert!(
            found.is_none(),
            "find_one on empty collection must return None"
        );
    }
}
