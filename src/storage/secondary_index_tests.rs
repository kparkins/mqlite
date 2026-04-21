    use super::*;
    use crate::storage::btree::{BTree, MemPageStore};
    use crate::storage::catalog::IndexEntry;
    use bson::{doc, oid::ObjectId, Bson};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn fresh_tree() -> BTree<MemPageStore> {
        BTree::create(MemPageStore::new()).expect("create tree")
    }

    /// Drain a `WriteTxn`'s pending sec-index writes into `tree`, mirroring
    /// the commit-time `install_pending_sec_index` pass. Lets tests keep
    /// observable tree-state assertions after staging.
    fn install_pending<S: BTreePageStore>(
        txn: &mut WriteTxn,
        tree: &mut BTree<S>,
    ) -> Result<()> {
        let writes = std::mem::take(&mut txn.pending_sec_index);
        for w in writes {
            match w.op {
                SecIndexOp::Insert { id_bytes } => {
                    tree.insert(&w.key, &id_bytes)?;
                }
                SecIndexOp::Delete => {
                    let _ = tree.delete(&w.key)?;
                }
            }
        }
        Ok(())
    }

    /// Stage + install helper for insert tests.
    fn stage_insert<S: BTreePageStore>(
        doc: &Document,
        doc_id: &Bson,
        tree: &mut BTree<S>,
        entry: &IndexEntry,
    ) -> Result<bool> {
        let mut txn = WriteTxn::new(0);
        let r = update_index_on_insert(doc, doc_id, tree, entry, &mut txn)?;
        install_pending(&mut txn, tree)?;
        Ok(r)
    }

    /// Stage + install helper for delete tests.
    fn stage_delete<S: BTreePageStore>(
        doc: &Document,
        doc_id: &Bson,
        tree: &mut BTree<S>,
        entry: &IndexEntry,
    ) -> Result<()> {
        let mut txn = WriteTxn::new(0);
        update_index_on_delete(doc, doc_id, entry, &mut txn)?;
        install_pending(&mut txn, tree)
    }

    /// Stage + install helper for update tests.
    fn stage_update<S: BTreePageStore>(
        old_doc: &Document,
        new_doc: &Document,
        old_id: &Bson,
        new_id: &Bson,
        tree: &mut BTree<S>,
        entry: &IndexEntry,
    ) -> Result<bool> {
        let mut txn = WriteTxn::new(0);
        let r = update_index_on_update(old_doc, new_doc, old_id, new_id, tree, entry, &mut txn)?;
        install_pending(&mut txn, tree)?;
        Ok(r)
    }

    fn make_index_entry(key_pattern: Document, unique: bool, sparse: bool) -> IndexEntry {
        IndexEntry {
            name: generate_index_name(&key_pattern),
            collection: "test".into(),
            root_page: 1,
            root_level: 0,
            key_pattern,
            unique,
            sparse,
            multikey: false,
            entry_count: 0,
            state: crate::storage::catalog::IndexState::Ready,
        }
    }

    fn oid_bson() -> Bson {
        Bson::ObjectId(ObjectId::new())
    }

    // -----------------------------------------------------------------------
    // generate_index_name
    // -----------------------------------------------------------------------

    #[test]
    fn index_name_single_ascending() {
        assert_eq!(generate_index_name(&doc! { "email": 1 }), "email_1");
    }

    #[test]
    fn index_name_single_descending() {
        assert_eq!(generate_index_name(&doc! { "score": -1 }), "score_-1");
    }

    #[test]
    fn index_name_compound() {
        assert_eq!(
            generate_index_name(&doc! { "name": 1, "age": -1 }),
            "name_1_age_-1"
        );
    }

    // -----------------------------------------------------------------------
    // extract_field_value
    // -----------------------------------------------------------------------

    #[test]
    fn extract_top_level_field() {
        let doc = doc! { "email": "alice@example.com" };
        let val = extract_field_value(&doc, "email").unwrap();
        assert_eq!(val, &Bson::String("alice@example.com".into()));
    }

    #[test]
    fn extract_nested_field() {
        let doc = doc! { "address": { "city": "Vancouver" } };
        let val = extract_field_value(&doc, "address.city").unwrap();
        assert_eq!(val, &Bson::String("Vancouver".into()));
    }

    #[test]
    fn extract_missing_field_returns_none() {
        let doc = doc! { "name": "Alice" };
        assert!(extract_field_value(&doc, "email").is_none());
        assert!(extract_field_value(&doc, "address.city").is_none());
    }

    #[test]
    fn extract_missing_nested_segment_returns_none() {
        let doc = doc! { "address": "123 Main St" }; // not a document
        assert!(extract_field_value(&doc, "address.city").is_none());
    }

    // -----------------------------------------------------------------------
    // build_index_keys — single field
    // -----------------------------------------------------------------------

    #[test]
    fn single_field_key_ascending() {
        let id = Bson::Int32(42);
        let doc = doc! { "email": "alice@example.com" };
        let (keys, multikey) = build_index_keys(&doc, &doc! { "email": 1 }, &id, false).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(!multikey);
    }

    #[test]
    fn single_field_key_missing_field_uses_null() {
        let id = oid_bson();
        let doc = doc! { "name": "Alice" }; // no "score"
        let (keys, _) = build_index_keys(
            &doc,
            &doc! { "score": 1 },
            &id,
            false, // non-sparse
        )
        .unwrap();
        assert_eq!(keys.len(), 1, "non-sparse: one null entry expected");
    }

    #[test]
    fn sparse_index_skips_missing_field() {
        let id = oid_bson();
        let doc = doc! { "name": "Alice" }; // no "score"
        let (keys, _) = build_index_keys(
            &doc,
            &doc! { "score": 1 },
            &id,
            true, // sparse
        )
        .unwrap();
        assert!(keys.is_empty(), "sparse: document should be skipped");
    }

    // -----------------------------------------------------------------------
    // build_index_keys — compound
    // -----------------------------------------------------------------------

    #[test]
    fn compound_key_sort_order() {
        let id = Bson::Int32(1);

        let doc_a = doc! { "name": "Alice", "age": 30 };
        let doc_b = doc! { "name": "Alice", "age": 40 };
        let doc_c = doc! { "name": "Bob",   "age": 20 };

        let pattern = doc! { "name": 1, "age": 1 };

        let (ka, _) = build_index_keys(&doc_a, &pattern, &id, false).unwrap();
        let (kb, _) = build_index_keys(&doc_b, &pattern, &id, false).unwrap();
        let (kc, _) = build_index_keys(&doc_c, &pattern, &id, false).unwrap();

        assert!(
            ka[0] < kb[0],
            "Alice/30 should sort before Alice/40 (same name, lower age)"
        );
        assert!(
            kb[0] < kc[0],
            "Alice/40 should sort before Bob/20 (name: Alice < Bob)"
        );
    }

    #[test]
    fn compound_descending_reverses_age_order() {
        let id = Bson::Int32(1);
        let doc_a = doc! { "name": "Alice", "age": 30 };
        let doc_b = doc! { "name": "Alice", "age": 40 };

        // age: -1 means higher age should sort FIRST (lower key bytes).
        let pattern = doc! { "name": 1, "age": -1 };

        let (ka, _) = build_index_keys(&doc_a, &pattern, &id, false).unwrap();
        let (kb, _) = build_index_keys(&doc_b, &pattern, &id, false).unwrap();

        assert!(
            kb[0] < ka[0],
            "age DESC: age=40 should produce a lower key than age=30"
        );
    }

    // -----------------------------------------------------------------------
    // build_index_keys — multikey (array field)
    // -----------------------------------------------------------------------

    #[test]
    fn multikey_one_entry_per_element() {
        let id = Bson::Int32(1);
        let doc = doc! { "tags": ["rust", "db", "bson"] };
        let (keys, multikey) = build_index_keys(&doc, &doc! { "tags": 1 }, &id, false).unwrap();
        assert_eq!(keys.len(), 3, "three tags → three index entries");
        assert!(multikey);
    }

    #[test]
    fn multikey_duplicate_array_elements() {
        let id = Bson::Int32(1);
        let doc = doc! { "tags": ["rust", "rust"] }; // duplicate
        let (keys, multikey) = build_index_keys(&doc, &doc! { "tags": 1 }, &id, false).unwrap();
        // Two identical keys are produced; deduplication happens at insert time.
        assert_eq!(keys.len(), 2);
        assert!(multikey);
        assert_eq!(keys[0], keys[1]); // same encoded bytes
    }

    #[test]
    fn compound_parallel_arrays_error() {
        let id = Bson::Int32(1);
        let doc = doc! { "a": [1, 2], "b": [3, 4] };
        let result = build_index_keys(&doc, &doc! { "a": 1, "b": 1 }, &id, false);
        assert!(
            matches!(result, Err(Error::Internal(_))),
            "parallel arrays in compound index should be an error"
        );
    }

    // -----------------------------------------------------------------------
    // update_index_on_insert
    // -----------------------------------------------------------------------

    #[test]
    fn insert_single_field_entry() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let doc = doc! { "_id": id.clone(), "email": "a@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, false, false);

        let multikey = stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        assert!(!multikey);

        // Verify the entry exists in the tree.
        let (keys, _) = build_index_keys(&doc, &entry.key_pattern, &id, false).unwrap();
        let found = tree.search(&keys[0]).unwrap();
        assert!(found.is_some(), "index entry should exist after insert");
    }

    #[test]
    fn insert_multikey_dedup() {
        let mut tree = fresh_tree();
        let id = Bson::Int32(1);
        let doc = doc! { "_id": id.clone(), "tags": ["rust", "rust"] };
        let entry = make_index_entry(doc! { "tags": 1 }, false, false);

        // Should succeed even though two identical keys are produced.
        let multikey = stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        assert!(multikey);
    }

    #[test]
    fn insert_sparse_skips_missing() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let doc = doc! { "_id": id.clone(), "name": "Alice" }; // no "score"
        let entry = make_index_entry(doc! { "score": 1 }, false, true /* sparse */);

        let multikey = stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        assert!(!multikey);

        // Tree should be empty.
        let all = tree.range_scan(None, None).unwrap();
        assert!(all.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unique constraint
    // -----------------------------------------------------------------------

    #[test]
    fn unique_insert_second_same_field_errors() {
        let mut tree = fresh_tree();
        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());

        let doc1 = doc! { "_id": id1.clone(), "email": "dup@test.com" };
        let doc2 = doc! { "_id": id2.clone(), "email": "dup@test.com" };

        let entry = make_index_entry(doc! { "email": 1 }, true /* unique */, false);

        stage_insert(&doc1, &id1, &mut tree, &entry).unwrap();
        let result = stage_insert(&doc2, &id2, &mut tree, &entry);

        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "unique index: second document with same email should fail"
        );
    }

    #[test]
    fn unique_insert_different_field_succeeds() {
        let mut tree = fresh_tree();
        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());

        let doc1 = doc! { "_id": id1.clone(), "email": "a@test.com" };
        let doc2 = doc! { "_id": id2.clone(), "email": "b@test.com" };

        let entry = make_index_entry(doc! { "email": 1 }, true, false);

        stage_insert(&doc1, &id1, &mut tree, &entry).unwrap();
        stage_insert(&doc2, &id2, &mut tree, &entry).unwrap(); // must succeed
    }

    #[test]
    fn unique_insert_same_id_idempotent() {
        // Inserting the same (field_value, _id) pair twice should only fail
        // on the BTree DuplicateKey at the tree level, not at the unique check.
        let mut tree = fresh_tree();
        let id = Bson::ObjectId(ObjectId::new());
        let doc = doc! { "_id": id.clone(), "email": "a@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, true, false);

        stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        // Second insert (same _id): unique check passes, BTree DuplicateKey error.
        let result = stage_insert(&doc, &id, &mut tree, &entry);
        // BTree-level duplicate, not a unique-constraint violation.
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // MVCC T5' — staging-specific behaviours (sub-step 5)
    // -----------------------------------------------------------------------

    /// In-txn conflict: two stage_insert calls with the same unique key in
    /// the same txn must be caught by the `pending_sec_index` scan in
    /// `update_index_on_insert` — the durable tree is still empty at that
    /// point so `check_unique_constraint` would otherwise pass.
    #[test]
    fn staged_unique_in_txn_conflict() {
        let tree = fresh_tree();
        let entry = make_index_entry(doc! { "email": 1 }, true /* unique */, false);
        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());
        let doc1 = doc! { "_id": id1.clone(), "email": "dup@test.com" };
        let doc2 = doc! { "_id": id2.clone(), "email": "dup@test.com" };

        let mut txn = WriteTxn::new(0);
        update_index_on_insert(&doc1, &id1, &tree, &entry, &mut txn)
            .expect("first insert stages cleanly");
        let result = update_index_on_insert(&doc2, &id2, &tree, &entry, &mut txn);
        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "second staged insert with same unique key must fail as in-txn conflict"
        );
    }

    /// Multikey dedupe happens at stage time via `HashSet`: duplicate array
    /// elements (e.g. `tags: ["rust", "rust"]`) produce a single
    /// `SecIndexWrite` in `pending_sec_index`, not two.
    #[test]
    fn staged_multikey_dedupe_single_pending_entry() {
        let tree = fresh_tree();
        let entry = make_index_entry(doc! { "tags": 1 }, false, false);
        let id = Bson::Int32(1);
        let doc = doc! { "_id": id.clone(), "tags": ["rust", "rust", "db"] };

        let mut txn = WriteTxn::new(0);
        let is_multikey = update_index_on_insert(&doc, &id, &tree, &entry, &mut txn)
            .expect("stage multikey insert");
        assert!(is_multikey);
        assert_eq!(
            txn.pending_sec_index.len(),
            2,
            "three array elems with one dup should yield two staged writes (rust, db)",
        );
        // All staged ops are Inserts targeting this index's root_page.
        for w in &txn.pending_sec_index {
            assert_eq!(w.index_root_page, entry.root_page);
            assert!(matches!(w.op, SecIndexOp::Insert { .. }));
        }
    }

    // -----------------------------------------------------------------------
    // update_index_on_delete
    // -----------------------------------------------------------------------

    #[test]
    fn delete_removes_entry() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let doc = doc! { "_id": id.clone(), "email": "del@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, false, false);

        stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        stage_delete(&doc, &id, &mut tree, &entry).unwrap();

        let all = tree.range_scan(None, None).unwrap();
        assert!(all.is_empty(), "entry should be removed after delete");
    }

    #[test]
    fn delete_multikey_removes_all_array_entries() {
        let mut tree = fresh_tree();
        let id = Bson::Int32(5);
        let doc = doc! { "_id": id.clone(), "tags": ["a", "b", "c"] };
        let entry = make_index_entry(doc! { "tags": 1 }, false, false);

        stage_insert(&doc, &id, &mut tree, &entry).unwrap();

        let before = tree.range_scan(None, None).unwrap();
        assert_eq!(before.len(), 3);

        stage_delete(&doc, &id, &mut tree, &entry).unwrap();

        let after = tree.range_scan(None, None).unwrap();
        assert!(after.is_empty(), "all multikey entries should be removed");
    }

    // -----------------------------------------------------------------------
    // update_index_on_update
    // -----------------------------------------------------------------------

    #[test]
    fn update_replaces_old_entry_with_new() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let old_doc = doc! { "_id": id.clone(), "email": "old@test.com" };
        let new_doc = doc! { "_id": id.clone(), "email": "new@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, false, false);

        stage_insert(&old_doc, &id, &mut tree, &entry).unwrap();
        stage_update(&old_doc, &new_doc, &id, &id, &mut tree, &entry).unwrap();

        let all = tree.range_scan(None, None).unwrap();
        assert_eq!(all.len(), 1, "exactly one entry after update");

        // Verify the entry is for "new@test.com", not "old@test.com".
        let (old_keys, _) = build_index_keys(&old_doc, &entry.key_pattern, &id, false).unwrap();
        let (new_keys, _) = build_index_keys(&new_doc, &entry.key_pattern, &id, false).unwrap();

        assert!(
            tree.search(&old_keys[0]).unwrap().is_none(),
            "old entry should be gone"
        );
        assert!(
            tree.search(&new_keys[0]).unwrap().is_some(),
            "new entry should be present"
        );
    }

    // -----------------------------------------------------------------------
    // build_index (full scan)
    // -----------------------------------------------------------------------

    #[test]
    fn build_index_populates_from_data_tree() {
        use crate::keys::encode_key;

        let mut data_tree = fresh_tree();
        let mut idx_tree = fresh_tree();

        // Insert 3 documents into the data tree (simulates primary storage).
        let docs = vec![
            (
                Bson::ObjectId(ObjectId::new()),
                doc! { "email": "alice@test.com", "score": 10 },
            ),
            (
                Bson::ObjectId(ObjectId::new()),
                doc! { "email": "bob@test.com",   "score": 20 },
            ),
            (
                Bson::ObjectId(ObjectId::new()),
                doc! { "email": "carol@test.com", "score": 15 },
            ),
        ];

        for (id, mut doc) in docs.clone() {
            doc.insert("_id", id.clone());
            let key = encode_key(&id);
            let value = bson::to_vec(&doc).unwrap();
            data_tree.insert(&key, &value).unwrap();
        }

        let index_entry = make_index_entry(doc! { "email": 1 }, false, false);
        let any_multikey = build_index(&data_tree, &mut idx_tree, &index_entry).unwrap();
        assert!(!any_multikey);

        let all_idx = idx_tree.range_scan(None, None).unwrap();
        assert_eq!(all_idx.len(), 3, "three documents → three index entries");
    }

    #[test]
    fn build_index_detects_multikey() {
        use crate::keys::encode_key;

        let mut data_tree = fresh_tree();
        let mut idx_tree = fresh_tree();

        let id = Bson::ObjectId(ObjectId::new());
        let mut doc = doc! { "tags": ["rust", "db"] };
        doc.insert("_id", id.clone());

        let key = encode_key(&id);
        let value = bson::to_vec(&doc).unwrap();
        data_tree.insert(&key, &value).unwrap();

        let index_entry = make_index_entry(doc! { "tags": 1 }, false, false);
        let any_multikey = build_index(&data_tree, &mut idx_tree, &index_entry).unwrap();
        assert!(
            any_multikey,
            "document with array field should trigger multikey"
        );

        let all_idx = idx_tree.range_scan(None, None).unwrap();
        assert_eq!(all_idx.len(), 2, "two tags → two index entries");
    }

    #[test]
    fn build_index_unique_detects_duplicate() {
        use crate::keys::encode_key;

        let mut data_tree = fresh_tree();
        let mut idx_tree = fresh_tree();

        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());

        let mut doc1 = doc! { "email": "dup@test.com" };
        doc1.insert("_id", id1.clone());
        let mut doc2 = doc! { "email": "dup@test.com" };
        doc2.insert("_id", id2.clone());

        data_tree
            .insert(&encode_key(&id1), &bson::to_vec(&doc1).unwrap())
            .unwrap();
        data_tree
            .insert(&encode_key(&id2), &bson::to_vec(&doc2).unwrap())
            .unwrap();

        let index_entry = make_index_entry(doc! { "email": 1 }, true /* unique */, false);
        let result = build_index(&data_tree, &mut idx_tree, &index_entry);
        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "build_index on data with duplicate emails must error on unique index"
        );
    }

    // -----------------------------------------------------------------------
    // Prefix query semantics (validates unique-range logic)
    // -----------------------------------------------------------------------

    #[test]
    fn index_supports_prefix_query() {
        // Verify that a range scan with a prefix of the secondary key returns
        // only entries for that prefix.
        let mut tree = fresh_tree();
        let entry = make_index_entry(doc! { "score": 1 }, false, false);

        let id1 = Bson::Int32(1);
        let id2 = Bson::Int32(2);
        let id3 = Bson::Int32(3);

        let doc1 = doc! { "_id": id1.clone(), "score": 100 };
        let doc2 = doc! { "_id": id2.clone(), "score": 100 };
        let doc3 = doc! { "_id": id3.clone(), "score": 200 };

        stage_insert(&doc1, &id1, &mut tree, &entry).unwrap();
        stage_insert(&doc2, &id2, &mut tree, &entry).unwrap();
        stage_insert(&doc3, &id3, &mut tree, &entry).unwrap();

        // Use unique_range to scan all entries for score=100.
        let score_100 = Bson::Int32(100);
        let fv = [(&score_100, true)];
        let (start, end) = {
            use crate::keys::encode_compound_key;
            let mut s = encode_compound_key(&fv);
            s.push(COMPOUND_SEP);
            let mut e = s.clone();
            *e.last_mut().unwrap() += 1;
            (s, e)
        };

        let results = tree.range_scan(Some(&start), Some(&end)).unwrap();
        assert_eq!(results.len(), 2, "two docs with score=100 should be found");
    }
