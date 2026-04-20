//! Document-level helpers: ids, validation, projection, sorting.

use std::time::{SystemTime, UNIX_EPOCH};

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::key_encoding::encode_key;
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::query::get_nested_field;
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::{BTree, BTreePageStore, CellValue};
use crate::storage::oid::ObjectIdGenerator;
use crate::update_operators::{apply_update, is_operator_update, upsert_base_from_filter};
use crate::validation::validate_document;

/// Return current Unix milliseconds.
pub(super) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Ensure a document has an `_id` field.  Auto-assigns an [`ObjectId`] if absent.
pub(super) fn ensure_id(doc: &mut Document) -> Bson {
    if let Some(id) = doc.get("_id") {
        id.clone()
    } else {
        let oid = Bson::ObjectId(ObjectIdGenerator::generate());
        doc.insert("_id", oid.clone());
        oid
    }
}

/// Validate that an index key pattern does not request an unsupported index type.
///
/// Rejects `text`, `2d`, `2dsphere`, and `hashed` indexes (Phase 2 features).
pub(super) fn validate_index_keys(keys: &Document) -> Result<()> {
    const SUGGESTION: &str =
        "Phase 1 supports single-field, compound, unique, sparse, and multikey \
         indexes. Text, geospatial, hashed, TTL, and partial indexes are \
         planned for a future release.";

    for (_field, value) in keys {
        let type_name: Option<&str> = match value {
            Bson::String(s) => match s.as_str() {
                "text" => Some("text"),
                "2d" => Some("2d"),
                "2dsphere" => Some("2dsphere"),
                "hashed" => Some("hashed"),
                _ => None,
            },
            _ => None,
        };
        if let Some(t) = type_name {
            return Err(Error::UnsupportedIndexOption {
                option: t.to_owned(),
                suggestion: SUGGESTION.to_owned(),
            });
        }
    }
    Ok(())
}

/// Check unique index constraints before inserting `new_doc` into `tree`.
///
/// `unique_specs` is a list of `(index_name, fields, sparse)` for each unique index.
/// If any existing document matches the new doc on all indexed fields, returns
/// [`Error::DuplicateKey`].
pub(super) fn check_unique_constraints<S: BTreePageStore>(
    tree: &BTree<S>,
    unique_specs: &[(String, Vec<String>, bool)],
    new_doc: &Document,
) -> Result<()> {
    if unique_specs.is_empty() {
        return Ok(());
    }

    let null_encoded = encode_key(&Bson::Null);

    for (idx_name, fields, sparse) in unique_specs {
        // Encode the candidate document's indexed fields.
        let new_encoded: Vec<Vec<u8>> = fields
            .iter()
            .map(|f| encode_key(new_doc.get(f.as_str()).unwrap_or(&Bson::Null)))
            .collect();

        // Sparse: skip if all indexed fields are null/absent.
        if *sparse && new_encoded.iter().all(|v| v == &null_encoded) {
            continue;
        }

        // Scan all documents in the tree.
        let pairs = tree.range_scan(None, None)?;
        for (_, cv) in pairs {
            let bson_bytes = resolve_cell(tree, cv)?;
            let existing: Document =
                bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;

            let existing_encoded: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| encode_key(existing.get(f.as_str()).unwrap_or(&Bson::Null)))
                .collect();

            if new_encoded == existing_encoded {
                return Err(Error::DuplicateKey {
                    detail: format!(
                        "E11000 duplicate key error — unique index '{}': dup key {{{}}}",
                        idx_name,
                        fields
                            .iter()
                            .map(|f| format!("{}: {:?}", f, new_doc.get(f.as_str())))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Resolve a [`CellValue`] from a B+ tree to raw bytes.
pub(super) fn resolve_cell<S: BTreePageStore>(tree: &BTree<S>, cv: CellValue) -> Result<Vec<u8>> {
    match cv {
        CellValue::Inline(b) => Ok(b),
        CellValue::Overflow {
            first_page,
            total_length,
        } => tree.read_overflow(first_page, total_length),
    }
}

// ---------------------------------------------------------------------------
// Sort / projection helpers (replicated from engine.rs for local use)
// ---------------------------------------------------------------------------

pub(super) fn sort_docs(docs: &mut [Document], sort: &Document) {
    docs.sort_by(|a, b| compare_docs(a, b, sort));
}

pub(super) fn compare_docs(a: &Document, b: &Document, sort: &Document) -> std::cmp::Ordering {
    for (field, dir) in sort {
        let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
        let av = get_nested_field(a, field).cloned().unwrap_or(Bson::Null);
        let bv = get_nested_field(b, field).cloned().unwrap_or(Bson::Null);
        let ord = encode_key(&av).cmp(&encode_key(&bv));
        if ord == std::cmp::Ordering::Equal {
            continue;
        }
        return if ascending { ord } else { ord.reverse() };
    }
    std::cmp::Ordering::Equal
}

pub(super) fn apply_projection_to_doc(mut doc: Document, proj: &Document) -> Document {
    let is_inclusion = proj
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    let explicit_id_excl = proj
        .get("_id")
        .is_some_and(|v| matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    if is_inclusion {
        let mut result = Document::new();
        if !explicit_id_excl {
            if let Some(id) = doc.get("_id") {
                result.insert("_id", id.clone());
            }
        }
        for (k, v) in proj {
            if k == "_id" {
                continue;
            }
            if !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) {
                if let Some(val) = doc.get(k) {
                    result.insert(k, val.clone());
                }
            }
        }
        result
    } else {
        for (k, _) in proj {
            doc.remove(k);
        }
        doc
    }
}

// ---------------------------------------------------------------------------
// Engine-level doc operation free functions
// ---------------------------------------------------------------------------

use super::btree_ops::btree_insert_doc;
use super::catalog_ops::{new_txn_store, sync_catalog_root_overlay};
use super::index_maint::{maintain_secondary_on_delete, maintain_secondary_on_insert, maintain_secondary_on_update};
use super::snapshot_ops::{apply_find_opts, execute_snapshot_pairs_from_snap};

pub(super) fn insert(engine: &super::PagedEngine, ns: &str, mut doc: Document) -> Result<Bson> {
    engine.run_write(ns, |shared, md, overlay, txn| {
        let entry = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-write", ns)))?;
        let mut tree = BTree::open(
            new_txn_store(shared, overlay),
            entry.data_root_page,
            entry.data_root_level,
        );
        let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut doc, &[])?;
        if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
            let mut updated = entry.clone();
            updated.data_root_page = tree.root_page;
            updated.data_root_level = tree.root_level;
            md.catalog.lock().expect("catalog poisoned").update_collection(&updated)?;
            sync_catalog_root_overlay(shared, md, overlay)?;
        }
        txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
        maintain_secondary_on_insert(shared, md, overlay, ns, &doc, &id, txn)?;
        Ok(id)
    })
}

pub(super) fn find(engine: &super::PagedEngine, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
    let snap = engine.shared.published.load();
    let ns_snap = match snap.namespaces.get(ns) {
        None => return Ok(Vec::new()),
        Some(n) => n,
    };
    let matched: Vec<Document> = execute_snapshot_pairs_from_snap(
        &engine.shared,
        ns,
        ns_snap,
        filter,
        snap.publish_ts,
        true,
    )?
    .into_iter()
    .map(|(_, doc)| doc)
    .collect();
    Ok(apply_find_opts(matched, opts))
}

pub(super) fn find_one(engine: &super::PagedEngine, ns: &str, filter: &Document) -> Result<Option<Document>> {
    let opts = FindOptions::new();
    let mut results = find(engine, ns, filter, &opts)?;
    Ok(if results.is_empty() {
        None
    } else {
        Some(results.remove(0))
    })
}

pub(super) fn update(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    update_doc: &Document,
    opts: &UpdateOptions,
    many: bool,
) -> Result<UpdateResult> {
    if !is_operator_update(update_doc) {
        return Err(Error::Internal(
            "update requires an operator update document (e.g. {$set: {...}});                  use find_one_and_replace for replacements"
                .into(),
        ));
    }

    let snap = engine.shared.published.load();
    let ns_snap_opt = snap.namespaces.get(ns);
    let matched_pairs: Vec<(Vec<u8>, Document)> = match ns_snap_opt {
        None => {
            if opts.upsert {
                return do_upsert_update(engine, ns, filter, update_doc);
            }
            return Ok(UpdateResult {
                matched_count: 0,
                modified_count: 0,
                upserted_id: None,
            });
        }
        Some(ns_snap) => {
            execute_snapshot_pairs_from_snap(
                &engine.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?
        }
    };

    if matched_pairs.is_empty() && opts.upsert {
        return do_upsert_update(engine, ns, filter, update_doc);
    }

    let pairs_to_process: Vec<(Vec<u8>, Document)> = if many {
        matched_pairs
    } else {
        matched_pairs.into_iter().take(1).collect()
    };

    engine.run_write_existing(ns, |shared, md, overlay, txn| {
        let mut matched_count = 0u64;
        let mut modified_count = 0u64;
        for (key, mut doc) in pairs_to_process {
            matched_count += 1;
            let before = doc.clone();
            let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
            apply_update(&mut doc, update_doc, false)?;
            if doc != before {
                modified_count += 1;
                let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
                maintain_secondary_on_update(
                    shared, md, overlay, ns, &before, &doc, &before_id, &new_id, txn,
                )?;
                let entry_opt = md
                    .catalog
                    .lock()
                    .expect("catalog poisoned")
                    .get_collection(ns)?;
                if let Some(entry) = entry_opt {
                    let mut tree = BTree::open(
                        new_txn_store(shared, overlay),
                        entry.data_root_page,
                        entry.data_root_level,
                    );
                    tree.delete(&key)?;
                    tree.insert(&key, &new_bytes)?;
                    if tree.root_page != entry.data_root_page
                        || tree.root_level != entry.data_root_level
                    {
                        let mut updated = entry.clone();
                        updated.data_root_page = tree.root_page;
                        updated.data_root_level = tree.root_level;
                        md.catalog
                            .lock()
                            .expect("catalog poisoned")
                            .update_collection(&updated)?;
                        sync_catalog_root_overlay(shared, md, overlay)?;
                    }
                    txn.stage_primary_update(ns.to_string(), key, new_bytes);
                }
            }
        }
        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id: None,
        })
    })
}

pub(super) fn delete(engine: &super::PagedEngine, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
    let snap = engine.shared.published.load();
    let pairs_to_delete: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
        None => return Ok(DeleteResult { deleted_count: 0 }),
        Some(ns_snap) => {
            let pairs = execute_snapshot_pairs_from_snap(
                &engine.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?;
            if many {
                pairs
            } else {
                pairs.into_iter().take(1).collect()
            }
        }
    };

    let deleted_count = pairs_to_delete.len() as u64;
    if deleted_count == 0 {
        return Ok(DeleteResult { deleted_count: 0 });
    }

    engine.run_write_existing(ns, |shared, md, overlay, txn| {
        for (key, doc) in &pairs_to_delete {
            let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
            maintain_secondary_on_delete(shared, md, overlay, ns, doc, &doc_id, txn)?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(key)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_delete(ns.to_string(), key.clone());
            }
        }
        Ok(())
    })?;

    Ok(DeleteResult { deleted_count })
}

pub(super) fn count(engine: &super::PagedEngine, ns: &str, filter: &Document) -> Result<u64> {
    let snap = engine.shared.published.load();
    let ns_snap = match snap.namespaces.get(ns) {
        None => return Ok(0),
        Some(n) => n,
    };
    Ok(
        execute_snapshot_pairs_from_snap(
            &engine.shared,
            ns,
            ns_snap,
            filter,
            snap.publish_ts,
            false,
        )?
        .len() as u64,
    )
}

pub(super) fn find_one_and_update_doc(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    update_doc: &Document,
    opts: &FindOneAndUpdateOptions,
) -> Result<Option<Document>> {
    if !is_operator_update(update_doc) {
        return Err(Error::Internal(
            "find_one_and_update requires an operator update document".into(),
        ));
    }

    let snap = engine.shared.published.load();
    let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
        None => {
            if opts.upsert {
                return fam_upsert_update(engine, ns, filter, update_doc, opts);
            }
            return Ok(None);
        }
        Some(ns_snap) => {
            execute_snapshot_pairs_from_snap(
                &engine.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?
        }
    };

    if matched.is_empty() {
        if opts.upsert {
            return fam_upsert_update(engine, ns, filter, update_doc, opts);
        }
        return Ok(None);
    }

    if let Some(s) = &opts.sort {
        matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
    }

    let (key, mut doc) = matched.remove(0);
    let before = doc.clone();
    let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
    apply_update(&mut doc, update_doc, false)?;
    let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
    let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;

    engine.run_write_existing(ns, |shared, md, overlay, txn| {
        maintain_secondary_on_update(
            shared, md, overlay, ns, &before, &doc, &before_id, &new_id, txn,
        )?;
        let entry_opt = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?;
        if let Some(entry) = entry_opt {
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            tree.delete(&key)?;
            tree.insert(&key, &new_bytes)?;
            if tree.root_page != entry.data_root_page
                || tree.root_level != entry.data_root_level
            {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_update(ns.to_string(), key, new_bytes);
        }
        Ok(())
    })?;

    Ok(Some(match opts.return_document {
        ReturnDocument::Before => before,
        ReturnDocument::After => doc,
    }))
}

pub(super) fn find_one_and_delete_doc(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    opts: &FindOneAndDeleteOptions,
) -> Result<Option<Document>> {
    let snap = engine.shared.published.load();
    let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
        None => return Ok(None),
        Some(ns_snap) => execute_snapshot_pairs_from_snap(
            &engine.shared,
            ns,
            ns_snap,
            filter,
            snap.publish_ts,
            false,
        )?,
    };

    if matched.is_empty() {
        return Ok(None);
    }

    if let Some(s) = &opts.sort {
        matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
    }

    let (key, doc) = matched.remove(0);
    let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);

    engine.run_write_existing(ns, |shared, md, overlay, txn| {
        maintain_secondary_on_delete(shared, md, overlay, ns, &doc, &doc_id, txn)?;
        let entry_opt = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?;
        if let Some(entry) = entry_opt {
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            tree.delete(&key)?;
            if tree.root_page != entry.data_root_page
                || tree.root_level != entry.data_root_level
            {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_delete(ns.to_string(), key);
        }
        Ok(())
    })?;

    Ok(Some(doc))
}

pub(super) fn find_one_and_replace_doc(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    replacement: &Document,
    opts: &FindOneAndReplaceOptions,
) -> Result<Option<Document>> {
    let snap = engine.shared.published.load();
    let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
        None => {
            if opts.upsert {
                return fam_upsert_replace(engine, ns, replacement, opts);
            }
            return Ok(None);
        }
        Some(ns_snap) => {
            execute_snapshot_pairs_from_snap(
                &engine.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?
        }
    };

    if matched.is_empty() {
        if opts.upsert {
            return fam_upsert_replace(engine, ns, replacement, opts);
        }
        return Ok(None);
    }

    if let Some(s) = &opts.sort {
        matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
    }

    let (old_key, old_doc) = matched.remove(0);

    let mut new_doc = replacement.clone();
    let original_id = old_doc.get("_id").cloned().unwrap_or(Bson::Null);
    new_doc.insert("_id", original_id.clone());
    validate_document(&new_doc)?;

    let new_key = encode_key(&original_id);
    let new_bytes = bson::to_vec(&new_doc).map_err(Error::BsonSerialization)?;

    let old_doc_clone = old_doc.clone();
    let new_doc_clone = new_doc.clone();
    engine.run_write_existing(ns, |shared, md, overlay, txn| {
        maintain_secondary_on_update(
            shared,
            md,
            overlay,
            ns,
            &old_doc_clone,
            &new_doc_clone,
            &original_id,
            &original_id,
            txn,
        )?;
        let entry_opt = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?;
        if let Some(entry) = entry_opt {
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            tree.delete(&old_key)?;
            tree.insert(&new_key, &new_bytes)?;
            if tree.root_page != entry.data_root_page
                || tree.root_level != entry.data_root_level
            {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_update(ns.to_string(), new_key, new_bytes);
        }
        Ok(())
    })?;

    Ok(Some(match opts.return_document {
        ReturnDocument::Before => old_doc,
        ReturnDocument::After => new_doc,
    }))
}

pub(super) fn do_upsert_update(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    update_doc: &Document,
) -> Result<UpdateResult> {
    let mut new_doc = upsert_base_from_filter(filter);
    apply_update(&mut new_doc, update_doc, true)?;
    let id = engine.run_write(ns, |shared, md, overlay, txn| {
        let entry = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
        let mut tree = BTree::open(
            new_txn_store(shared, overlay),
            entry.data_root_page,
            entry.data_root_level,
        );
        let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
        if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
            let mut updated = entry.clone();
            updated.data_root_page = tree.root_page;
            updated.data_root_level = tree.root_level;
            md.catalog
                .lock()
                .expect("catalog poisoned")
                .update_collection(&updated)?;
            sync_catalog_root_overlay(shared, md, overlay)?;
        }
        txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
        maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
        Ok(id)
    })?;
    Ok(UpdateResult {
        matched_count: 0,
        modified_count: 0,
        upserted_id: Some(id),
    })
}

pub(super) fn fam_upsert_update(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    update_doc: &Document,
    opts: &FindOneAndUpdateOptions,
) -> Result<Option<Document>> {
    let mut new_doc = upsert_base_from_filter(filter);
    apply_update(&mut new_doc, update_doc, true)?;
    engine.run_write(ns, |shared, md, overlay, txn| {
        let entry = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
        let mut tree = BTree::open(
            new_txn_store(shared, overlay),
            entry.data_root_page,
            entry.data_root_level,
        );
        let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
        if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
            let mut updated = entry.clone();
            updated.data_root_page = tree.root_page;
            updated.data_root_level = tree.root_level;
            md.catalog
                .lock()
                .expect("catalog poisoned")
                .update_collection(&updated)?;
            sync_catalog_root_overlay(shared, md, overlay)?;
        }
        txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
        maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
        Ok(())
    })?;
    Ok(match opts.return_document {
        ReturnDocument::Before => None,
        ReturnDocument::After => Some(new_doc),
    })
}

pub(super) fn fam_upsert_replace(
    engine: &super::PagedEngine,
    ns: &str,
    replacement: &Document,
    opts: &FindOneAndReplaceOptions,
) -> Result<Option<Document>> {
    let mut new_doc = replacement.clone();
    engine.run_write(ns, |shared, md, overlay, txn| {
        let entry = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
        let mut tree = BTree::open(
            new_txn_store(shared, overlay),
            entry.data_root_page,
            entry.data_root_level,
        );
        let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
        if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
            let mut updated = entry.clone();
            updated.data_root_page = tree.root_page;
            updated.data_root_level = tree.root_level;
            md.catalog
                .lock()
                .expect("catalog poisoned")
                .update_collection(&updated)?;
            sync_catalog_root_overlay(shared, md, overlay)?;
        }
        txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
        maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
        Ok(())
    })?;
    Ok(match opts.return_document {
        ReturnDocument::Before => None,
        ReturnDocument::After => Some(new_doc),
    })
}
