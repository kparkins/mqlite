//! Engine-level CRUD free functions: insert, find, update, delete, and the
//! `findOneAnd*` family. Pure helpers (id, validation, projection, sort,
//! unique-constraint checks, cell resolution) live in [`super::doc_helpers`].

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::mvcc::transaction::Ns;
use crate::keys::encode_key;
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::BTree;
use crate::update::{apply_update, is_operator_update, upsert_base_from_filter};
use crate::validation::validate_document;

use super::btree_ops::btree_insert_doc;
use super::catalog_ops::{new_txn_store, sync_catalog_root_overlay};
use super::doc_helpers::compare_docs;
use super::index_maint::{
    maintain_secondary_on_delete, maintain_secondary_on_insert, maintain_secondary_on_update,
};
use super::snapshot_ops::{
    apply_find_opts, execute_snapshot_pairs_from_snap, execute_snapshot_pairs_only,
};

pub(super) fn insert(engine: &super::PagedEngine, ns: &str, mut doc: Document) -> Result<Bson> {
    let ns_arc = Ns::from(ns);
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
        txn.stage_primary_insert(ns_arc.clone(), key, bson_bytes);
        maintain_secondary_on_insert(shared, md, overlay, ns, &doc, &id, txn)?;
        Ok(id)
    })
}

pub(super) fn find(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    opts: &FindOptions,
) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)> {
    let snap = engine.shared.published.load();
    let ns_snap = match snap.namespaces.get(ns) {
        None => {
            // No namespace → planner never ran; report an empty collscan.
            let plan = crate::query::planner::ScanPlan::CollScan;
            return Ok((
                Vec::new(),
                crate::query::explain::ExplainResult::from_plan(&plan, 0),
            ));
        }
        Some(n) => n,
    };
    let (plan, pairs) = execute_snapshot_pairs_from_snap(
        &engine.shared,
        ns,
        ns_snap,
        filter,
        snap.publish_ts,
        true,
    )?;
    let docs_examined = pairs.len() as u64;
    let matched: Vec<Document> = pairs.into_iter().map(|(_, doc)| doc).collect();
    let explain = crate::query::explain::ExplainResult::from_plan(&plan, docs_examined);
    Ok((apply_find_opts(matched, opts), explain))
}

pub(super) fn find_one(engine: &super::PagedEngine, ns: &str, filter: &Document) -> Result<Option<Document>> {
    let opts = FindOptions::new();
    let (mut results, _explain) = find(engine, ns, filter, &opts)?;
    Ok((!results.is_empty()).then(|| results.remove(0)))
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
            execute_snapshot_pairs_only(
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

    let ns_arc = Ns::from(ns);
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
                    txn.stage_primary_update(ns_arc.clone(), key, new_bytes);
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
            let pairs = execute_snapshot_pairs_only(
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

    let ns_arc = Ns::from(ns);
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
                txn.stage_primary_delete(ns_arc.clone(), key.clone());
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
        execute_snapshot_pairs_only(
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
            execute_snapshot_pairs_only(
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

    let ns_arc = Ns::from(ns);
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
            txn.stage_primary_update(ns_arc.clone(), key, new_bytes);
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
        Some(ns_snap) => execute_snapshot_pairs_only(
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

    let ns_arc = Ns::from(ns);
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
            txn.stage_primary_delete(ns_arc.clone(), key);
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
            execute_snapshot_pairs_only(
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
    let ns_arc = Ns::from(ns);
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
            txn.stage_primary_update(ns_arc.clone(), new_key, new_bytes);
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
    let ns_arc = Ns::from(ns);
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
        txn.stage_primary_insert(ns_arc.clone(), key, bson_bytes);
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
    let ns_arc = Ns::from(ns);
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
        txn.stage_primary_insert(ns_arc.clone(), key, bson_bytes);
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
    let ns_arc = Ns::from(ns);
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
        txn.stage_primary_insert(ns_arc.clone(), key, bson_bytes);
        maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
        Ok(())
    })?;
    Ok(match opts.return_document {
        ReturnDocument::Before => None,
        ReturnDocument::After => Some(new_doc),
    })
}
