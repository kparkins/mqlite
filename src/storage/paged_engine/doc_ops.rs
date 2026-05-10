//! Engine-level CRUD helpers for find, update, delete, the `findOneAnd*`
//! family, and insert staging. Pure helpers (id, validation, projection, sort,
//! unique-constraint checks, cell resolution) live in [`super::doc_helpers`].

use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::mvcc::transaction::{ExpectedHead, Ns};
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::BTree;
use crate::update::{apply_update, is_operator_update, upsert_base_from_filter};
use crate::validation::validate_document;

use super::btree_ops::prepare_insert_document;
use super::doc_helpers::{compare_docs, ensure_id};
use super::index_maint::{
    maintain_secondary_on_delete, maintain_secondary_on_insert, maintain_secondary_on_update,
};
use super::snapshot_ops::{
    apply_find_opts, plan_and_collect_snapshot_pairs, plan_and_collect_snapshot_pairs_limited,
};
use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::root_snapshot::NamespaceSnapshot;

type MutationCandidate = (Vec<u8>, Document, Option<ExpectedHead>);

fn expected_head_for_key(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    key: &[u8],
) -> Result<Option<ExpectedHead>> {
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let leaf_page = tree.find_leaf(key)?;
    let page = shared.handle.pool().pin_for_read(leaf_page)?;
    Ok(page.expected_head(key))
}

fn attach_expected_heads(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    pairs: Vec<(Vec<u8>, Document)>,
) -> Result<Vec<MutationCandidate>> {
    pairs
        .into_iter()
        .map(|(key, doc)| {
            let expected_head = expected_head_for_key(shared, ns_snap, &key)?;
            Ok((key, doc, expected_head))
        })
        .collect()
}

fn collect_mutation_candidates(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    epoch: Arc<crate::storage::root_snapshot::PublishedEpoch>,
) -> Result<Vec<MutationCandidate>> {
    let (_, pairs) = plan_and_collect_snapshot_pairs(shared, ns_snap, filter, epoch, false)?;
    attach_expected_heads(shared, ns_snap, pairs)
}

pub(super) fn stage_insert(
    shared: &SharedState,
    md: &MetadataState,
    txn: &mut crate::mvcc::transaction::WriteTxn,
    vis: &WriteVisibility<'_>,
    ns: &str,
    mut doc: Document,
) -> Result<Bson> {
    let ns_arc = Ns::from(ns);
    let entry = md
        .catalog_lock()
        .get_collection(ns)?
        .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-write", ns)))?;
    let tree = BTree::open(
        shared.new_btree_store(),
        entry.data_root_page,
        entry.data_root_level,
    );
    let (id, key, bson_bytes) = prepare_insert_document(
        &tree,
        &mut doc,
        &[],
        vis,
        txn.pending_primary.as_slice(),
        ns,
    )?;
    let entry_id = entry.id;
    txn.stage_primary_insert(entry_id, ns_arc, key, bson_bytes, None);
    maintain_secondary_on_insert(shared, md, ns, &doc, &id, vis, txn)?;
    Ok(id)
}

pub(super) fn find(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    opts: &FindOptions,
) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)> {
    let snap = engine.shared.load_published();
    let ns_snap = match snap.catalog.get_by_name(ns) {
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
    let (plan, pairs) =
        plan_and_collect_snapshot_pairs(&engine.shared, ns_snap, filter, Arc::clone(&snap), true)?;
    let docs_examined = pairs.len() as u64;
    let matched: Vec<Document> = pairs.into_iter().map(|(_, doc)| doc).collect();
    let explain = crate::query::explain::ExplainResult::from_plan(&plan, docs_examined);
    Ok((apply_find_opts(matched, opts), explain))
}

/// Return the first document that matches `filter`, short-circuiting after
/// one match. Callers that only need a single result should prefer this over
/// [`find`] to avoid decoding the entire collection.
pub(super) fn find_first(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
) -> Result<Option<Document>> {
    let snap = engine.shared.load_published();
    let ns_snap = match snap.catalog.get_by_name(ns) {
        None => return Ok(None),
        Some(n) => n,
    };
    let (_, pairs) = plan_and_collect_snapshot_pairs_limited(
        &engine.shared,
        ns_snap,
        filter,
        Arc::clone(&snap),
        true,
        Some(1),
    )?;
    Ok(pairs.into_iter().next().map(|(_, doc)| doc))
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

    let snap = engine.shared.load_published();
    let ns_snap_opt = snap.catalog.get_by_name(ns);
    let matched_pairs: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> = match ns_snap_opt {
        None => {
            if opts.upsert {
                return upsert_for_update(engine, ns, filter, update_doc);
            }
            return Ok(UpdateResult {
                matched_count: 0,
                modified_count: 0,
                upserted_id: None,
            });
        }
        Some(ns_snap) => {
            collect_mutation_candidates(&engine.shared, ns_snap, filter, Arc::clone(&snap))?
        }
    };

    if matched_pairs.is_empty() && opts.upsert {
        return upsert_for_update(engine, ns, filter, update_doc);
    }

    let pairs_to_process: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> = if many {
        matched_pairs
    } else {
        matched_pairs.into_iter().take(1).collect()
    };

    let ns_arc = Ns::from(ns);
    engine.run_write_commit_envelope(ns, None, |shared, md, txn, vis| {
        let mut matched_count = 0u64;
        let mut modified_count = 0u64;
        for (key, mut doc, expected_head) in pairs_to_process {
            matched_count += 1;
            let before = doc.clone();
            let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
            apply_update(&mut doc, update_doc, false)?;
            if doc != before {
                modified_count += 1;
                let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
                maintain_secondary_on_update(
                    shared, md, ns, &before, &doc, &before_id, &new_id, vis, txn,
                )?;
                if let Some(entry) = md.catalog_lock().get_collection(ns)? {
                    txn.stage_primary_update(
                        entry.id,
                        ns_arc.clone(),
                        key,
                        new_bytes,
                        expected_head,
                    );
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

pub(super) fn delete(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    many: bool,
) -> Result<DeleteResult> {
    let snap = engine.shared.load_published();
    let pairs_to_delete: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> = match snap
        .catalog
        .get_by_name(ns)
    {
        None => return Ok(DeleteResult { deleted_count: 0 }),
        Some(ns_snap) => {
            let pairs =
                collect_mutation_candidates(&engine.shared, ns_snap, filter, Arc::clone(&snap))?;
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
    engine.run_write_commit_envelope(ns, None, |_shared, md, txn, _vis| {
        for (key, doc, expected_head) in &pairs_to_delete {
            let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
            maintain_secondary_on_delete(md, ns, doc, &doc_id, txn)?;
            if let Some(entry) = md.catalog_lock().get_collection(ns)? {
                txn.stage_primary_delete(entry.id, ns_arc.clone(), key.clone(), *expected_head);
            }
        }
        Ok(())
    })?;

    Ok(DeleteResult { deleted_count })
}

pub(super) fn count(engine: &super::PagedEngine, ns: &str, filter: &Document) -> Result<u64> {
    let snap = engine.shared.load_published();
    let ns_snap = match snap.catalog.get_by_name(ns) {
        None => return Ok(0),
        Some(n) => n,
    };
    Ok(
        plan_and_collect_snapshot_pairs(&engine.shared, ns_snap, filter, Arc::clone(&snap), false)
            .map(|(_, p)| p)?
            .len() as u64,
    )
}

pub(super) fn find_one_and_update(
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

    let snap = engine.shared.load_published();
    let mut matched: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> =
        match snap.catalog.get_by_name(ns) {
            None => {
                if opts.upsert {
                    return upsert_for_find_one_and_update(engine, ns, filter, update_doc, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => {
                collect_mutation_candidates(&engine.shared, ns_snap, filter, Arc::clone(&snap))?
            }
        };

    if matched.is_empty() {
        if opts.upsert {
            return upsert_for_find_one_and_update(engine, ns, filter, update_doc, opts);
        }
        return Ok(None);
    }

    if let Some(s) = &opts.sort {
        matched.sort_by(|(_, a, _), (_, b, _)| compare_docs(a, b, s));
    }

    let (key, mut doc, expected_head) = matched.remove(0);
    let before = doc.clone();
    let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
    apply_update(&mut doc, update_doc, false)?;
    let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
    let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;

    let ns_arc = Ns::from(ns);
    engine.run_write_commit_envelope(ns, None, |shared, md, txn, vis| {
        maintain_secondary_on_update(shared, md, ns, &before, &doc, &before_id, &new_id, vis, txn)?;
        if let Some(entry) = md.catalog_lock().get_collection(ns)? {
            txn.stage_primary_update(entry.id, ns_arc.clone(), key, new_bytes, expected_head);
        }
        Ok(())
    })?;

    Ok(Some(match opts.return_document {
        ReturnDocument::Before => before,
        ReturnDocument::After => doc,
    }))
}

pub(super) fn find_one_and_delete(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    opts: &FindOneAndDeleteOptions,
) -> Result<Option<Document>> {
    let snap = engine.shared.load_published();
    let mut matched: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> =
        match snap.catalog.get_by_name(ns) {
            None => return Ok(None),
            Some(ns_snap) => {
                collect_mutation_candidates(&engine.shared, ns_snap, filter, Arc::clone(&snap))?
            }
        };

    if matched.is_empty() {
        return Ok(None);
    }

    if let Some(s) = &opts.sort {
        matched.sort_by(|(_, a, _), (_, b, _)| compare_docs(a, b, s));
    }

    let (key, doc, expected_head) = matched.remove(0);
    let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);

    let ns_arc = Ns::from(ns);
    engine.run_write_commit_envelope(ns, None, |_shared, md, txn, _vis| {
        maintain_secondary_on_delete(md, ns, &doc, &doc_id, txn)?;
        if let Some(entry) = md.catalog_lock().get_collection(ns)? {
            txn.stage_primary_delete(entry.id, ns_arc.clone(), key, expected_head);
        }
        Ok(())
    })?;

    Ok(Some(doc))
}

pub(super) fn find_one_and_replace(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    replacement: &Document,
    opts: &FindOneAndReplaceOptions,
) -> Result<Option<Document>> {
    let snap = engine.shared.load_published();
    let mut matched: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> =
        match snap.catalog.get_by_name(ns) {
            None => {
                if opts.upsert {
                    return upsert_for_find_one_and_replace(engine, ns, replacement, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => {
                collect_mutation_candidates(&engine.shared, ns_snap, filter, Arc::clone(&snap))?
            }
        };

    if matched.is_empty() {
        if opts.upsert {
            return upsert_for_find_one_and_replace(engine, ns, replacement, opts);
        }
        return Ok(None);
    }

    if let Some(s) = &opts.sort {
        matched.sort_by(|(_, a, _), (_, b, _)| compare_docs(a, b, s));
    }

    let (old_key, old_doc, expected_head) = matched.remove(0);

    let mut new_doc = replacement.clone();
    let original_id = old_doc.get("_id").cloned().unwrap_or(Bson::Null);
    new_doc.insert("_id", original_id.clone());
    validate_document(&new_doc)?;

    let new_bytes = bson::to_vec(&new_doc).map_err(Error::BsonSerialization)?;

    let ns_arc = Ns::from(ns);
    engine.run_write_commit_envelope(ns, None, |shared, md, txn, vis| {
        maintain_secondary_on_update(
            shared,
            md,
            ns,
            &old_doc,
            &new_doc,
            &original_id,
            &original_id,
            vis,
            txn,
        )?;
        if let Some(entry) = md.catalog_lock().get_collection(ns)? {
            txn.stage_primary_update(entry.id, ns_arc.clone(), old_key, new_bytes, expected_head);
        }
        Ok(())
    })?;

    Ok(Some(match opts.return_document {
        ReturnDocument::Before => old_doc,
        ReturnDocument::After => new_doc,
    }))
}

fn upsert_stage(engine: &super::PagedEngine, ns: &str, doc: Document) -> Result<(Bson, Document)> {
    let snapshot = doc.clone();
    let id = engine.run_write(ns, |shared, md, txn, vis| {
        stage_insert(shared, md, txn, vis, ns, doc)
    })?;
    Ok((id, snapshot))
}

pub(super) fn upsert_for_update(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    update_doc: &Document,
) -> Result<UpdateResult> {
    let mut doc = upsert_base_from_filter(filter);
    apply_update(&mut doc, update_doc, true)?;
    let (id, _) = upsert_stage(engine, ns, doc)?;
    Ok(UpdateResult {
        matched_count: 0,
        modified_count: 0,
        upserted_id: Some(id),
    })
}

pub(super) fn upsert_for_find_one_and_update(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    update_doc: &Document,
    opts: &FindOneAndUpdateOptions,
) -> Result<Option<Document>> {
    let mut doc = upsert_base_from_filter(filter);
    apply_update(&mut doc, update_doc, true)?;
    ensure_id(&mut doc);
    let (_, after) = upsert_stage(engine, ns, doc)?;
    Ok(match opts.return_document {
        ReturnDocument::Before => None,
        ReturnDocument::After => Some(after),
    })
}

pub(super) fn upsert_for_find_one_and_replace(
    engine: &super::PagedEngine,
    ns: &str,
    replacement: &Document,
    opts: &FindOneAndReplaceOptions,
) -> Result<Option<Document>> {
    let mut doc = replacement.clone();
    ensure_id(&mut doc);
    let (_, after) = upsert_stage(engine, ns, doc)?;
    Ok(match opts.return_document {
        ReturnDocument::Before => None,
        ReturnDocument::After => Some(after),
    })
}
