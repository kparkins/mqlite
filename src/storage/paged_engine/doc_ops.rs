//! Engine-level CRUD helpers for find, update, delete, the `findOneAnd*`
//! family, and insert staging. Pure helpers (id, validation, projection, sort,
//! unique-constraint checks, cell resolution) live in [`super::doc_helpers`].

use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result, WriteConflictReason};
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::{ExpectedHead, Ns, PrimaryTarget};
use crate::mvcc::{VersionData, VersionEntry, VersionState};
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::{read_overflow_chain, BTree};
use crate::update::{apply_update, is_operator_update, upsert_base_from_filter};
use crate::validation::validate_document;

use super::btree_ops::prepare_insert_document;
use super::doc_helpers::{compare_docs, ensure_id};
use super::index_maint::{
    maintain_secondary_on_delete, maintain_secondary_on_insert,
    maintain_secondary_on_insert_snapshot, maintain_secondary_on_update,
};
use super::snapshot_ops::{
    apply_find_opts, open_snapshot_read_view, plan_and_collect_snapshot_pairs,
    plan_and_collect_snapshot_pairs_limited,
};
use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::root_snapshot::NamespaceSnapshot;

type MutationCandidate = (Vec<u8>, Document, Option<ExpectedHead>);

fn unsorted_materialization_limit(opts: &FindOptions) -> Option<usize> {
    if opts.sort.is_some() {
        return None;
    }
    let limit = opts.limit.filter(|limit| *limit > 0)? as usize;
    Some(
        opts.skip
            .and_then(|skip| usize::try_from(skip).ok())
            .unwrap_or(0)
            .saturating_add(limit),
    )
}

fn selected_version_stale() -> Error {
    Error::WriteConflict {
        reason: WriteConflictReason::StaleSnapshot,
    }
}

fn expected_head_from_entry(entry: &VersionEntry) -> ExpectedHead {
    ExpectedHead {
        commit_ts: entry.start_ts,
        txn_id: entry.txn_id,
    }
}

fn version_entry_bytes(store: &BufferPoolPageStore, entry: &VersionEntry) -> Result<Vec<u8>> {
    match &entry.data {
        VersionData::Inline(bytes) => Ok(bytes.clone()),
        VersionData::Overflow(overflow) => {
            read_overflow_chain(store, overflow.first_page(), overflow.total_length() as u32)
        }
    }
}

fn expected_head_for_selected_doc(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    key: &[u8],
    selected: &Document,
) -> Result<Option<ExpectedHead>> {
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let leaf_page = tree.find_leaf(key)?;
    let page = shared.handle.pool().pin_for_read(leaf_page)?;
    let Some(head) = page.live_head(key) else {
        return Ok(None);
    };
    drop(page);

    if !matches!(head.state, VersionState::Committed) || head.is_tombstone {
        return Err(selected_version_stale());
    }

    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let live_bytes = version_entry_bytes(&store, &head)?;
    let live_doc: Document = bson::from_slice(&live_bytes).map_err(Error::BsonDeserialization)?;
    if &live_doc != selected {
        return Err(selected_version_stale());
    }

    Ok(Some(expected_head_from_entry(&head)))
}

fn attach_expected_heads(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    pairs: Vec<(Vec<u8>, Document)>,
) -> Result<Vec<MutationCandidate>> {
    pairs
        .into_iter()
        .map(|(key, doc)| {
            let expected_head = expected_head_for_selected_doc(shared, ns_snap, &key, &doc)?;
            Ok((key, doc, expected_head))
        })
        .collect()
}

fn collect_mutation_candidates(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    view: &ReadView,
) -> Result<Vec<MutationCandidate>> {
    let (_, pairs) = plan_and_collect_snapshot_pairs(shared, ns_snap, filter, view, false)?;
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
    let snap = vis.read_view.published_epoch();
    if let Some(ns_snap) = snap.catalog.get_by_name(ns) {
        return stage_insert_snapshot(shared, md, txn, vis, ns, ns_snap, doc);
    }

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
    txn.stage_primary_insert(
        PrimaryTarget::new(
            entry.id,
            ns_arc,
            entry.data_root_page,
            entry.data_root_level,
        ),
        key,
        bson_bytes,
        None,
    );
    maintain_secondary_on_insert(shared, md, ns, &doc, &id, vis, txn)?;
    Ok(id)
}

fn stage_insert_snapshot(
    shared: &SharedState,
    md: &MetadataState,
    txn: &mut crate::mvcc::transaction::WriteTxn,
    vis: &WriteVisibility<'_>,
    ns: &str,
    ns_snap: &NamespaceSnapshot,
    mut doc: Document,
) -> Result<Bson> {
    let tree = BTree::open(
        shared.new_btree_store(),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let (id, key, bson_bytes) = prepare_insert_document(
        &tree,
        &mut doc,
        &[],
        vis,
        txn.pending_primary.as_slice(),
        ns,
    )?;
    txn.stage_primary_insert(
        PrimaryTarget::new(
            ns_snap.id,
            ns,
            ns_snap.data_root_page,
            ns_snap.data_root_level,
        ),
        key,
        bson_bytes,
        None,
    );
    maintain_secondary_on_insert_snapshot(shared, md, ns, &ns_snap.indexes, &doc, &id, vis, txn)?;
    Ok(id)
}

pub(super) fn find(
    engine: &super::PagedEngine,
    ns: &str,
    filter: &Document,
    opts: &FindOptions,
) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)> {
    // ITEM 1: open the view FIRST so the conservative registry pin precedes
    // the epoch load, then route `ns_snap` from the view's pinned epoch — the
    // single epoch this read uses.
    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
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
    let (plan, pairs) = plan_and_collect_snapshot_pairs_limited(
        &engine.shared,
        ns_snap,
        filter,
        &view,
        true,
        unsorted_materialization_limit(opts),
    )?;
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
    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
    let ns_snap = match snap.catalog.get_by_name(ns) {
        None => return Ok(None),
        Some(n) => n,
    };
    let (_, pairs) = plan_and_collect_snapshot_pairs_limited(
        &engine.shared,
        ns_snap,
        filter,
        &view,
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

    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
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
        Some(ns_snap) => collect_mutation_candidates(&engine.shared, ns_snap, filter, &view)?,
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
                        PrimaryTarget::new(
                            entry.id,
                            ns_arc.clone(),
                            entry.data_root_page,
                            entry.data_root_level,
                        ),
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
    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
    let pairs_to_delete: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> = match snap
        .catalog
        .get_by_name(ns)
    {
        None => return Ok(DeleteResult { deleted_count: 0 }),
        Some(ns_snap) => {
            let pairs = collect_mutation_candidates(&engine.shared, ns_snap, filter, &view)?;
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
                txn.stage_primary_delete(
                    entry.id,
                    ns_arc.clone(),
                    entry.data_root_page,
                    entry.data_root_level,
                    key.clone(),
                    *expected_head,
                );
            }
        }
        Ok(())
    })?;

    Ok(DeleteResult { deleted_count })
}

pub(super) fn count(engine: &super::PagedEngine, ns: &str, filter: &Document) -> Result<u64> {
    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
    let ns_snap = match snap.catalog.get_by_name(ns) {
        None => return Ok(0),
        Some(n) => n,
    };
    Ok(
        plan_and_collect_snapshot_pairs(&engine.shared, ns_snap, filter, &view, false)
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

    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
    let mut matched: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> =
        match snap.catalog.get_by_name(ns) {
            None => {
                if opts.upsert {
                    return upsert_for_find_one_and_update(engine, ns, filter, update_doc, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => collect_mutation_candidates(&engine.shared, ns_snap, filter, &view)?,
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
            txn.stage_primary_update(
                PrimaryTarget::new(
                    entry.id,
                    ns_arc.clone(),
                    entry.data_root_page,
                    entry.data_root_level,
                ),
                key,
                new_bytes,
                expected_head,
            );
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
    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
    let mut matched: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> =
        match snap.catalog.get_by_name(ns) {
            None => return Ok(None),
            Some(ns_snap) => collect_mutation_candidates(&engine.shared, ns_snap, filter, &view)?,
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
            txn.stage_primary_delete(
                entry.id,
                ns_arc.clone(),
                entry.data_root_page,
                entry.data_root_level,
                key,
                expected_head,
            );
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
    let view = open_snapshot_read_view(&engine.shared)?;
    let snap = view.published_epoch();
    let mut matched: Vec<(Vec<u8>, Document, Option<ExpectedHead>)> =
        match snap.catalog.get_by_name(ns) {
            None => {
                if opts.upsert {
                    return upsert_for_find_one_and_replace(engine, ns, replacement, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => collect_mutation_candidates(&engine.shared, ns_snap, filter, &view)?,
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
            txn.stage_primary_update(
                PrimaryTarget::new(
                    entry.id,
                    ns_arc.clone(),
                    entry.data_root_page,
                    entry.data_root_level,
                ),
                old_key,
                new_bytes,
                expected_head,
            );
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
