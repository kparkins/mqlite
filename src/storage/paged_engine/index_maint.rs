//! Secondary-index maintenance + pending-write installation helpers.

use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::key_encoding::{encode_compound_key, COMPOUND_SEP};
use crate::mvcc::transaction::WriteTxn;
use crate::query::planner::IndexCondition;
use crate::storage::btree::{BTree, CellValue};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::IndexEntry;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::secondary_index::{
    update_index_on_delete, update_index_on_insert, update_index_on_update,
};
use crate::storage::txn_page_store::TxnOverlay;

use super::catalog_ops::{new_txn_store, sync_catalog_root_overlay};
use super::state::{MetadataState, SharedState};

/// Retrieve the serialised `_id` value stored in an index tree entry.
pub(super) fn index_entry_id_free(
    handle: &Arc<BufferPoolHandle>,
    cv: CellValue,
) -> Result<Bson> {
    let bytes = match cv {
        CellValue::Inline(b) => b,
        CellValue::Overflow {
            first_page,
            total_length,
        } => {
            let tmp_store = BufferPoolPageStore::new(Arc::clone(handle));
            let tmp_tree = BTree::open(tmp_store, 1, 0);
            tmp_tree.read_overflow(first_page, total_length)?
        }
    };
    if bytes.is_empty() {
        return Ok(Bson::Null);
    }
    let doc: Document = bson::from_slice(&bytes).map_err(Error::BsonDeserialization)?;
    Ok(doc.get("_id").cloned().unwrap_or(Bson::Null))
}

/// Build the [start, end] range for a secondary index B+ tree scan.
pub(super) fn index_bounds_free(
    condition: &IndexCondition,
    ascending: bool,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    fn prefix(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = encode_compound_key(&[(val, asc)]);
        p.push(COMPOUND_SEP);
        p
    }
    fn prefix_next(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = prefix(val, asc);
        *p.last_mut().expect("prefix always contains at least COMPOUND_SEP") += 1;
        p
    }
    match condition {
        IndexCondition::Eq(v) => {
            (Some(prefix(v, ascending)), Some(prefix_next(v, ascending)))
        }
        IndexCondition::Any => (None, None),
        IndexCondition::In(_) => (None, None),
        IndexCondition::Range { gt, gte, lt, lte } => {
            if ascending {
                let start = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, true)),
                    (None, Some(v)) => Some(prefix_next(v, true)),
                    _ => None,
                };
                let end = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, true)),
                    (None, Some(v)) => Some(prefix(v, true)),
                    _ => None,
                };
                (start, end)
            } else {
                let start = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, false)),
                    (None, Some(v)) => Some(prefix_next(v, false)),
                    _ => None,
                };
                let end = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, false)),
                    (None, Some(v)) => Some(prefix(v, false)),
                    _ => None,
                };
                (start, end)
            }
        }
    }
}

/// Persist updated root/level and multikey flag for an index entry.
pub(super) fn sync_index_entry(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    orig: &IndexEntry,
    new_root: u32,
    new_level: u8,
    new_multikey: bool,
) -> Result<()> {
    let root_changed = new_root != orig.root_page || new_level != orig.root_level;
    let multikey_changed = new_multikey && !orig.multikey;
    if !root_changed && !multikey_changed {
        return Ok(());
    }
    let mut updated = orig.clone();
    if root_changed {
        updated.root_page = new_root;
        updated.root_level = new_level;
    }
    if multikey_changed {
        updated.multikey = true;
    }
    md.catalog.lock().expect("catalog poisoned").update_index(&updated)?;
    sync_catalog_root_overlay(shared, md, overlay)
}

/// Maintain all secondary indexes after a document insert.
pub(super) fn maintain_secondary_on_insert(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog.lock().expect("catalog poisoned").list_indexes(ns)?;
    for entry in entries {
        let store = new_txn_store(shared, overlay);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let is_multikey = update_index_on_insert(doc, doc_id, &idx_tree, &entry, txn)?;
        // Extract root values before dropping idx_tree so the &mut overlay
        // borrow from its store is released before sync_index_entry borrows
        // overlay again.
        let (new_root, new_level) = (idx_tree.root_page, idx_tree.root_level);
        drop(idx_tree);
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            new_root,
            new_level,
            is_multikey,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes after a document delete.
pub(super) fn maintain_secondary_on_delete(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog.lock().expect("catalog poisoned").list_indexes(ns)?;
    for entry in entries {
        update_index_on_delete(doc, doc_id, &entry, txn)?;
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            entry.root_page,
            entry.root_level,
            false,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes when a document is replaced.
pub(super) fn maintain_secondary_on_update(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    ns: &str,
    old_doc: &Document,
    new_doc: &Document,
    old_id: &Bson,
    new_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog.lock().expect("catalog poisoned").list_indexes(ns)?;
    for entry in entries {
        let store = new_txn_store(shared, overlay);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let is_multikey = update_index_on_update(
            old_doc, new_doc, old_id, new_id, &idx_tree, &entry, txn,
        )?;
        let (new_root, new_level) = (idx_tree.root_page, idx_tree.root_level);
        drop(idx_tree);
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            new_root,
            new_level,
            is_multikey,
        )?;
    }
    Ok(())
}

/// Drain the given `SecIndexWrite` batch and perform the actual
/// `BTree::insert` / `delete` into each target index tree.
pub(super) fn install_pending_sec_index(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    writes: Vec<crate::mvcc::SecIndexWrite>,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    use crate::mvcc::SecIndexOp;
    use std::collections::HashMap as StdHashMap;

    let mut entry_by_root: StdHashMap<u32, IndexEntry> = StdHashMap::new();
    {
        let cat = md.catalog.lock().expect("catalog poisoned");
        let collections = cat.list_collections()?;
        for coll in &collections {
            for entry in cat.list_indexes(&coll.name)? {
                entry_by_root.insert(entry.root_page, entry);
            }
        }
    }

    struct TreeState {
        current_root: u32,
        current_level: u8,
        entry: IndexEntry,
    }
    let mut states: StdHashMap<u32, TreeState> = StdHashMap::new();

    for write in writes {
        let state = match states.entry(write.index_root_page) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                let entry = entry_by_root
                    .get(&write.index_root_page)
                    .cloned()
                    .ok_or_else(|| {
                        Error::Internal(format!(
                            "pending sec-index write references unknown root_page {}",
                            write.index_root_page
                        ))
                    })?;
                slot.insert(TreeState {
                    current_root: entry.root_page,
                    current_level: entry.root_level,
                    entry,
                })
            }
        };

        let store = new_txn_store(shared, overlay);
        let mut idx_tree = BTree::open(store, state.current_root, state.current_level);
        match write.op {
            SecIndexOp::Insert { id_bytes } => {
                idx_tree.insert(&write.key, &id_bytes)?;
            }
            SecIndexOp::Delete => {
                let _ = idx_tree.delete(&write.key)?;
            }
        }
        state.current_root = idx_tree.root_page;
        state.current_level = idx_tree.root_level;
    }

    for (_, state) in states {
        sync_index_entry(
            shared,
            md,
            overlay,
            &state.entry,
            state.current_root,
            state.current_level,
            state.entry.multikey,
        )?;
    }

    Ok(())
}

/// Install staged primary-tree writes as fresh heads on each key's
/// per-leaf version chain.
pub(super) fn install_pending_primary(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    writes: Vec<crate::mvcc::PrimaryWrite>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    use crate::mvcc::{PrimaryOp, Ts, VersionData, VersionEntry};
    use crate::storage::buffer_pool::PageSize;

    for write in writes {
        let (root_page, root_level) = match md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(&write.ns)?
        {
            Some(c) => (c.data_root_page, c.data_root_level),
            None => continue,
        };
        let tree = BTree::open(new_txn_store(shared, overlay), root_page, root_level);
        let leaf_page = tree.find_leaf(&write.key)?;
        let _pin = shared.handle.fetch_page(leaf_page, PageSize::Large32k)?;
        let mut chain_arc = shared
            .handle
            .pool()
            .take_chain(leaf_page, &write.key)?
            .unwrap_or_default();
        {
            let chain_mut = std::sync::Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.front_mut() {
                prev_head.stop_ts = commit_ts;
            }
            let (data, is_tombstone) = match write.op {
                PrimaryOp::Insert { data } => (VersionData::Inline(data), false),
                PrimaryOp::Update { data } => (VersionData::Inline(data), false),
                PrimaryOp::Delete => (VersionData::Inline(Vec::new()), true),
            };
            chain_mut.push_front(VersionEntry {
                start_ts: commit_ts,
                stop_ts: Ts::MAX,
                txn_id,
                data,
                is_tombstone,
            });
        }
        shared
            .handle
            .pool()
            .put_chain(leaf_page, write.key, chain_arc)?;
    }
    Ok(())
}

/// Outcome of `create_index_reserve` (reserve step of the 3-step build).
#[derive(Clone, Copy)]
pub(super) enum ReserveOutcome {
    /// A fresh Building entry was reserved; caller should proceed to
    /// the build and commit steps.
    Reserved,
    /// An index with the same name already exists; `create_index` is
    /// idempotent and returns Ok immediately.
    AlreadyExists,
}

// ---------------------------------------------------------------------------
// Engine-level index operation free functions
// ---------------------------------------------------------------------------

use crate::index::{IndexInfo, IndexModel};
use crate::storage::secondary_index::generate_index_name;

use super::doc_helpers::validate_index_keys;

pub(super) fn create_index(engine: &super::PagedEngine, ns: &str, model: &IndexModel) -> crate::error::Result<String> {
    validate_index_keys(&model.keys)?;
    let name = model
        .options
        .name
        .clone()
        .unwrap_or_else(|| generate_index_name(&model.keys));

    let reserve_outcome = engine.create_index_reserve(ns, model, &name)?;
    match reserve_outcome {
        ReserveOutcome::AlreadyExists => return Ok(name),
        ReserveOutcome::Reserved => {}
    }

    if let Err(build_err) = engine.create_index_build(ns, &name) {
        if let Err(cleanup_err) = engine.create_index_cleanup(ns, &name) {
            return Err(crate::error::Error::Internal(format!(
                "create_index build failed: {}; cleanup also failed: {}",
                build_err, cleanup_err
            )));
        }
        return Err(build_err);
    }

    engine.create_index_commit(ns, &name)?;
    Ok(name)
}

pub(super) fn drop_index(engine: &super::PagedEngine, ns: &str, name: &str) -> crate::error::Result<()> {
    if name == "_id_" {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: "drop of '_id_' index is not permitted".to_string(),
        });
    }
    let lane_arc = engine.lane_for(ns);
    let _lane_guard = engine.acquire_lane(lane_arc)?;
    engine.run_ddl(|shared, md, overlay| {
        let removed = md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .drop_index(ns, name)?;
        if removed {
            sync_catalog_root_overlay(shared, md, overlay)?;
            Ok(())
        } else {
            Err(crate::error::Error::Internal(format!(
                "index '{}' not found on '{}'",
                name, ns
            )))
        }
    })
}

pub(super) fn list_indexes(engine: &super::PagedEngine, ns: &str) -> crate::error::Result<Vec<IndexInfo>> {
    let snap = engine.shared.published.load();
    let ns_snap = match snap.namespaces.get(ns) {
        None => return Ok(Vec::new()),
        Some(n) => n,
    };
    Ok(ns_snap
        .indexes
        .iter()
        .map(|i| IndexInfo {
            name: i.name.clone(),
            keys: i.key_pattern.clone(),
            unique: i.unique,
            sparse: i.sparse,
        })
        .collect())
}
