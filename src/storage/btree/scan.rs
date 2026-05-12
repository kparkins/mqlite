//! B+ tree read paths: point lookup, MVCC-aware point lookup, range
//! scan, MVCC-aware range scan, and the root-to-leaf traversal helpers
//! shared by writers.

use std::cmp::Ordering as CmpOrdering;
use std::ops::Bound;

use crate::error::{Error, Result};
use crate::mvcc::read_view::{ChainSnapshot, ReadView};
use crate::mvcc::version::{VersionData, VersionEntry};
use crate::storage::buffer_pool::PageSize;
use crate::storage::page::{OverflowPageHeader, OVERFLOW_HEADER_SIZE, PAGE_SIZE_LEAF};

use super::node::{InternalNode, LeafNode};
use super::{BTree, BTreePageStore, CellValue, HistoryProbe, LeafPageImage};

// ---------------------------------------------------------------------------
// Overflow read helper
// ---------------------------------------------------------------------------

pub(crate) fn read_overflow_chain<S: BTreePageStore>(
    store: &S,
    first_page: u32,
    total_length: u32,
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(total_length as usize);
    let mut cur = first_page;
    while cur != 0 {
        let (buf, _) = store.read_leaf(cur)?;
        let hdr = OverflowPageHeader::from_bytes(&buf[..])?;
        hdr.validate_type()?;
        let data_len = hdr.data_length as usize;
        if OVERFLOW_HEADER_SIZE + data_len > PAGE_SIZE_LEAF as usize {
            return Err(Error::Internal(format!(
                "overflow page {cur}: data_length {data_len} exceeds page size"
            )));
        }
        result.extend_from_slice(&buf[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + data_len]);
        cur = hdr.next_overflow_page;
    }
    result.truncate(total_length as usize);
    Ok(result)
}

/// A visible delta key paired with `Some(value)` for live versions or `None`
/// for tombstones.
pub(crate) type VisibleDeltaEntry = (Vec<u8>, Option<Vec<u8>>);
type LeafRead = (LeafPageImage, Option<ChainSnapshot>);

impl<S: BTreePageStore> BTree<S> {
    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    /// Search for `key`, returning the value if found.
    ///
    /// If the value is an overflow pointer, the raw bytes are **not** read here
    /// (the caller must call [`BTree::read_overflow`] explicitly).  Use
    /// [`BTree::get`] for a fully resolved lookup.
    pub(crate) fn search(&self, key: &[u8]) -> Result<Option<CellValue>> {
        let (_, (buf, _)) = self.read_leaf_for_point_key(key)?;
        LeafNode::cell_value(&buf[..], key)
    }

    /// Like [`BTree::search`] but resolves overflow pointers, returning the raw
    /// BSON bytes for all cases.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.search(key)? {
            None => Ok(None),
            Some(CellValue::Inline(v)) => Ok(Some(v)),
            Some(CellValue::Overflow {
                first_page,
                total_length,
            }) => Ok(Some(read_overflow_chain(
                &self.store,
                first_page,
                total_length,
            )?)),
        }
    }

    /// Read a previously written overflow chain starting at `first_page`.
    pub(crate) fn read_overflow(&self, first_page: u32, total_length: u32) -> Result<Vec<u8>> {
        read_overflow_chain(&self.store, first_page, total_length)
    }

    /// MVCC-aware point lookup.
    ///
    /// Consults the owning leaf frame's version chain via `ChainSnapshot`
    /// first; if a [`VersionEntry`] visible to `view` exists for `key`,
    /// its payload is returned (respecting `is_tombstone`). Otherwise the
    /// on-disk cell is used for pre-MVCC keys that never got a staged write.
    pub(crate) fn get_mvcc(
        &self,
        key: &[u8],
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Option<Vec<u8>>> {
        let (_, (buf, snap)) = self.read_leaf_for_point_key(key)?;
        if let Some(snap) = snap.as_ref() {
            if let Some(entry) = snap.visible_at(key, view) {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(Some(match &entry.data {
                    VersionData::Inline(v) => v.clone(),
                    VersionData::Overflow(oref) => read_overflow_chain(
                        &self.store,
                        oref.first_page(),
                        oref.total_length() as u32,
                    )?,
                }));
            }
        }
        // History fallthrough: the chain had no entry visible at
        // `view.read_ts` — an evicted entry in the history store might.
        let history_is_candidate = snap
            .as_ref()
            .map_or(true, |snap| snap.history_is_candidate(key, view.read_ts));
        if history_is_candidate {
            if let Some(probe) = history {
                if let Some(entry) = probe.probe_visible_version(key, view.read_ts)? {
                    if entry.is_tombstone {
                        return Ok(None);
                    }
                    return Ok(Some(match &entry.data {
                        VersionData::Inline(v) => v.clone(),
                        VersionData::Overflow(oref) => read_overflow_chain(
                            &self.store,
                            oref.first_page(),
                            oref.total_length() as u32,
                        )?,
                    }));
                }
            }
        }
        match LeafNode::cell_value(&buf[..], key)? {
            Some(CellValue::Inline(v)) => Ok(Some(v)),
            Some(CellValue::Overflow {
                first_page,
                total_length,
            }) => Ok(Some(read_overflow_chain(
                &self.store,
                first_page,
                total_length,
            )?)),
            None => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // Range scan
    // -----------------------------------------------------------------------

    /// Collect all `(key, value)` pairs in the range `[start_key, end_key]`.
    ///
    /// Both bounds are optional (use `None` for an unbounded side).  Keys are
    /// returned in ascending order following leaf sibling pointers.
    ///
    /// Overflow values are **not** resolved here; the caller receives
    /// [`CellValue::Overflow`] pointers and can call [`BTree::read_overflow`]
    /// to fetch the data.
    pub(crate) fn range_scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, CellValue)>> {
        let mut results = Vec::new();

        // Find the first leaf that might contain start_key.
        let (_, mut leaf_read) = match start_key {
            Some(k) => self.read_leaf_for_key(k)?,
            None => self.read_leftmost_leaf_latch_coupled()?,
        };

        loop {
            let (buf, _) = leaf_read;
            let node = LeafNode::parse(&buf[..])?;

            let start_idx = match start_key {
                Some(k) => node.binary_search(k).unwrap_or_else(|i| i),
                None => 0,
            };

            for i in start_idx..node.cells.len() {
                let cell = &node.cells[i];
                if let Some(ek) = end_key {
                    if cell.key.as_slice() > ek {
                        return Ok(results);
                    }
                }
                results.push((cell.key.clone(), cell.value.clone()));
            }

            let cur_page = node.next_leaf_page;
            if cur_page == 0 {
                break;
            }
            leaf_read = self.store.read_leaf(cur_page)?;
        }

        Ok(results)
    }

    /// MVCC-aware range scan using legacy inclusive optional bounds.
    ///
    /// Preserves the historical `[start_key, end_key]` semantics by delegating
    /// to [`BTree::range_scan_mvcc_bounded`] with included bounds.
    pub(crate) fn range_scan_mvcc(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_scan_mvcc_bounded(
            start_key.map_or(Bound::Unbounded, Bound::Included),
            end_key.map_or(Bound::Unbounded, Bound::Included),
            view,
            history,
        )
    }

    /// MVCC-aware range scan with explicit bound semantics.
    ///
    /// Walks sibling leaves like [`BTree::range_scan`], but each leaf is
    /// produced by merging two ordered sources: base cells from the page image
    /// and visible resident delta chains from the frame snapshot. For equal
    /// keys, the chain entry wins; visible tombstones suppress the key.
    ///
    /// Unlike [`BTree::range_scan_mvcc`], which preserves the historical
    /// inclusive end bound, this method honors [`Bound::Excluded`] for callers
    /// such as secondary unique-prefix scans.
    pub(crate) fn range_scan_mvcc_bounded(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        self.try_for_each_range_scan_mvcc_bounded(start, end, view, history, |key, value| {
            results.push((key, value));
            Ok(true)
        })?;
        Ok(results)
    }

    /// MVCC-aware range scan with a caller-controlled stop condition.
    pub(crate) fn try_for_each_range_scan_mvcc_bounded<F>(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>, Vec<u8>) -> Result<bool>,
    {
        let (_, mut leaf_read) = match start {
            Bound::Included(k) | Bound::Excluded(k) => self.read_leaf_for_key(k)?,
            Bound::Unbounded => self.read_leftmost_leaf_latch_coupled()?,
        };

        let resolve_entry = |entry: &VersionEntry| -> Result<Vec<u8>> {
            match &entry.data {
                VersionData::Inline(v) => Ok(v.clone()),
                VersionData::Overflow(oref) => {
                    let total_length = oref.total_length() as u32;
                    read_overflow_chain(&self.store, oref.first_page(), total_length)
                }
            }
        };
        let resolve_cell = |value: &CellValue| -> Result<Vec<u8>> {
            match value {
                CellValue::Inline(v) => Ok(v.clone()),
                CellValue::Overflow {
                    first_page,
                    total_length,
                } => read_overflow_chain(&self.store, *first_page, *total_length),
            }
        };

        loop {
            let (buf, snap) = leaf_read;
            let node = LeafNode::parse(&buf[..])?;

            let start_idx = base_start_index(&node, start);
            let mut base_iter = node.cells[start_idx..].iter().peekable();
            let mut chain_iter = snap
                .as_ref()
                .map(|snapshot| snapshot.visible_range(start, end, view).peekable());

            loop {
                let base_key = base_iter.peek().map(|cell| cell.key.as_slice());
                let chain_key = chain_iter
                    .as_mut()
                    .and_then(|iter| iter.peek().map(|(key, _)| *key));

                let Some((source, source_key)) = merge_source(base_key, chain_key) else {
                    break;
                };

                if end_excludes_key(end, source_key) {
                    return Ok(());
                }

                match source {
                    MergeSource::Base => {
                        let Some(cell) = base_iter.next() else {
                            break;
                        };
                        let history_is_candidate = match snap.as_ref() {
                            Some(snapshot) => {
                                snapshot.history_is_candidate(&cell.key, view.read_ts)
                            }
                            None => true,
                        };
                        if history_is_candidate {
                            if let Some(probe) = history {
                                let maybe_entry =
                                    probe.probe_visible_version(&cell.key, view.read_ts)?;
                                if let Some(entry) = maybe_entry {
                                    if !entry.is_tombstone {
                                        let bytes = resolve_entry(&entry)?;
                                        if !visit(cell.key.clone(), bytes)? {
                                            return Ok(());
                                        }
                                    }
                                    continue;
                                }
                            }
                        }
                        if !visit(cell.key.clone(), resolve_cell(&cell.value)?)? {
                            return Ok(());
                        }
                    }
                    MergeSource::Chain => {
                        let next = chain_iter.as_mut().and_then(|iter| iter.next());
                        let Some((key, entry)) = next else {
                            break;
                        };
                        if !entry.is_tombstone && !visit(key.to_vec(), resolve_entry(entry)?)? {
                            return Ok(());
                        }
                    }
                    MergeSource::Both => {
                        let Some(cell) = base_iter.next() else {
                            break;
                        };
                        let next = chain_iter.as_mut().and_then(|iter| iter.next());
                        let Some((_, entry)) = next else {
                            break;
                        };
                        if !entry.is_tombstone && !visit(cell.key.clone(), resolve_entry(entry)?)? {
                            return Ok(());
                        }
                    }
                }
            }

            let cur_page = node.next_leaf_page;
            if cur_page == 0 {
                break;
            }
            leaf_read = self.store.read_leaf(cur_page)?;
        }

        Ok(())
    }

    /// Return visible resident delta entries without base-only cells.
    ///
    /// Checkpoint uses this to fold committed delta heads into durable page
    /// images without treating the base page as visibility authority.
    pub(crate) fn visible_delta_entries(&self, view: &ReadView) -> Result<Vec<VisibleDeltaEntry>> {
        let mut results = Vec::new();
        let (_, mut leaf_read) = self.read_leftmost_leaf_latch_coupled()?;

        loop {
            let (buf, snap) = leaf_read;
            let node = LeafNode::parse(&buf[..])?;
            if let Some(snapshot) = snap.as_ref() {
                for (key, entry) in snapshot.visible_range(Bound::Unbounded, Bound::Unbounded, view)
                {
                    let value = if entry.is_tombstone {
                        None
                    } else {
                        Some(match &entry.data {
                            VersionData::Inline(v) => v.clone(),
                            VersionData::Overflow(oref) => read_overflow_chain(
                                &self.store,
                                oref.first_page(),
                                oref.total_length() as u32,
                            )?,
                        })
                    };
                    results.push((key.to_vec(), value));
                }
            }
            let cur_page = node.next_leaf_page;
            if cur_page == 0 {
                break;
            }
            leaf_read = self.store.read_leaf(cur_page)?;
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Private helpers (shared with writers)
    // -----------------------------------------------------------------------

    /// Traverse root-to-leaf with reader latch coupling and read the leaf
    /// while the leaf shared latch is still held.
    fn read_leaf_for_key(&self, key: &[u8]) -> Result<(u32, LeafRead)> {
        let mut page = self.root_page;
        let mut level = self.root_level;
        let mut guard = self
            .store
            .pin_shared_for_read(page, page_size_for_level(level))?;
        record_reader_shared_acquire(page, level);

        while level > 0 {
            let buf = self.store.read_internal_guarded(page, &guard)?;
            let node = InternalNode::parse(&buf[..])?;
            let child_page = node.find_child(key);
            let child_level = level - 1;
            let child_guard = self
                .store
                .pin_shared_for_read(child_page, page_size_for_level(child_level))?;
            record_reader_shared_acquire(child_page, child_level);
            record_reader_parent_release_after_child(page, child_page);
            drop(guard);
            guard = child_guard;
            page = child_page;
            level = child_level;
        }

        let leaf = self.store.read_leaf_guarded(page, &guard)?;
        drop(guard);
        pause_before_iteration()?;
        Ok((page, leaf))
    }

    /// Traverse root-to-leaf for a point read and snapshot only `key`'s
    /// resident delta chain while the leaf shared latch is still held.
    fn read_leaf_for_point_key(&self, key: &[u8]) -> Result<(u32, LeafRead)> {
        let mut page = self.root_page;
        let mut level = self.root_level;
        let mut guard = self
            .store
            .pin_shared_for_read(page, page_size_for_level(level))?;
        record_reader_shared_acquire(page, level);

        while level > 0 {
            let buf = self.store.read_internal_guarded(page, &guard)?;
            let node = InternalNode::parse(&buf[..])?;
            let child_page = node.find_child(key);
            let child_level = level - 1;
            let child_guard = self
                .store
                .pin_shared_for_read(child_page, page_size_for_level(child_level))?;
            record_reader_shared_acquire(child_page, child_level);
            record_reader_parent_release_after_child(page, child_page);
            drop(guard);
            guard = child_guard;
            page = child_page;
            level = child_level;
        }

        let leaf = self.store.read_leaf_for_key_guarded(page, &guard, key)?;
        drop(guard);
        pause_before_iteration()?;
        Ok((page, leaf))
    }

    /// Follow the leftmost path with reader latch coupling and copy the leaf
    /// while the leaf shared latch is still held.
    fn read_leftmost_leaf_latch_coupled(&self) -> Result<(u32, LeafRead)> {
        let mut page = self.root_page;
        let mut level = self.root_level;
        let mut guard = self
            .store
            .pin_shared_for_read(page, page_size_for_level(level))?;
        record_reader_shared_acquire(page, level);

        while level > 0 {
            let buf = self.store.read_internal_guarded(page, &guard)?;
            let node = InternalNode::parse(&buf[..])?;
            let child_page = if node.entries.is_empty() {
                node.rightmost_child
            } else {
                node.entries[0].1
            };
            let child_level = level - 1;
            let child_guard = self
                .store
                .pin_shared_for_read(child_page, page_size_for_level(child_level))?;
            record_reader_shared_acquire(child_page, child_level);
            record_reader_parent_release_after_child(page, child_page);
            drop(guard);
            guard = child_guard;
            page = child_page;
            level = child_level;
        }

        let leaf = self.store.read_leaf_guarded(page, &guard)?;
        drop(guard);
        pause_before_iteration()?;
        Ok((page, leaf))
    }

    /// Traverse from the root to the leaf page that should contain `key`.
    pub(crate) fn find_leaf(&self, key: &[u8]) -> Result<u32> {
        let mut page = self.root_page;
        let mut level = self.root_level;

        while level > 0 {
            let buf = self.store.read_internal(page)?;
            let node = InternalNode::parse(&buf[..])?;
            page = node.find_child(key);
            level -= 1;
        }

        Ok(page)
    }

    /// Traverse from the root to the leaf page for `key`, retaining parent
    /// linkage for post-latch structural revalidation.
    pub(crate) fn path_to_leaf(&self, key: &[u8]) -> Result<Vec<super::BTreePathStep>> {
        let mut path = Vec::new();
        let mut page = self.root_page;
        let mut level = self.root_level;
        let mut parent_page = None;
        let mut child_slot = None;

        loop {
            path.push(super::BTreePathStep {
                page_id: page,
                parent_page,
                child_slot,
                level,
            });

            if level == 0 {
                return Ok(path);
            }

            let buf = self.store.read_internal(page)?;
            let node = InternalNode::parse(&buf[..])?;
            let idx = node.find_child_idx(key);
            let child_page = node.child_at(idx);
            parent_page = Some(page);
            child_slot = Some(idx);
            page = child_page;
            level -= 1;
        }
    }

    /// Verify that a previously planned root-to-leaf path still resolves to
    /// the same pages and child slots.
    pub(crate) fn revalidate_path(
        &self,
        key: &[u8],
        expected: &[super::BTreePathStep],
    ) -> Result<bool> {
        Ok(self.path_to_leaf(key)? == expected)
    }

    /// Follow leftmost child pointers from the root to reach the
    /// leftmost leaf page.
    #[allow(dead_code, reason = "unit tests and tree invariants use this helper")]
    pub(super) fn leftmost_leaf(&self) -> Result<u32> {
        let mut page = self.root_page;
        let mut level = self.root_level;

        while level > 0 {
            let buf = self.store.read_internal(page)?;
            let node = InternalNode::parse(&buf[..])?;
            // Follow leftmost child.
            page = if node.entries.is_empty() {
                node.rightmost_child
            } else {
                node.entries[0].1
            };
            level -= 1;
        }

        Ok(page)
    }
}

#[derive(Clone, Copy)]
enum MergeSource {
    Base,
    Chain,
    Both,
}

fn merge_source<'a>(
    base: Option<&'a [u8]>,
    chain: Option<&'a [u8]>,
) -> Option<(MergeSource, &'a [u8])> {
    match (base, chain) {
        (None, None) => None,
        (Some(base), None) => Some((MergeSource::Base, base)),
        (None, Some(chain)) => Some((MergeSource::Chain, chain)),
        (Some(base), Some(chain)) => match base.cmp(chain) {
            CmpOrdering::Less => Some((MergeSource::Base, base)),
            CmpOrdering::Equal => Some((MergeSource::Both, base)),
            CmpOrdering::Greater => Some((MergeSource::Chain, chain)),
        },
    }
}

fn base_start_index(node: &LeafNode, start: Bound<&[u8]>) -> usize {
    match start {
        Bound::Unbounded => 0,
        Bound::Included(key) => node.binary_search(key).unwrap_or_else(|index| index),
        Bound::Excluded(key) => match node.binary_search(key) {
            Ok(index) => index + 1,
            Err(index) => index,
        },
    }
}

fn end_excludes_key(end: Bound<&[u8]>, key: &[u8]) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Included(end_key) => key > end_key,
        Bound::Excluded(end_key) => key >= end_key,
    }
}

fn page_size_for_level(level: u8) -> PageSize {
    if level == 0 {
        PageSize::Large32k
    } else {
        PageSize::Small4k
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn record_reader_shared_acquire(page_id: u32, level: u8) {
    super::reader_crabbing_observations::record_shared_acquire(page_id, level);
}

#[cfg(not(any(test, feature = "test-hooks")))]
fn record_reader_shared_acquire(_page_id: u32, _level: u8) {}

#[cfg(any(test, feature = "test-hooks"))]
fn record_reader_parent_release_after_child(parent_page: u32, child_page: u32) {
    super::reader_crabbing_observations::record_parent_release_after_child(parent_page, child_page);
}

#[cfg(not(any(test, feature = "test-hooks")))]
fn record_reader_parent_release_after_child(_parent_page: u32, _child_page: u32) {}

#[cfg(any(test, feature = "test-hooks"))]
fn pause_before_iteration() -> Result<()> {
    super::range_scan_latch_scope::pause_before_iteration()
}

#[cfg(not(any(test, feature = "test-hooks")))]
fn pause_before_iteration() -> Result<()> {
    Ok(())
}
