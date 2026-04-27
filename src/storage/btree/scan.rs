//! B+ tree read paths: point lookup, MVCC-aware point lookup, range
//! scan, MVCC-aware range scan, and the root-to-leaf traversal helpers
//! shared by writers.

use std::cmp::Ordering as CmpOrdering;
use std::ops::Bound;

use crate::error::Result;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::version::{VersionData, VersionEntry};

use super::chain::read_overflow_chain;
use super::node::{InternalNode, LeafNode};
use super::{BTree, BTreePageStore, CellValue, HistoryProbe};

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
        let leaf_page = self.find_leaf(key)?;
        let (buf, _) = self.store.read_leaf(leaf_page)?;
        let node = LeafNode::parse(&buf[..])?;
        match node.binary_search(key) {
            Ok(i) => Ok(Some(node.cells[i].value.clone())),
            Err(_) => Ok(None),
        }
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
    /// on-disk cell is used — pre-MVCC keys that never got a staged write
    /// flow through the on-disk fallback.
    ///
    /// Not yet called from the engine's reader paths — those route through
    /// `range_scan_mvcc` via `btree_collscan`.
    #[allow(dead_code)]
    pub(crate) fn get_mvcc(
        &self,
        key: &[u8],
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Option<Vec<u8>>> {
        let leaf_page = self.find_leaf(key)?;
        let (buf, snap) = self.store.read_leaf(leaf_page)?;
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
                if let Some(entry) = probe.probe(key, view.read_ts)? {
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
        // Fall back to the on-disk cell (dual-write intermediate).
        let node = LeafNode::parse(&buf[..])?;
        match node.binary_search(key) {
            Ok(i) => match &node.cells[i].value {
                CellValue::Inline(v) => Ok(Some(v.clone())),
                CellValue::Overflow {
                    first_page,
                    total_length,
                } => Ok(Some(read_overflow_chain(
                    &self.store,
                    *first_page,
                    *total_length,
                )?)),
            },
            Err(_) => Ok(None),
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
        let first_leaf = match start_key {
            Some(k) => self.find_leaf(k)?,
            None => self.leftmost_leaf()?,
        };

        let mut cur_page = first_leaf;
        'outer: while cur_page != 0 {
            let (buf, _) = self.store.read_leaf(cur_page)?;
            let node = LeafNode::parse(&buf[..])?;

            let start_idx = match start_key {
                Some(k) => match node.binary_search(k) {
                    Ok(i) => i,
                    Err(i) => i,
                },
                None => 0,
            };

            for i in start_idx..node.cells.len() {
                let cell = &node.cells[i];
                if let Some(ek) = end_key {
                    if cell.key.as_slice() > ek {
                        break 'outer;
                    }
                }
                results.push((cell.key.clone(), cell.value.clone()));
            }

            cur_page = node.next_leaf_page;
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

        let first_leaf = match start {
            Bound::Included(k) | Bound::Excluded(k) => self.find_leaf(k)?,
            Bound::Unbounded => self.leftmost_leaf()?,
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

        let mut cur_page = first_leaf;
        'outer: while cur_page != 0 {
            let (buf, snap) = self.store.read_leaf(cur_page)?;
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

                let Some(source) = merge_source(base_key, chain_key) else {
                    break;
                };

                if end_excludes_key(end, source.key()) {
                    break 'outer;
                }

                match source {
                    MergeSource::Base(_) => {
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
                                let maybe_entry = probe.probe(&cell.key, view.read_ts)?;
                                if let Some(entry) = maybe_entry {
                                    if !entry.is_tombstone {
                                        let bytes = resolve_entry(&entry)?;
                                        results.push((cell.key.clone(), bytes));
                                    }
                                    continue;
                                }
                            }
                        }
                        results.push((cell.key.clone(), resolve_cell(&cell.value)?));
                    }
                    MergeSource::Chain(_) => {
                        let next = chain_iter.as_mut().and_then(|iter| iter.next());
                        let Some((key, entry)) = next else {
                            break;
                        };
                        if !entry.is_tombstone {
                            results.push((key.to_vec(), resolve_entry(entry)?));
                        }
                    }
                    MergeSource::Both(_) => {
                        let Some(cell) = base_iter.next() else {
                            break;
                        };
                        let next = chain_iter.as_mut().and_then(|iter| iter.next());
                        let Some((_, entry)) = next else {
                            break;
                        };
                        if !entry.is_tombstone {
                            results.push((cell.key.clone(), resolve_entry(entry)?));
                        }
                    }
                }
            }

            cur_page = node.next_leaf_page;
        }

        Ok(results)
    }

    /// Return visible resident delta entries without base-only cells.
    ///
    /// Checkpoint uses this to fold committed delta heads into durable page
    /// images without treating the base page as visibility authority.
    pub(crate) fn visible_delta_entries(
        &self,
        view: &ReadView,
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let mut results = Vec::new();
        let mut cur_page = self.leftmost_leaf()?;

        while cur_page != 0 {
            let (buf, snap) = self.store.read_leaf(cur_page)?;
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
            cur_page = node.next_leaf_page;
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Private helpers (shared with writers)
    // -----------------------------------------------------------------------

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

    /// Follow leftmost child pointers from the root to reach the
    /// leftmost leaf page.
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
enum MergeSource<'a> {
    Base(&'a [u8]),
    Chain(&'a [u8]),
    Both(&'a [u8]),
}

impl<'a> MergeSource<'a> {
    fn key(self) -> &'a [u8] {
        match self {
            MergeSource::Base(key) => key,
            MergeSource::Chain(key) => key,
            MergeSource::Both(key) => key,
        }
    }
}

fn merge_source<'a>(base: Option<&'a [u8]>, chain: Option<&'a [u8]>) -> Option<MergeSource<'a>> {
    match (base, chain) {
        (None, None) => None,
        (Some(base), None) => Some(MergeSource::Base(base)),
        (None, Some(chain)) => Some(MergeSource::Chain(chain)),
        (Some(base), Some(chain)) => match base.cmp(chain) {
            CmpOrdering::Less => Some(MergeSource::Base(base)),
            CmpOrdering::Equal => Some(MergeSource::Both(base)),
            CmpOrdering::Greater => Some(MergeSource::Chain(chain)),
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
