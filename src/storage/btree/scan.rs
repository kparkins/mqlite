//! B+ tree read paths: point lookup, MVCC-aware point lookup, range
//! scan, MVCC-aware range scan, and the root-to-leaf traversal helpers
//! shared by writers.

use crate::error::Result;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::version::VersionData;

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

    /// MVCC-aware point lookup (T5' sub-step 3).
    ///
    /// Consults the owning leaf frame's version chain via `ChainSnapshot`
    /// first; if a [`VersionEntry`] visible to `view` exists for `key`,
    /// its payload is returned (respecting `is_tombstone`). Otherwise the
    /// on-disk cell is used — this is the dual-write intermediate state
    /// (T5' has both the in-memory chain and the on-disk cell; T6
    /// reconciliation will collapse them). Pre-MVCC keys that never got a
    /// staged write flow through the on-disk fallback.
    ///
    /// Not yet called from the engine's reader paths — those route through
    /// `range_scan_mvcc` via `btree_collscan`. Kept as a T5' acceptance
    /// deliverable and for future point-lookup fast-paths (T6+).
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
        // Plan §T7: history fallthrough. The chain had no entry visible at
        // `view.read_ts` — an evicted entry in the history store might.
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

    /// MVCC-aware range scan (T5' sub-step 3).
    ///
    /// Walks sibling leaves like [`BTree::range_scan`], but for each
    /// candidate cell consults the frame's `ChainSnapshot` via
    /// [`ChainSnapshot::visible_at`]: a visible [`VersionEntry`] wins
    /// (returning its resolved inline/overflow bytes, or skipping on
    /// tombstone); otherwise the on-disk cell value is yielded.
    ///
    /// Unlike the legacy `range_scan` which hands back `CellValue`
    /// placeholders for overflow payloads, this path fully resolves every
    /// row to `Vec<u8>` so chain-sourced and cell-sourced values share one
    /// shape at the call site. Keys are returned in ascending order.
    pub(crate) fn range_scan_mvcc(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        let first_leaf = match start_key {
            Some(k) => self.find_leaf(k)?,
            None => self.leftmost_leaf()?,
        };

        let mut cur_page = first_leaf;
        'outer: while cur_page != 0 {
            let (buf, snap) = self.store.read_leaf(cur_page)?;
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

                // Chain-first: a visible VersionEntry wins over the on-disk
                // cell. If the entry is a tombstone, skip the key entirely.
                let chain_hit = snap
                    .as_ref()
                    .and_then(|s| s.visible_at(&cell.key, view));
                if let Some(entry) = chain_hit {
                    if entry.is_tombstone {
                        continue;
                    }
                    let bytes = match &entry.data {
                        VersionData::Inline(v) => v.clone(),
                        VersionData::Overflow(oref) => read_overflow_chain(
                            &self.store,
                            oref.first_page(),
                            oref.total_length() as u32,
                        )?,
                    };
                    results.push((cell.key.clone(), bytes));
                    continue;
                }

                // Plan §T7: history fallthrough before falling back to the
                // on-disk cell. A visible evicted entry in the history store
                // is preferred over the cell (which reflects the latest
                // committed baseline, not necessarily visible at `read_ts`).
                if let Some(probe) = history {
                    if let Some(entry) = probe.probe(&cell.key, view.read_ts)? {
                        if entry.is_tombstone {
                            continue;
                        }
                        let bytes = match &entry.data {
                            VersionData::Inline(v) => v.clone(),
                            VersionData::Overflow(oref) => read_overflow_chain(
                                &self.store,
                                oref.first_page(),
                                oref.total_length() as u32,
                            )?,
                        };
                        results.push((cell.key.clone(), bytes));
                        continue;
                    }
                }

                // Fall back to the on-disk cell.
                let bytes = match &cell.value {
                    CellValue::Inline(v) => v.clone(),
                    CellValue::Overflow {
                        first_page,
                        total_length,
                    } => read_overflow_chain(&self.store, *first_page, *total_length)?,
                };
                results.push((cell.key.clone(), bytes));
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
