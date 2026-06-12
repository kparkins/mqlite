//! `ChainSnapshot` — reader-path snapshot of a frame's per-key version chains.
//!
//! Construction deep-clones every `VersionEntry`, pinning each entry's backing
//! overflow chain for the snapshot's lifetime under the force-expiry handoff
//! protocol documented on [`ChainSnapshot`]. The [`version_visible_to`]
//! predicate decides MVCC visibility for each entry against the holding
//! [`ReadView`].

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::mvcc::read_view::ReadView;
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{VersionEntry, VersionState};

/// Reader-side snapshot of a leaf frame's per-key version chains.
///
/// Construction deep-clones every `VersionEntry` in every chain, which runs
/// `OverflowRef::Clone` (CAS-loop incref) on each `VersionData::Overflow`.
/// Every entry observed through the snapshot is therefore pinned — its
/// backing overflow chain cannot be freed while the snapshot is live.
///
/// Drop follows the default Rust drop-glue: the outer map drops each
/// `VecDeque<VersionEntry>`, which drops every contained `VersionEntry`,
/// which in turn runs `OverflowRef::Drop` (atomic decref + deferred-free
/// enqueue on 0).
///
/// **Force-expiry contract:**
///
/// 1. `new` checks `view.poisoned` BEFORE taking any refcount bumps. If
///    poisoned, it returns an empty snapshot (no `fetch_add`, no clones).
/// 2. `new` takes `pin_ops_in_flight.fetch_add(1, Release)`, performs the
///    deep clone (each entry's refcount bumped), then re-checks
///    `poisoned` under an `Acquire` load and decrements
///    `pin_ops_in_flight`. If poisoned-after, the cloned chains are
///    dropped here — RAII decrefs every bumped entry so the net refcount
///    delta is zero.
/// 3. No explicit `Drop` impl: ordinary drop glue suffices because
///    `force_expire` does NOT walk snapshot pins. Every refcount bump has
///    a matching decref through a single code path.
pub struct ChainSnapshot {
    /// Deep-cloned per-key chains. Each `VecDeque<VersionEntry>` is owned
    /// exclusively by this snapshot; the `VersionEntry` values inside each
    /// `VecDeque` were cloned from the source (running `OverflowRef::Clone`
    /// for `VersionData::Overflow` entries).
    chains: BTreeMap<Vec<u8>, VecDeque<VersionEntry>>,
    /// Back-reference to the owning reader's `ReadView`, used for the
    /// poison check during `new`. `None` for standalone callers (primarily
    /// tests that exercise snapshot visibility without a registry).
    view: Option<Arc<ReadView>>,
}

impl std::fmt::Debug for ChainSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainSnapshot")
            .field("num_keys", &self.chains.len())
            .field("view_attached", &self.view.is_some())
            .finish()
    }
}

impl ChainSnapshot {
    /// Construct a snapshot from a frame's per-key version chains.
    ///
    /// Deep-clones every entry (bumping overflow refcounts via
    /// `OverflowRef::Clone`) under the atomic-handoff protocol. See
    /// type-level docs for the poison contract.
    #[must_use]
    pub fn new(
        source: &BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
        view: Option<Arc<ReadView>>,
    ) -> Self {
        // Pre-check: if the owning view is already poisoned, refuse to
        // pin any entries. The empty snapshot is the "force-expired view
        // sees nothing" contract.
        if let Some(v) = &view {
            if v.poisoned.load(Ordering::Acquire) {
                return ChainSnapshot {
                    chains: BTreeMap::new(),
                    view,
                };
            }
            v.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        }

        // Deep clone: each inner `VersionEntry::clone()` runs
        // `OverflowRef::clone()` which is the CAS-loop incref.
        let mut chains = BTreeMap::new();
        for (k, chain) in source {
            let cloned: VecDeque<VersionEntry> = chain.iter().cloned().collect();
            chains.insert(k.clone(), cloned);
        }

        // Re-check poison AFTER the bumps. If force-expiry fired while we
        // were cloning, drop the cloned chains here — RAII decrefs every
        // entry we just bumped so the net refcount delta is zero.
        if let Some(v) = &view {
            let poisoned_after = v.poisoned.load(Ordering::Acquire);
            v.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
            if poisoned_after {
                return ChainSnapshot {
                    chains: BTreeMap::new(),
                    view,
                };
            }
        }

        ChainSnapshot { chains, view }
    }

    /// Construct a snapshot containing only the resident chain for `key`.
    ///
    /// Point reads only need the chain for the searched key. Cloning the
    /// entire leaf's delta map on every point lookup recreates the same
    /// per-reader allocation pressure as copying full page images.
    #[must_use]
    pub fn new_for_key(
        source: &BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
        key: &[u8],
        view: Option<Arc<ReadView>>,
    ) -> Self {
        if let Some(v) = &view {
            if v.poisoned.load(Ordering::Acquire) {
                return ChainSnapshot {
                    chains: BTreeMap::new(),
                    view,
                };
            }
            v.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        }

        let mut chains = BTreeMap::new();
        if let Some(chain) = source.get(key) {
            chains.insert(key.to_vec(), chain.iter().cloned().collect());
        }

        if let Some(v) = &view {
            let poisoned_after = v.poisoned.load(Ordering::Acquire);
            v.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
            if poisoned_after {
                return ChainSnapshot {
                    chains: BTreeMap::new(),
                    view,
                };
            }
        }

        ChainSnapshot { chains, view }
    }

    /// Find the entry in the chain for `key` visible at `view.read_ts`.
    ///
    /// Visibility rule:
    /// - Own pending entry: visible by matching `txn_id`.
    /// - Foreign pending entry: same timestamp window and
    ///   `start_ts <= view.sequencer_frontier()`.
    /// - Committed entry: `start_ts <= read_ts < stop_ts`.
    /// - Aborted entry: skipped.
    #[must_use]
    pub fn visible_at(&self, key: &[u8], view: &ReadView) -> Option<&VersionEntry> {
        self.chains
            .get(key)
            .and_then(|chain| chain.iter().find(|entry| version_visible_to(entry, view)))
    }

    /// Iterate visible `(key, entry)` pairs within the supplied byte bounds.
    ///
    /// Uses the same visibility predicate as [`Self::visible_at`].
    pub fn visible_range<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        view: &'a ReadView,
    ) -> impl Iterator<Item = (&'a [u8], &'a VersionEntry)> + 'a {
        self.chains
            .range::<[u8], _>((start, end))
            .filter_map(move |(key, chain)| {
                chain
                    .iter()
                    .find(|entry| version_visible_to(entry, view))
                    .map(|entry| (key.as_slice(), entry))
            })
    }

    /// True when history can contain a useful version for `key` at `read_ts`.
    #[must_use]
    pub fn history_is_candidate(&self, key: &[u8], read_ts: Ts) -> bool {
        self.chains.get(key).map_or(true, |chain| {
            chain.iter().all(|entry| {
                entry.start_ts > read_ts || matches!(entry.state, VersionState::Pending { .. })
            })
        })
    }

    /// Iterate, in ascending key order, the keys within `start..end` whose
    /// resident chain holds NO entry visible at `view` yet remains a history
    /// candidate (every entry is newer than `read_ts` or still `Pending`).
    ///
    /// These are exactly the delta-only-but-not-visible keys the MVCC *point*
    /// read surfaces through its history fallthrough: [`Self::visible_at`]
    /// misses them, so [`Self::visible_range`] never yields them, yet
    /// [`Self::history_is_candidate`] is true so the point read probes history
    /// for them. A range scan that merges only base cells and
    /// [`Self::visible_range`] would silently drop them; this accessor lets the
    /// scan enumerate them as a third merge source so it can probe history the
    /// same way [`crate::storage::btree`]'s `get_mvcc` does.
    ///
    /// A key is excluded when it has any visible entry (the scan already
    /// surfaces it through [`Self::visible_range`]) or when it is not a history
    /// candidate (a chain entry at or before `read_ts` means history cannot
    /// hold a newer useful version). The yielded key may also have a base cell
    /// on the owning leaf — the scan must dedup against base cells so a key
    /// with a base cell is probed only once (through its base-cell arm).
    pub fn history_candidate_keys_without_visible_entry<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        view: &'a ReadView,
        read_ts: Ts,
    ) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.chains
            .range::<[u8], _>((start, end))
            .filter_map(move |(key, chain)| {
                let has_visible = chain.iter().any(|entry| version_visible_to(entry, view));
                if has_visible {
                    return None;
                }
                let is_candidate = chain.iter().all(|entry| {
                    entry.start_ts > read_ts || matches!(entry.state, VersionState::Pending { .. })
                });
                is_candidate.then_some(key.as_slice())
            })
    }

    /// Number of distinct keys with chains in this snapshot.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.chains.len()
    }

    /// True iff the snapshot holds no chains.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
    }

    /// Length of the chain for `key`, or 0 if absent.
    #[must_use]
    pub fn chain_len(&self, key: &[u8]) -> usize {
        self.chains.get(key).map_or(0, |c| c.len())
    }
}

/// Decide whether `entry` is visible to `view` at its snapshot timestamp.
///
/// The subtle case is a *foreign* `Pending` entry — a version a different
/// transaction staged but whose state flag this reader still observes as
/// `Pending` because the committer has not yet flipped the in-memory chain
/// entry to `Committed`. A reader cannot wait for that flip, so it needs a
/// way to decide, from data it can read lock-free, whether the version has
/// in fact committed.
///
/// The sequencer frontier provides exactly that proof. Commits are
/// published in dense `publish_seq` order: a commit's `(publish_seq,
/// commit_ts)` pair is allocated atomically, and the publish sequencer
/// advances `published_frontier` to a commit's `commit_ts` only after every
/// commit with an earlier `publish_seq` has reached a terminal state and
/// published. Because `publish_seq` order and `commit_ts` order agree, once
/// the frontier has advanced to some timestamp `F`, every transaction whose
/// `commit_ts <= F` has already been published — i.e. has committed (or
/// aborted, in which case its chain entry is `Aborted`, not the value we are
/// looking at). Therefore `entry.start_ts <= frontier` lets a reader treat a
/// still-`Pending`-flagged foreign version as committed: the frontier
/// passing its commit timestamp is the durable signal that the commit
/// happened, even though the per-chain state flag lags behind. The
/// additional `start_ts <= read_ts < stop_ts` window keeps the usual MVCC
/// visibility bound so the reader only sees versions live at its own
/// `read_ts`.
fn version_visible_to(entry: &VersionEntry, view: &ReadView) -> bool {
    let read_ts = view.visible_ts();
    match entry.state {
        VersionState::Pending { txn_id } => {
            if txn_id == view.txn_id {
                true
            } else {
                entry.start_ts <= read_ts
                    && read_ts < entry.stop_ts
                    && entry.start_ts <= view.sequencer_frontier()
            }
        }
        VersionState::Committed => entry.start_ts <= read_ts && read_ts < entry.stop_ts,
        VersionState::Aborted => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
#[path = "tests/read_view_pending_visibility.rs"]
mod read_view_pending_visibility;

#[cfg(test)]
#[cfg(not(loom))]
#[path = "tests/chain_snapshot_range.rs"]
mod chain_snapshot_range;
