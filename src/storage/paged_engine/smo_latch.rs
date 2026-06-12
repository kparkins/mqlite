//! Phase 5 US-010 structural-modification classification and latch planning.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::{Error, Result, WriteConflictReason};
use crate::mvcc::transaction::{PrimaryOp, SecIndexOp};
use crate::storage::btree::{
    leaf_can_insert_value, leaf_needs_rebalance_after_delete, page_size_for_level, BTree,
    BTreePathStep, OVERFLOW_THRESHOLD,
};
use crate::storage::buffer_pool::{LatchedPinnedPage, PageSize};

use super::state::SharedState;

/// Shape of a staged write for Phase 5 SMO gating (§10.24).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WriteShape {
    /// Fits on the current leaf; no parent rewrite or root movement.
    RootNeutral,
    /// Leaf would split.
    LeafSplit,
    /// Delete would underflow and merge or redistribute.
    LeafMerge,
    /// Value may allocate or free overflow pages.
    OverflowChange,
    /// Unique-prefix scan spans sibling leaves.
    #[allow(
        dead_code,
        reason = "US-011 unique-index install constructs this shape"
    )]
    UniqueSpansSibling { leaves: Vec<u32> },
}

impl WriteShape {
    fn is_root_neutral(&self) -> bool {
        matches!(self, Self::RootNeutral)
    }
}

/// Staged operation data needed for write-shape classification.
#[derive(Clone, Debug)]
pub(crate) enum SmoWriteOp {
    /// Insert a value of `value_len` bytes.
    Insert { key: Vec<u8>, value_len: usize },
    /// Update a value of `value_len` bytes.
    Update { key: Vec<u8>, value_len: usize },
    /// Delete an existing key.
    Delete { key: Vec<u8> },
}

impl SmoWriteOp {
    pub(crate) fn from_primary(key: &[u8], op: &PrimaryOp) -> Self {
        match op {
            PrimaryOp::Insert { data } => Self::Insert {
                key: key.to_vec(),
                value_len: data.len(),
            },
            PrimaryOp::Update { data } => Self::Update {
                key: key.to_vec(),
                value_len: data.len(),
            },
            PrimaryOp::Delete => Self::Delete { key: key.to_vec() },
        }
    }

    pub(crate) fn from_secondary(key: &[u8], op: &SecIndexOp) -> Self {
        match op {
            SecIndexOp::Insert { id_bytes } => Self::Insert {
                key: key.to_vec(),
                value_len: id_bytes.len(),
            },
            SecIndexOp::Delete => Self::Delete { key: key.to_vec() },
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Insert { key, .. } | Self::Update { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// One B-tree write target participating in SMO latch planning.
#[derive(Clone, Debug)]
pub(crate) struct SmoWriteTarget {
    pub(crate) root_page: u32,
    pub(crate) root_level: u8,
    pub(crate) key: Vec<u8>,
    pub(crate) op: SmoWriteOp,
    pub(crate) unique_prefix_range: Option<(Vec<u8>, Vec<u8>)>,
}

/// Held latches plus target leaf mapping in the same order as input targets.
pub(crate) struct SmoLatchSet<'pool> {
    pages: Vec<LatchedPinnedPage<'pool>>,
    target_leaves: Vec<u32>,
}

impl<'pool> SmoLatchSet<'pool> {
    pub(crate) fn empty() -> Self {
        Self {
            pages: Vec::new(),
            target_leaves: Vec::new(),
        }
    }

    pub(crate) fn target_leaf(&self, idx: usize) -> Option<u32> {
        self.target_leaves.get(idx).copied()
    }

    pub(crate) fn page_mut(&mut self, page_id: u32) -> Option<&mut LatchedPinnedPage<'pool>> {
        self.pages.iter_mut().find(|page| page.page_id() == page_id)
    }
}

#[derive(Clone)]
struct PlannedWrite {
    target: SmoWriteTarget,
    path: Vec<BTreePathStep>,
    shape: WriteShape,
}

/// Classify a staged write while holding a short shared page latch.
#[allow(dead_code, reason = "US-010 contract names this helper explicitly")]
pub(crate) fn classify_write(
    shared: &SharedState,
    leaf: u32,
    op: &SmoWriteOp,
) -> Result<WriteShape> {
    let page = shared.handle.pool().pin_for_read(leaf)?;
    let data = page.data_snapshot();
    let target = SmoWriteTarget {
        root_page: leaf,
        root_level: 0,
        key: op.key().to_vec(),
        op: op.clone(),
        unique_prefix_range: None,
    };
    let shape = classify_leaf_bytes(data.as_slice(), false, &target)?;
    #[cfg(any(test, feature = "test-hooks"))]
    super::smo_classification_observations::record_classification("shared", &shape);
    #[cfg(any(test, feature = "test-hooks"))]
    if let Some(shape) = super::smo_classification_observations::override_classification("shared") {
        return Ok(shape);
    }
    Ok(shape)
}

/// Result of one [`try_acquire_latches_once`] attempt.
///
/// Names the two non-error outcomes so the retry driver reads as a small
/// state machine. A plain `enum` (the `Settled` payload is the latch set
/// the caller already needed to build) keeps the attempt allocation-free
/// beyond the latch set itself.
enum LatchAttempt<'pool> {
    /// Shared-latch classification held under exclusive latches: the
    /// acquired latch set is final.
    Settled(SmoLatchSet<'pool>),
    /// Exclusive reclassification widened the required page set; the held
    /// latches were dropped and the caller must re-plan under fresh
    /// classification.
    Reclassify,
}

/// Acquire the page-local latches required by `targets`, retrying bounded
/// reclassification when shared-latch classification is stale.
///
/// Invariant: each iteration re-plans from scratch (`plan_targets` under a
/// fresh shared latch), so a retry never reuses a stale path or page set.
/// Forward progress is guaranteed by `smo_classification_retry_cap`:
/// reclassification can only widen the page set finitely often before the
/// cap converts the livelock into a `StructuralContention` write conflict
/// that the writer can resolve by retrying its commit.
pub(crate) fn acquire_smo_latches<'pool>(
    shared: &'pool SharedState,
    targets: &[SmoWriteTarget],
) -> Result<SmoLatchSet<'pool>> {
    if targets.is_empty() {
        return Ok(SmoLatchSet::empty());
    }

    let mut reclassifications = 0u32;
    loop {
        match try_acquire_latches_once(shared, targets)? {
            LatchAttempt::Settled(latches) => return Ok(latches),
            LatchAttempt::Reclassify => {
                reclassifications = reclassifications.saturating_add(1);
                #[cfg(any(test, feature = "test-hooks"))]
                super::smo_classification_observations::record_reclassification(reclassifications);
                if reclassifications >= shared.smo_classification_retry_cap {
                    return structural_contention();
                }
                // Re-plan under a fresh shared latch on the next iteration.
            }
        }
    }
}

/// One plan → latch → revalidate → reclassify attempt.
///
/// On `Reclassify` the held latches are already dropped. A stale path
/// (`paths_still_valid` false) is a hard `StructuralContention` rather than
/// a retry: the tree shape changed under us, so re-planning would race the
/// same way.
fn try_acquire_latches_once<'pool>(
    shared: &'pool SharedState,
    targets: &[SmoWriteTarget],
) -> Result<LatchAttempt<'pool>> {
    let planned = plan_targets(shared, targets)?;
    let required_pages = required_pages(&planned);
    let pages = acquire_pages(shared, &required_pages)?;

    if !paths_still_valid(shared, &planned)? {
        return structural_contention();
    }
    #[cfg(any(test, feature = "test-hooks"))]
    if super::smo_classification_observations::force_revalidation_failure_once() {
        return structural_contention();
    }

    let exclusive_shapes = reclassify_exclusive(&pages, &planned)?;
    let exclusive_pages = required_pages_for_shapes(&planned, &exclusive_shapes);
    if exclusive_pages != required_pages {
        // Exclusive-latch classification disagrees with the shared-latch
        // plan (e.g. a concurrent write turned a root-neutral leaf into a
        // split). Drop everything and let the caller re-plan.
        drop(pages);
        return Ok(LatchAttempt::Reclassify);
    }

    Ok(LatchAttempt::Settled(SmoLatchSet {
        pages,
        target_leaves: planned
            .iter()
            .filter_map(|plan| plan.path.last().map(|step| step.page_id))
            .collect(),
    }))
}

fn plan_targets(shared: &SharedState, targets: &[SmoWriteTarget]) -> Result<Vec<PlannedWrite>> {
    let mut planned = Vec::with_capacity(targets.len());
    for target in targets {
        let path = {
            let tree = BTree::open(
                shared.new_btree_store(),
                target.root_page,
                target.root_level,
            );
            tree.path_to_leaf(&target.key)?
        };
        let leaf = path
            .last()
            .ok_or_else(|| Error::Internal("empty B-tree path".into()))?;
        let leaf_page = leaf.page_id;
        let is_root_leaf = target.root_level == 0;
        let shape = classify_write_from_leaf(shared, leaf_page, is_root_leaf, target)?;
        planned.push(PlannedWrite {
            target: target.clone(),
            path,
            shape,
        });
    }
    Ok(planned)
}

fn classify_write_from_leaf(
    shared: &SharedState,
    leaf: u32,
    is_root_leaf: bool,
    target: &SmoWriteTarget,
) -> Result<WriteShape> {
    let page = shared.handle.pool().pin_for_read(leaf)?;
    let snapshot = page.data_snapshot();
    let data = snapshot.as_slice();
    let shape = classify_leaf_bytes(data, is_root_leaf, target)?;
    #[cfg(any(test, feature = "test-hooks"))]
    {
        super::smo_classification_observations::record_classification("shared", &shape);
        if let Some(override_shape) =
            super::smo_classification_observations::override_classification("shared")
        {
            return Ok(override_shape);
        }
    }
    Ok(shape)
}

fn classify_leaf_bytes(
    data: &[u8],
    is_root_leaf: bool,
    target: &SmoWriteTarget,
) -> Result<WriteShape> {
    if let Some((start, end)) = &target.unique_prefix_range {
        let leaves = crate::storage::btree::leaf_unique_prefix_sibling_pages(data, start, end)?;
        if !leaves.is_empty() {
            return Ok(WriteShape::UniqueSpansSibling { leaves });
        }
    }

    match &target.op {
        SmoWriteOp::Insert { value_len, .. } => {
            if *value_len > OVERFLOW_THRESHOLD {
                return Ok(WriteShape::OverflowChange);
            }
            if leaf_can_insert_value(data, target.op.key().len(), *value_len)? {
                Ok(WriteShape::RootNeutral)
            } else {
                Ok(WriteShape::LeafSplit)
            }
        }
        SmoWriteOp::Update { value_len, .. } => {
            if *value_len > OVERFLOW_THRESHOLD {
                Ok(WriteShape::OverflowChange)
            } else {
                Ok(WriteShape::RootNeutral)
            }
        }
        SmoWriteOp::Delete { .. } => {
            if !is_root_leaf && leaf_needs_rebalance_after_delete(data, target.op.key())? {
                Ok(WriteShape::LeafMerge)
            } else {
                Ok(WriteShape::RootNeutral)
            }
        }
    }
}

fn required_pages(planned: &[PlannedWrite]) -> BTreeMap<u32, PageSize> {
    required_pages_for_shapes(
        planned,
        &planned
            .iter()
            .map(|plan| plan.shape.clone())
            .collect::<Vec<_>>(),
    )
}

/// Compute the set of pages a write batch must hold under exclusive latch.
///
/// The rule splits on whether any planned write is structural (a split,
/// merge, or overflow change — anything that is not `RootNeutral`):
///
/// - A purely root-neutral write only rewrites its own leaf, so it latches
///   that single leaf.
/// - As soon as one write is structural, every batched write latches its
///   entire root-to-leaf path (the "spine"), not just the leaf.
///
/// Latching the whole spine is what makes a structural modification atomic
/// against concurrent readers and writers. A split or merge does not stay on
/// the leaf: it rewrites the parent's separator keys and child pointers, can
/// propagate upward, and may move the root. A concurrent descent that latched
/// only the leaf would read a parent whose pointers were mid-rewrite and could
/// follow a stale or dangling child link to the wrong subtree — a lost or
/// duplicated key. Holding every page from the root down freezes the exact
/// portion of the tree the modification can touch, so no other operation can
/// observe the spine in a half-rebalanced state. The pages are returned as an
/// ordered `BTreeSet` so callers acquire them in a single deterministic page
/// order, which is also what prevents two structural writers from deadlocking.
fn required_pages_for_shapes(
    planned: &[PlannedWrite],
    shapes: &[WriteShape],
) -> BTreeMap<u32, PageSize> {
    // Keyed by page id so the acquire order stays deterministic (ascending
    // page id) exactly as the prior `BTreeSet<u32>` ordering, while carrying
    // each page's true allocator size from its path-step level.
    let mut pages = BTreeMap::new();
    let any_structural = shapes.iter().any(|shape| !shape.is_root_neutral());
    for (plan, shape) in planned.iter().zip(shapes) {
        if any_structural {
            for step in &plan.path {
                pages.insert(step.page_id, page_size_for_level(step.level));
            }
        } else if let Some(leaf) = plan.path.last() {
            pages.insert(leaf.page_id, page_size_for_level(leaf.level));
        }
        if let WriteShape::UniqueSpansSibling { leaves } = shape {
            // Sibling leaves are level-0 leaves (32 KiB).
            for leaf in leaves {
                pages.insert(*leaf, PageSize::Large32k);
            }
        }
    }
    pages
}

fn acquire_pages<'pool>(
    shared: &'pool SharedState,
    required_pages: &BTreeMap<u32, PageSize>,
) -> Result<Vec<LatchedPinnedPage<'pool>>> {
    let mut pages = Vec::with_capacity(required_pages.len());
    for (page_id, size) in required_pages {
        #[cfg(any(test, feature = "test-hooks"))]
        super::smo_classification_observations::record_exclusive_acquire(*page_id);
        // Pin at the path-known size — NOT via `pin_for_write`'s residency
        // heuristic, which would load an evicted interior (4 KiB) page as a
        // 32 KiB frame in the wrong partition (duplicate frame, latch excludes
        // nothing). See `page_size_for_level`.
        pages.push(shared.handle.pool().pin_for_write_sized(*page_id, *size)?);
    }
    Ok(pages)
}

fn paths_still_valid(shared: &SharedState, planned: &[PlannedWrite]) -> Result<bool> {
    for plan in planned {
        let tree = BTree::open(
            shared.new_btree_store(),
            plan.target.root_page,
            plan.target.root_level,
        );
        if !tree.revalidate_path(&plan.target.key, &plan.path)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn reclassify_exclusive(
    pages: &[LatchedPinnedPage<'_>],
    planned: &[PlannedWrite],
) -> Result<Vec<WriteShape>> {
    let held: BTreeSet<u32> = pages.iter().map(LatchedPinnedPage::page_id).collect();
    let mut shapes = Vec::with_capacity(planned.len());
    for plan in planned {
        let leaf = plan
            .path
            .last()
            .ok_or_else(|| Error::Internal("empty B-tree path".into()))?
            .page_id;
        if !held.contains(&leaf) {
            return Err(Error::Internal(format!(
                "missing exclusive latch for leaf page {leaf}"
            )));
        }
        let page = pages
            .iter()
            .find(|page| page.page_id() == leaf)
            .ok_or_else(|| Error::Internal(format!("missing exclusive page {leaf}")))?;
        let snapshot = page.data_snapshot();
        let data = snapshot.as_slice();
        let is_root_leaf = plan.target.root_level == 0;
        let shape = classify_leaf_bytes(data, is_root_leaf, &plan.target)?;
        #[cfg(any(test, feature = "test-hooks"))]
        {
            super::smo_classification_observations::record_classification("exclusive", &shape);
            if let Some(override_shape) =
                super::smo_classification_observations::override_classification("exclusive")
            {
                shapes.push(override_shape);
                continue;
            }
        }
        shapes.push(shape);
    }
    Ok(shapes)
}

fn structural_contention<T>() -> Result<T> {
    Err(Error::WriteConflict {
        reason: WriteConflictReason::StructuralContention,
    })
}

#[cfg(test)]
mod bugsuspect_tests {
    //! Bug-suspect (rank ~3): the SMO latch planner must latch each required
    //! page at its TRUE allocator size, not via `BufferPool::detect_page_size`,
    //! which defaults a non-resident page to 32 KiB. Before the fix,
    //! `required_pages_for_shapes` collapsed the planned path to a bare
    //! `BTreeSet<u32>` and `acquire_pages` pinned every page with the residency
    //! heuristic — so an interior (4 KiB) page evicted between plan and acquire
    //! was loaded as a 32 KiB frame in the wrong partition (its exclusive latch
    //! excluding nothing). The hazard of the heuristic itself is pinned in
    //! `buffer_pool::tests::bugsuspect_detect_page_size_misclassification`.
    //!
    //! This test pins the fix: for a structural write over a 2-level tree, the
    //! planned page set carries the interior page at `Small4k` and the leaf at
    //! `Large32k`, so `acquire_pages` can pin each at its known size.

    use super::*;

    fn planned_two_level_split() -> PlannedWrite {
        // Path: root interior at level 1 (page 5, 4 KiB) -> leaf at level 0
        // (page 9, 32 KiB).
        let path = vec![
            BTreePathStep {
                page_id: 5,
                parent_page: None,
                child_slot: None,
                level: 1,
            },
            BTreePathStep {
                page_id: 9,
                parent_page: Some(5),
                child_slot: Some(0),
                level: 0,
            },
        ];
        PlannedWrite {
            target: SmoWriteTarget {
                root_page: 5,
                root_level: 1,
                key: b"k".to_vec(),
                op: SmoWriteOp::Insert {
                    key: b"k".to_vec(),
                    value_len: 8,
                },
                unique_prefix_range: None,
            },
            path,
            // A structural shape forces the full spine (interior + leaf) into
            // the required set.
            shape: WriteShape::LeafSplit,
        }
    }

    #[test]
    fn required_pages_for_shapes_carries_interior_4k_size() {
        let plan = planned_two_level_split();
        let required = required_pages_for_shapes(
            std::slice::from_ref(&plan),
            std::slice::from_ref(&plan.shape),
        );

        assert_eq!(
            required.get(&5),
            Some(&PageSize::Small4k),
            "interior path page (level 1) must be latched at its true 4 KiB \
             size so an evicted interior page is not loaded into the 32 KiB \
             partition"
        );
        assert_eq!(
            required.get(&9),
            Some(&PageSize::Large32k),
            "leaf page (level 0) must be latched as 32 KiB"
        );
    }

    #[test]
    fn root_neutral_leaf_only_keeps_leaf_32k_size() {
        let plan = PlannedWrite {
            shape: WriteShape::RootNeutral,
            ..planned_two_level_split()
        };
        let required = required_pages_for_shapes(
            std::slice::from_ref(&plan),
            std::slice::from_ref(&plan.shape),
        );
        // Root-neutral: only the leaf is latched, at 32 KiB.
        assert_eq!(required.get(&9), Some(&PageSize::Large32k));
        assert!(
            !required.contains_key(&5),
            "a root-neutral write must not latch the interior spine"
        );
    }
}
