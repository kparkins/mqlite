//! Phase 5 US-010 structural-modification classification and latch planning.

use std::collections::BTreeSet;

use crate::error::{Error, Result, WriteConflictReason};
use crate::mvcc::{PrimaryOp, SecIndexOp};
use crate::storage::btree::{
    leaf_can_insert_value, leaf_needs_rebalance_after_delete, BTree, BTreePathStep,
    OVERFLOW_THRESHOLD,
};
use crate::storage::buffer_pool::LatchedPinnedPage;

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

/// Acquire the page-local latches required by `targets`, retrying bounded
/// reclassification when shared-latch classification is stale.
pub(crate) fn acquire_smo_latches<'pool>(
    shared: &'pool SharedState,
    targets: &[SmoWriteTarget],
) -> Result<SmoLatchSet<'pool>> {
    if targets.is_empty() {
        return Ok(SmoLatchSet::empty());
    }

    let mut reclassifications = 0u32;
    loop {
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
        if exclusive_pages == required_pages {
            return Ok(SmoLatchSet {
                pages,
                target_leaves: planned
                    .iter()
                    .filter_map(|plan| plan.path.last().map(|step| step.page_id))
                    .collect(),
            });
        }

        drop(pages);
        reclassifications = reclassifications.saturating_add(1);
        #[cfg(any(test, feature = "test-hooks"))]
        super::smo_classification_observations::record_reclassification(reclassifications);
        if reclassifications >= shared.smo_classification_retry_cap {
            return structural_contention();
        }
    }
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

fn required_pages(planned: &[PlannedWrite]) -> BTreeSet<u32> {
    required_pages_for_shapes(
        planned,
        &planned
            .iter()
            .map(|plan| plan.shape.clone())
            .collect::<Vec<_>>(),
    )
}

fn required_pages_for_shapes(planned: &[PlannedWrite], shapes: &[WriteShape]) -> BTreeSet<u32> {
    let mut pages = BTreeSet::new();
    let any_structural = shapes.iter().any(|shape| !shape.is_root_neutral());
    for (plan, shape) in planned.iter().zip(shapes) {
        if any_structural {
            for step in &plan.path {
                pages.insert(step.page_id);
            }
        } else if let Some(leaf) = plan.path.last() {
            pages.insert(leaf.page_id);
        }
        if let WriteShape::UniqueSpansSibling { leaves } = shape {
            pages.extend(leaves.iter().copied());
        }
    }
    pages
}

fn acquire_pages<'pool>(
    shared: &'pool SharedState,
    required_pages: &BTreeSet<u32>,
) -> Result<Vec<LatchedPinnedPage<'pool>>> {
    let mut pages = Vec::with_capacity(required_pages.len());
    for page_id in required_pages {
        #[cfg(any(test, feature = "test-hooks"))]
        super::smo_classification_observations::record_exclusive_acquire(*page_id);
        pages.push(shared.handle.pool().pin_for_write(*page_id)?);
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
