//! Reconcile planning types shared by the Phase 4 dirty-leaf index.

/// Stable identity for a tree whose leaves may need reconciliation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TreeIdent {
    /// Durable collection identifier that owns the tree.
    pub(crate) collection_id: i64,
    /// Primary or secondary tree discriminator.
    pub(crate) kind: TreeKind,
}

/// Kind of tree represented by a [`TreeIdent`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TreeKind {
    /// Primary collection data tree.
    Primary,
    /// Secondary index tree.
    Secondary {
        /// Durable index identifier for the secondary tree.
        index_id: i64,
    },
}

/// Dirty-leaf metadata retained until a checkpoint reconcile pass consumes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeafState {
    /// Reason the leaf was marked dirty.
    pub(crate) dirty_reason: DirtyReason,
}

/// Source operation that made a leaf eligible for reconcile planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirtyReason {
    /// Primary data tree write.
    PrimaryWrite,
    /// Secondary index tree write.
    SecondaryWrite,
}
