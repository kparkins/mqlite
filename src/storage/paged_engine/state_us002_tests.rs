//! Phase 4 US-002 tests for dirty-leaf marking.

use std::sync::Arc;

use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::plan::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::test_support::{ArcIo, MockIo};

use super::MetadataState;

fn new_shared_state() -> Arc<super::SharedState> {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let handle = Arc::new(BufferPoolHandle::new(
        pool,
        history_pool,
        FileHeader::new_now(),
    ));

    let (_metadata, shared) = MetadataState::new(handle, 0, 0, 3).expect("create metadata state");
    shared
}

#[test]
fn mark_leaf_dirty_records_leaf_state_by_tree_identity() {
    let shared = new_shared_state();
    let primary = TreeIdent {
        collection_id: 7,
        kind: TreeKind::Primary,
    };
    let secondary = TreeIdent {
        collection_id: 7,
        kind: TreeKind::Secondary { index_id: 9 },
    };

    shared.mark_leaf_dirty(primary.clone(), 42, DirtyReason::PrimaryWrite);
    shared.mark_leaf_dirty(secondary.clone(), 99, DirtyReason::SecondaryWrite);

    let primary_dirty = shared
        .dirty_leaves
        .get(&primary)
        .expect("primary dirty tree");
    assert_eq!(
        primary_dirty
            .get(&42)
            .expect("primary dirty leaf")
            .dirty_reason,
        DirtyReason::PrimaryWrite
    );

    let secondary_dirty = shared
        .dirty_leaves
        .get(&secondary)
        .expect("secondary dirty tree");
    assert_eq!(
        secondary_dirty
            .get(&99)
            .expect("secondary dirty leaf")
            .dirty_reason,
        DirtyReason::SecondaryWrite
    );
}
