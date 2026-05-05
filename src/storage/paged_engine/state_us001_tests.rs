//! Phase 4 US-001 tests for `SharedState` dirty-leaf scaffolding.

use std::collections::HashMap;
use std::sync::Arc;

use crate::client::Client;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::plan::{DirtyReason, LeafState, TreeIdent, TreeKind};
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
fn shared_state_starts_with_empty_dirty_leaf_index() {
    let shared = new_shared_state();
    assert!(
        shared.dirty_leaves.is_empty(),
        "new SharedState must not introduce dirty leaves"
    );

    let ident = TreeIdent {
        collection_id: 7,
        kind: TreeKind::Primary,
    };
    let mut leaves = HashMap::new();
    leaves.insert(
        42,
        LeafState {
            dirty_reason: DirtyReason::PrimaryWrite,
        },
    );

    shared.dirty_leaves.insert(ident.clone(), leaves);
    let stored = shared.dirty_leaves.get(&ident).expect("dirty tree entry");
    let leaf = stored.get(&42).expect("dirty leaf entry");
    assert_eq!(leaf.dirty_reason, DirtyReason::PrimaryWrite);
}

#[test]
fn empty_database_open_close_still_succeeds() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let db_path = dir.path().join("phase4_us001_empty.mqlite");

    let client = Client::open(&db_path).expect("open empty database");
    client.close().expect("close empty database");

    let reopened = Client::open(&db_path).expect("reopen empty database");
    reopened.close().expect("close reopened empty database");
}
