//! US-008 test-only structural page-byte accounting.
//!
//! Kept outside `structural_page_batch.rs` so intrusive test instrumentation
//! stays separate from the production structural page-batch implementation it
//! observes.

#![cfg(any(test, feature = "test-hooks"))]

use std::sync::atomic::{AtomicU64, Ordering};

static COMMITTED_STRUCTURAL_LEAF_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn reset_committed_structural_leaf_bytes() {
    COMMITTED_STRUCTURAL_LEAF_BYTES.store(0, Ordering::Release);
}

pub(crate) fn committed_structural_leaf_bytes() -> u64 {
    COMMITTED_STRUCTURAL_LEAF_BYTES.load(Ordering::Acquire)
}

pub(crate) fn record_committed_structural_leaf_bytes(bytes: usize) {
    COMMITTED_STRUCTURAL_LEAF_BYTES.fetch_add(bytes as u64, Ordering::AcqRel);
}
