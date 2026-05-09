//! Test-only [`AllocatorHandle`] accessors.

use crate::error::{Error, Result};
use crate::journal::BoundaryAppended;
use crate::mvcc::deferred_free::CheckpointLifetimeDrain;
use crate::storage::header::FileHeader;
use std::sync::atomic::Ordering;

use super::{AllocatorFreezeGuard, AllocatorHandle};

impl AllocatorHandle {
    /// Force-set the refcount for a page.
    pub(crate) fn set_overflow_refcount_for_test(&self, first_page: u32, value: u32) {
        let atomic = self.refcount_handle(first_page);
        atomic.store(value, Ordering::Release);
    }

    /// Commit a staged checkpoint header after the durable boundary exists.
    pub(crate) fn commit_staged_header_after_boundary(
        &self,
        mut freeze: AllocatorFreezeGuard,
        staged_header: FileHeader,
        boundary: BoundaryAppended,
        checkpoint_lifetime_drain: CheckpointLifetimeDrain,
    ) -> Result<()> {
        let boundary_page_count = boundary.db_page_count();
        let result = if staged_header.total_page_count != boundary_page_count {
            Err(Error::Internal(format!(
                "boundary page count {boundary_page_count} does not match staged header {}",
                staged_header.total_page_count
            )))
        } else {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
            state.header = staged_header;
            state.header_dirty = false;
            drop(checkpoint_lifetime_drain);
            Ok(())
        };
        freeze.release();
        result
    }
}
