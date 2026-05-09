//! Test-only constructors and accessors for [`BufferPoolHandle`].

use std::sync::Arc;

use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::BufferPool;
use crate::storage::handle::{BufferPoolHandle, BufferPoolPageSource};
use crate::storage::header::FileHeader;

impl BufferPoolHandle {
    /// Create a handle without a journal.
    pub(crate) fn new(
        pool: Arc<BufferPool>,
        history_pool: Arc<BufferPool>,
        header: FileHeader,
    ) -> Self {
        let allocator = AllocatorHandle::new(header);
        let pool_io = BufferPoolPageSource::new(Arc::clone(&pool));
        Self {
            pool,
            history_pool,
            allocator,
            pool_io,
            read_view_registry: crate::mvcc::ReadViewRegistry::new(),
            journal: None,
            journal_main_file: None,
        }
    }

    /// Borrow the page-source adapter routing allocator I/O through the pool.
    pub(crate) fn page_source(&self) -> &BufferPoolPageSource {
        &self.pool_io
    }
}
