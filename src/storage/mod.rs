//! Storage engine — B+ tree, page manager, buffer pool.
//!
//! This is a private internal module. The public API is exposed through
//! [`Collection`](crate::Collection) and [`Database`](crate::Database).

pub(crate) mod allocator;
pub(crate) mod btree;
pub(crate) mod btree_store;
pub(crate) mod buffer_pool;
pub(crate) mod catalog;
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod crash_cut_test_probe;
pub(crate) mod engine;
pub(crate) mod file_io;
pub(crate) mod handle;
pub(crate) mod header;
pub(crate) mod history_store;
pub(crate) mod lock;
pub(crate) mod oid;
pub(crate) mod page;
pub(crate) mod paged_engine;
pub(crate) mod reconcile;
pub(crate) mod root_snapshot;
pub(crate) mod secondary_index;
pub(crate) mod structural_page_batch;
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod structural_page_batch_test_probe;
#[cfg(test)]
pub(crate) mod test_support;
