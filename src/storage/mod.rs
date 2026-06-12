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
#[path = "tests/close_quadratic_probe.rs"]
pub(crate) mod close_quadratic_probe;
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
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/structural_batch_observations.rs"]
pub(crate) mod structural_batch_observations;
pub(crate) mod structural_page_batch;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/write_crash_cut_contract.rs"]
pub(crate) mod write_crash_cut_contract;
