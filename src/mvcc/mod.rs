//! MVCC (multi-version concurrency control) subsystem.
//!
//! This module hosts the WiredTiger-style in-memory version chain,
//! Hybrid Logical Clock timestamp oracle, read-view registry, and
//! reconciliation / deferred-free plumbing introduced in the T2–T9
//! rollout. Components are built up task-by-task and wired into the
//! storage engine only once all dependencies are in place.
//!
//! See `.omc/plans/mvcc-wiredtiger.md` for the full design.

pub mod deferred_free;
pub mod metrics;
pub mod read_view;
pub mod timestamp;
pub mod transaction;
pub mod version;

#[allow(unused_imports)]
pub use deferred_free::DeferredFreeQueue;
#[allow(unused_imports)]
pub use metrics::{
    record_secondary_index_tombstone_hit, reset_secondary_index_tombstone_hits,
    secondary_index_tombstone_hits_snapshot,
};
#[allow(unused_imports)]
pub use read_view::{ChainSnapshot, ReadView, ReadViewRegistry};
#[allow(unused_imports)]
pub use timestamp::{HlcState, TimestampOracle, Ts};
#[allow(unused_imports)]
pub(crate) use transaction::{PrimaryOp, PrimaryWrite, SecIndexOp, SecIndexWrite, WriteTxn};
#[allow(unused_imports)]
pub use version::{OverflowRef, VersionData, VersionEntry};
