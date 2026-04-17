//! MVCC (multi-version concurrency control) subsystem.
//!
//! This module hosts the WiredTiger-style in-memory version chain,
//! Hybrid Logical Clock timestamp oracle, read-view registry, and
//! reconciliation / deferred-free plumbing introduced in the T2–T9
//! rollout. Components are built up task-by-task and wired into the
//! storage engine only once all dependencies are in place.
//!
//! See `.omc/plans/mvcc-wiredtiger.md` for the full design.

pub mod timestamp;

#[allow(unused_imports)]
pub use timestamp::{HlcState, TimestampOracle, Ts};
