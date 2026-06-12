//! MVCC (multi-version concurrency control) subsystem.
//!
//! This module hosts the WiredTiger-style in-memory version chain,
//! Hybrid Logical Clock timestamp oracle, read-view registry, and
//! reconciliation / deferred-free plumbing.

pub mod chain_snapshot;
pub mod deferred_free;
pub mod metrics;
pub mod read_view;
pub mod registry;
pub mod timestamp;
pub mod transaction;
pub mod version;

// Public-API surface (R13 item 2): `mvcc` is `#[doc(hidden)] pub` so the
// crate's own integration tests can reach a small set of snapshot primitives
// through the crate root. The 138-line `pub use metrics::{…}` re-export wall
// was removed; every internal caller now uses the canonical
// `crate::mvcc::{metrics,read_view,timestamp,transaction,version}::X` path, and
// integration-test consumers of metrics counters use `mqlite::mvcc::metrics::X`.
//
// The items below are kept ONLY because `tests/*.rs` reference them through the
// crate root; each line is justified rather than blanket-re-exported.

// `tests/{panic_rollback,reconcile_race,registry_stress,…}` open and snapshot
// readers via `mqlite::mvcc::{ReadView, ReadViewRegistry, ChainSnapshot}`;
// `tests/mwmr_timestamp_frontier` drives `TestFrontierHandle`. The three types
// live in sibling modules after the R13 split; surfaced here for the crate-root
// integration-test paths.
pub use chain_snapshot::ChainSnapshot;
pub use read_view::{ReadView, TestFrontierHandle};
pub use registry::ReadViewRegistry;
// `tests/*` stamp `read_ts` values with `mqlite::mvcc::Ts`.
pub use timestamp::Ts;
// `tests/{pending_write_visibility,read_your_own_write,secondary_index_*,…}`
// build version chains with `mqlite::mvcc::{VersionData, VersionEntry, VersionState}`.
pub use version::{VersionData, VersionEntry, VersionState};
