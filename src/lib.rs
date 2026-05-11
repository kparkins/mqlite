//! # mqlite — Embedded MongoDB-compatible document store
//!
//! mqlite is a lightweight, embedded document database with MongoDB query semantics.
//! It is designed for:
//!
//! - **Embedded apps** — local storage without a server
//! - **Test doubles** — replace MongoDB containers with an in-memory database
//! - **mongosh interop** — inspect mqlite files with familiar MongoDB tooling (via `wire` feature)
//! - **Edge/IoT** — constrained environments, single-file databases, crash recovery
//!
//! # Quick Start
//!
//! ```toml
//! [dependencies]
//! mqlite = "0.1"
//! serde = { version = "1", features = ["derive"] }
//! ```
//!
//! ```no_run
//! use mqlite::{Client, doc};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Config { key: String, value: String }
//!
//! fn main() -> mqlite::Result<()> {
//!     let client = Client::open("myapp.mqlite")?;
//!     let db = client.database("myapp");
//!     let configs = db.collection::<Config>("config");
//!
//!     configs.insert_one(&Config {
//!         key: "theme".into(),
//!         value: "dark".into(),
//!     })?;
//!
//!     let theme = configs.find_one(doc! { "key": "theme" })?;
//!     println!("Theme: {:?}", theme.map(|c| c.value));
//!
//!     Ok(())
//! }
//! ```
//!
//! # Feature Flags
//!
//! | Flag | Description |
//! |------|-------------|
//! | `wire` | MongoDB wire protocol shim (requires tokio) |
//! | `tracing` | Observability via the `tracing` crate |
//!
//! # Async
//!
//! The base crate is **sync-only**. Enabling the `wire` feature adds an async
//! runtime dependency (tokio) for the TCP listener, but the core CRUD API remains
//! synchronous. This keeps the dependency footprint minimal for embedded and IoT use cases.
//!
//! # Thread Safety
//!
//! | Type | `Send` | `Sync` | Notes |
//! |------|--------|--------|-------|
//! | [`Client`] | ✅ | ✅ | Clone and share across threads freely |
//! | [`Database`] | ✅ | ✅ | Lightweight handle, same inner state as `Client` |
//! | [`Collection<T>`] | ✅ | ✅ | Same shared state as `Client`/`Database` |
//! | [`Cursor<T>`] | ✅ | ❌ | Move to another thread; use `Mutex` for concurrent access |
//! | [`Error`] | ✅ | ✅ | — |
//!
//! `Client`, `Database`, and `Collection<T>` can be cloned and sent to other threads without
//! any additional synchronization. The storage engine coordinates concurrent
//! writes through MVCC publication, page latches, and DDL admission fences.
//!
//! `Cursor<T>` is `Send` but not `Sync` — matching the MongoDB Rust driver contract.
//! Use `Mutex<Cursor<T>>` if you need to drive a cursor from multiple threads simultaneously.
//!
//! # File Lifecycle
//!
//! ```text
//! Client::open("myapp.mqlite")
//!   ├─ Creates myapp.mqlite            (main database file)
//!   └─ Creates myapp.mqlite-journal    (write-ahead journal; accumulates writes)
//!
//! Client::close(self)             (blocking flush + checkpoint)
//!   └─ myapp.mqlite-journal is checkpointed into myapp.mqlite and removed
//!      → "single file" state
//!
//! drop(client)                    (non-blocking)
//!   └─ myapp.mqlite-journal remains on disk
//!      → Replayed automatically on next Client::open
//! ```
//!
//! The `close()` method is the recommended shutdown path when you need a guaranteed-clean
//! single-file state (e.g., before copying the database as a backup).
//!
//! # Security Notes
//!
//! - **File permissions**: new `.mqlite` files are created with mode `0600` (Unix)
//! - **Symlink prevention**: [`Error::SymlinkRejected`] is returned if the path is a symlink
//! - **Wire protocol**: no authentication — bind to `127.0.0.1` only;
//!   see the [Wire Protocol Security Advisory](https://github.com/kyleparkinson/mqlite/blob/master/docs/WIRE-SECURITY.md)

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::missing_panics_doc,
        clippy::missing_errors_doc,
        clippy::module_inception,
        reason = "test modules use assertion-style panics and setup unwraps"
    )
)]

// ---------------------------------------------------------------------------
// Public modules
// ---------------------------------------------------------------------------

/// BSON re-exports for ergonomic use without a direct `bson` dependency.
pub mod bson;
/// Client entry point: `Client::open(path)` → `client.database(name)` → `db.collection::<T>(name)`.
///
/// The `Client`, `Database`, and `Collection<T>` handles all live in this module —
/// they share the same `Arc<ClientInner>` and form a single ownership hierarchy.
pub mod client;
/// Lazy cursor for iterating query results.
pub mod cursor;
/// Error types and MongoDB-compatible error codes.
pub mod error;
/// Index definition and metadata types.
pub mod index;
/// BSON key encoding for B+ tree index storage.
pub mod keys;
/// Configuration options for database opening and query operations.
pub mod options;
/// Operation result types returned by write operations.
pub mod results;

// Internal modules (not public API)
// `mvcc` is `pub` but `#[doc(hidden)]` — integration tests need to
// reference `ReadView` / `ReadViewRegistry` / `Ts` through the crate root,
// but the module is not part of the stable surface.
#[allow(dead_code)]
mod journal;
#[doc(hidden)]
#[allow(dead_code)]
pub mod mvcc;
mod query;
mod storage;
mod update;
mod validation;

// Wire protocol shim (feature-gated)
#[cfg(feature = "wire")]
pub mod wire;

// ---------------------------------------------------------------------------
// Public re-exports — `use mqlite::*` or `use mqlite::Database;` etc.
// ---------------------------------------------------------------------------

// Core entry points
pub use client::{Client, Collection, Database};
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use client::{JournalCatalogCommitKind, JournalLogRecordKind, JournalLogRecordSummary};
pub use cursor::Cursor;
pub use query::explain::ExplainResult;

// Error and Result
pub use error::{Error, Result};

// Configuration
pub use options::{DurabilityMode, IndexOptions, OpenOptions, ReturnDocument};
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::write_crash_cut_contract::{WriteEnvelopeProbeCut, WriteEnvelopeProbeReport};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::header::{read_durable_header_counters, DurableHeaderCounters};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::paged_engine::hidden_accessors::{
    arm_checkpoint_boundary_failpoint, arm_legacy_commit_failpoint, BeforeLogReservationHookGuard,
    CheckpointBoundaryFailpoint, CheckpointBoundaryFailpointGuard, CreateIndexBuildHookGuard,
    LegacyCommitFailpoint, LegacyCommitFailpointGuard, Us026PostRegisterFailpoint,
    WriteBodyEntryEvent, WriteBodyEntryHookGuard,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::buffer_pool::page_latch_upgrade_race::page_latch_upgrade_race_counts as __us019_page_latch_upgrade_race_counts;

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use journal::logical_replay_fixtures::{
    append_logical_replay_frames as __us018_append_logical_replay_frames, Us018LogicalReplayFrame,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use journal::append_sync_observations::Us039AppendSyncObservations;

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::paged_engine::group_commit_observations::{
    Us017GroupCommitObservations, Us017GroupCommitPauseGuard,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::paged_engine::smo_classification_observations::{
    drain_events as __us010_drain_events,
    force_revalidation_failures as __us010_force_revalidation_failures,
    push_classification_override_names as __us010_push_classification_override_names,
    reset as __us010_reset_probe, Us010ProbeEvent,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::btree::reader_crabbing_observations::{
    drain_events as __us025_drain_events, reset as __us025_reset_probe, Us025CrabbingEvent,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::btree::range_scan_latch_scope::{
    drain_latch_samples as __us016_drain_latch_samples,
    install_range_scan_iteration_pause as __us016_install_range_scan_iteration_pause,
    reset as __us016_reset_probe, Us016RangeScanPauseGuard, Us016ReadLatchSample,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::buffer_pool::page_latch_fairness_harness::{
    us020_upgrade_loser_backoff_progress, us020_writer_preference_bounds_reader_starvation,
    Us020UpgradeRaceProgress,
};

#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use storage::paged_engine::publish_registry_harness::{
    Us020PublishSequencer, Us020PublishSlot, Us020WriterRegistry, Us020WriterTicket,
};

// Collection action types (returned by Collection methods; users chain options onto them)
pub use client::{Find, FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate, InsertMany, Update};

// Index
pub use index::{IndexInfo, IndexModel};

// Operation results
pub use results::{BulkWriteError, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

// BSON re-exports — users don't need a direct `bson` dependency for basic usage
pub use bson::{doc, Bson, DateTime, Document, ObjectId};

// Wire protocol entry point (feature-gated)
#[cfg(feature = "wire")]
pub use wire::WireProtocol;

// ---------------------------------------------------------------------------
// PR1 perf-counter readers (feature = "perf-counters" only)
//
// Exposed publicly so `benches/perf/perf_matrix.rs` can print AC values at the
// end of a workload run. Production binaries that don't enable
// `perf-counters` never compile this module and pay zero overhead.
// ---------------------------------------------------------------------------
/// PR1 perf-counter readers exposed for the consolidated perf harness.
///
/// Only compiled when the `perf-counters` cargo feature is enabled.
/// Production binaries pay zero overhead and these symbols do not
/// exist.
#[cfg(feature = "perf-counters")]
pub mod perf_counters {
    pub use crate::storage::buffer_pool::{
        flip_retry_exhausted_count, flip_retry_rate, install_phase_b_mean_hold_ns,
        live_delta_check_mean_hold_ns, reset_flip_counters, reset_shared_latch_wait_hist,
        shared_latch_wait_p50_ns, shared_latch_wait_p99_ns,
    };
}

// ---------------------------------------------------------------------------
// Fuzz helpers (feature = "fuzz" only — never enable in production)
// ---------------------------------------------------------------------------

/// Evaluate a MongoDB filter document against a BSON document.
///
/// This is a thin shim over the internal `query::eval_filter` function,
/// exposed **only** under the `fuzz` feature so that fuzz targets in the
/// `fuzz/` crate can reach it without making it part of the stable API.
///
/// Do **not** call this from application code.
#[cfg(feature = "fuzz")]
pub fn fuzz_eval_filter(doc: &bson::Document, filter: &bson::Document) -> Result<bool> {
    query::eval_filter(doc, filter)
}

/// Phase 2 §9.2 / US-023 fuzz entry — `LogicalTxnFrame::decode` in
/// `Scanning` context. Returns the decoded frame on success, `None` on
/// any clean rejection. Never panics on arbitrary input.
///
/// Exposed **only** under the `fuzz` feature. Do not call from
/// application code.
#[cfg(feature = "fuzz")]
pub fn fuzz_logical_txn_decode_scanning(buf: &[u8], salt1: u32, salt2: u32) -> Result<()> {
    use crate::journal::log_file::{DecodeCtx, LogicalTxnFrame};
    let _ = LogicalTxnFrame::decode(buf, salt1, salt2, DecodeCtx::Scanning)?;
    Ok(())
}

/// Phase 2 §9.2 / US-023 fuzz entry — `try_skip_logical_txn` cursor
/// post-condition probe. Validates that the helper either advances by
/// the returned count OR fully rewinds on rejection.
///
/// Exposed **only** under the `fuzz` feature. Do not call from
/// application code.
#[cfg(feature = "fuzz")]
pub fn fuzz_try_skip_logical_txn(buf: &[u8], salt1: u32, salt2: u32) -> Result<()> {
    use crate::journal::log_file::try_skip_logical_txn;
    use std::io::{Cursor, Seek};
    let mut cursor = Cursor::new(buf);
    let start = cursor.stream_position().map_err(crate::error::Error::Io)?;
    match try_skip_logical_txn(&mut cursor, salt1, salt2)? {
        Some((n, _frame)) => {
            // Helper advanced — cursor must be at start + n.
            let pos = cursor.stream_position().map_err(crate::error::Error::Io)?;
            assert_eq!(pos, start + n, "advance contract");
        }
        None => {
            // Helper rejected — cursor must be back at start.
            let pos = cursor.stream_position().map_err(crate::error::Error::Io)?;
            assert_eq!(pos, start, "rewind contract");
        }
    }
    Ok(())
}

/// Phase 2 §9.2 / US-023 fuzz entry — recovery over an arbitrary
/// journal-file body. Creates a fresh DB via `Client::open_with_options`,
/// closes it, overwrites the `-journal` sidecar with the fuzzed bytes,
/// then re-opens. Recovery either succeeds (replay) or returns a
/// recoverable error — neither path may panic / loop / UB.
///
/// AC#6 also requires that `read_page_linear` not panic for any page
/// number after recovery. The post-open page probe below drives the
/// read path through pages 1..=8 via the public-API CRUD surface, which
/// internally calls `JournalManager::read_page_linear`. A panic in the
/// scan dispatch (e.g., a logical-frame skip helper that doesn't rewind
/// on a fuzzed boundary) would surface here.
///
/// Exposed **only** under the `fuzz` feature. Do not call from
/// application code.
#[cfg(feature = "fuzz")]
pub fn fuzz_logical_txn_recover(body: &[u8]) -> std::result::Result<(), ()> {
    let dir = tempfile::tempdir().map_err(|_| ())?;
    let db_path = dir.path().join("fuzz.mqlite");

    // Create a fresh DB so the journal salts are known.
    {
        let _client = match Client::open(&db_path) {
            Ok(c) => c,
            Err(_) => return Ok(()),
        };
        // Drop the client cleanly so the main file has a stable header.
    }

    // Overwrite the journal sidecar with fuzzed bytes.
    let journal_path = {
        let mut p = db_path.as_os_str().to_owned();
        p.push("-journal");
        std::path::PathBuf::from(p)
    };
    let _ = std::fs::write(&journal_path, body);

    // Reopen — exercises the full recovery scan over the fuzzed body.
    // Any panic / infinite loop / UB will surface here.
    let client = match Client::open(&db_path) {
        Ok(c) => c,
        Err(_) => return Ok(()),
    };

    // §9.2 / US-023 AC#6: drive `read_page_linear` post-recovery via
    // the public-API path. Each `find` traverses the catalog and
    // collection roots, which dispatches through the journal read
    // surface for any page that lives in the journal-but-not-main-file
    // window. Pages 1-8 cover the header, catalog root, and a couple
    // of likely-allocated B-tree pages — enough surface to surface
    // any frame-kind skip helper panic seeded from the fuzzed body.
    let db = client.database("fuzz_db");
    for col in ["c0", "c1"] {
        if let Ok(cursor) = db
            .collection::<bson::Document>(col)
            .find(bson::doc! {})
            .run()
        {
            let _ = cursor.take(4).count();
        }
    }
    drop(client);
    Ok(())
}
