//! Bug-suspect: a deterministically-failing Building index bricks reopen.
//!
//! Suspect (deep-refactor-2026-06-10, rank ~2, index_ddl.rs
//! `resume_building_indexes_after_open` ~:682, caller paged_engine.rs open()):
//! the resume loop `?`-propagates a build error straight out of `open()` with
//! NO `create_index_cleanup` fallback — unlike the LIVE `create_index` driver
//! (index_ddl.rs:71-87), which catches a non-conflict build error and drops
//! the orphan Building entry.
//!
//! Why it matters: the most likely reason a `Building` entry survives to
//! reopen is that the original build failed *deterministically* (a UNIQUE
//! violation over duplicate data). The rebuild during resume re-runs
//! `build_index_mvcc` -> `check_unique_constraint_*` -> `Err(DuplicateKey)`,
//! so EVERY reopen re-hits the same error and the database cannot open again.
//!
//! Repro shape (unit-level, mirrors index_build_recovery.rs): insert
//! duplicate `tag` values, then `create_index_reserve` a UNIQUE index on
//! `tag` (durably publishes `Building` WITHOUT running the build), then call
//! `resume_building_indexes_after_open()` — the same entry point open() uses.
//!
//! Verdict: if resume PROPAGATES the `DuplicateKey` -> REAL (open bricked).
//! After the fix, resume must SUCCEED and drop the orphan Building index, so
//! reopen completes and the namespace is usable.

use super::*;

use std::sync::Arc;

use bson::doc;

use crate::error::Result;
use crate::index::IndexModel;
use crate::options::{FindOptions, IndexOptions};
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::catalog::IndexState;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.bugsuspect.resume_brick";
const UNIQUE_INDEX: &str = "tag_unique";

fn buffered_engine() -> Result<PagedEngine> {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, 0, 0)
}

fn unique_tag_index_model() -> IndexModel {
    IndexModel::builder()
        .keys(doc! { "tag": 1 })
        .options(IndexOptions::new().unique(true))
        .build()
}

fn index_state(engine: &PagedEngine) -> Option<IndexState> {
    let _md = engine.metadata.read().ok()?;
    engine
        .metadata_state
        .catalog_lock()
        .get_index(NS, UNIQUE_INDEX)
        .ok()?
        .map(|idx| idx.state)
}

#[test]
fn resume_does_not_brick_reopen_on_deterministic_unique_violation() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");

    // Two documents with the SAME `tag` value — a unique index on `tag`
    // cannot be built over this data.
    engine
        .insert(NS, doc! { "_id": 1, "tag": "dup" })
        .expect("insert doc 1");
    engine
        .insert(NS, doc! { "_id": 2, "tag": "dup" })
        .expect("insert doc 2");

    // Reserve the UNIQUE index: this durably publishes a `Building` entry
    // WITHOUT running the build (the live driver would build + cleanup; here
    // we stop right after reserve, modelling a process that crashed between
    // publishing Building and finishing/cleaning up the build).
    let outcome = engine
        .create_index_reserve(NS, &unique_tag_index_model(), UNIQUE_INDEX)
        .expect("reserve Building unique index");
    assert!(matches!(
        outcome,
        super::index_maint::ReserveOutcome::Reserved(_)
    ));
    assert_eq!(
        index_state(&engine),
        Some(IndexState::Building),
        "precondition: the unique index is left in Building state"
    );

    // This is the exact entry point open() uses on reopen.
    let resume = engine.resume_building_indexes_after_open();

    // BUG: resume `?`-propagates the unique-violation DuplicateKey, so open()
    // would return Err forever — the database is permanently un-openable.
    assert!(
        resume.is_ok(),
        "resume_building_indexes_after_open bricked reopen: a Building unique \
         index over duplicate data re-hit its deterministic build error with \
         no create_index_cleanup fallback. open() would fail on every reopen. \
         err = {:?}",
        resume.err()
    );

    // After a correct resume, the orphan Building index must be gone (dropped
    // by cleanup) so the namespace is usable. The data must remain intact.
    assert_eq!(
        index_state(&engine),
        None,
        "the un-buildable orphan Building index must be cleaned up on resume"
    );
    let (docs, _) = engine
        .find(NS, &doc! {}, &FindOptions::default())
        .expect("find after successful resume");
    assert_eq!(docs.len(), 2, "documents must survive the resume cleanup");
}
