//! BUG-6 regression tests: `commit_catalog_batch_to_log` used to drop the
//! `StructuralPageBatch` without `abort` when `catalog_commit_payload` /
//! `reserve_log_record` failed non-fatally, and the DDL caller
//! (`run_namespace_create_ddl`) only `mark_aborted`ed the publish slot.
//! Because the DDL body mutated the live metadata `Catalog` BEFORE the
//! commit, the in-memory catalog ended up disagreeing with the published
//! epoch and with durable state. The commit path now aborts the batch on
//! every non-fatal pre-reservation failure and the create path rolls back
//! its in-memory catalog mutation.

use std::sync::Arc;
use std::time::Duration;

use bson::doc;

use super::*;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.bug6";

fn buffered_engine_with_busy(busy: Duration) -> PagedEngine {
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
    PagedEngine::new_buffered_with_busy(handle, 0, 0, busy, None, 3, DurabilityMode::default())
        .expect("create buffered engine")
}

/// Arm a one-shot non-fatal failure at the catalog log-record reservation,
/// run `create_namespace`, and assert the engine does not end up torn:
/// after the failed (aborted) DDL the metadata catalog and the published
/// epoch must agree about whether the namespace exists.
#[test]
fn failed_namespace_create_commit_must_not_tear_catalog_state() {
    let engine = buffered_engine_with_busy(Duration::from_millis(200));
    super::hidden_accessors::arm_catalog_commit_reserve_failure(&engine.shared);

    let err = engine
        .create_namespace(NS)
        .expect_err("armed catalog-commit reservation failpoint must fail create_namespace");
    assert!(
        matches!(err, Error::Internal(_)),
        "expected the injected non-fatal reservation failure, got {err:?}"
    );

    let published_has = engine
        .shared
        .load_published()
        .catalog
        .get_by_name(NS)
        .is_some();
    let metadata_has = engine
        .metadata_state
        .catalog_lock()
        .get_collection(NS)
        .expect("metadata catalog read")
        .is_some();
    assert_eq!(
        metadata_has, published_has,
        "aborted create_namespace left the metadata catalog and the published epoch \
         disagreeing about '{NS}' (metadata_has={metadata_has}, published_has={published_has})"
    );
}

/// After a non-fatally failed `create_namespace`, a subsequent insert into
/// that namespace must either bootstrap it or fail-and-recover; today the
/// torn catalog makes `run_write` skip bootstrap (metadata catalog has the
/// entry) while writer visibility resolves against the published epoch
/// (which does not), so the insert spins on `CollectionNotFound` until the
/// busy timeout and then fails.
#[test]
fn insert_after_failed_namespace_create_commit_bootstraps_namespace() {
    let engine = buffered_engine_with_busy(Duration::from_millis(200));
    super::hidden_accessors::arm_catalog_commit_reserve_failure(&engine.shared);
    let _ = engine
        .create_namespace(NS)
        .expect_err("armed catalog-commit reservation failpoint must fail create_namespace");

    let result = engine.insert(NS, doc! { "_id": 1, "value": "post-failure" });
    assert!(
        result.is_ok(),
        "insert after an aborted create_namespace must bootstrap the namespace and \
         succeed, got {result:?}"
    );
}
