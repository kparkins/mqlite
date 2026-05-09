//! Test-only `impl Client` accessors — NOT part of the production API.
//!
//! Every method here is `#[doc(hidden)]`, prefixed with `__`, and
//! used EXCLUSIVELY by integration and unit tests (tests/*.rs,
//! src/**/tests.rs). None of these methods should ever be invoked by
//! application code.
//!
//! Separated into its own module so the boundary between production
//! `Client` behavior (src/client/handle.rs) and test-support scaffolding
//! is visible at a glance — matches the Phase 0 convention of keeping
//! test helpers out of the primary code path
//! (tests/crash_harness.rs) and extends it to in-crate accessors that
//! integration tests need.

use std::io::{Read, Seek, SeekFrom};
use std::sync::{
    atomic::AtomicBool,
    mpsc::{Receiver, Sender},
    Arc,
};

use super::handle::Client;

/// Test-only Phase 8 log-record kind summary.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum JournalLogRecordKind {
    /// CRUD commit record.
    CrudCommit,
    /// Catalog commit record.
    CatalogCommit,
    /// Checkpoint boundary control record.
    CheckpointBoundary,
}

/// Test-only Phase 8 catalog-commit kind summary.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum JournalCatalogCommitKind {
    /// Namespace create or implicit collection bootstrap.
    NamespaceCreate,
    /// Namespace drop.
    NamespaceDrop,
    /// Building index reservation.
    IndexReserve,
    /// Index build page/catalog update.
    IndexBuild,
    /// Building index Ready transition.
    IndexBuildCommit,
    /// Failed Building index cleanup.
    IndexCleanup,
    /// Ready index drop.
    IndexDrop,
}

/// Test-only decoded Phase 8 log-record summary.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub struct JournalLogRecordSummary {
    /// Inclusive byte-LSN where the record starts.
    pub start_lsn: u64,
    /// Exclusive byte-LSN where the record ends.
    pub end_lsn: u64,
    /// Diagnostic transaction id stored in the record.
    pub txn_id: u64,
    /// Publish sequence stored in the record.
    pub publish_seq: u64,
    /// Commit timestamp as `(physical_ms, logical)`.
    pub commit_ts: (u64, u32),
    /// Outer Phase 8 record kind.
    pub kind: JournalLogRecordKind,
    /// Catalog operation kind for `CatalogCommit` records.
    pub catalog_kind: Option<JournalCatalogCommitKind>,
    /// Catalog generation before the operation.
    pub catalog_generation_before: Option<u64>,
    /// Catalog generation after the operation.
    pub catalog_generation_after: Option<u64>,
    /// Header checkpoint-applied LSN carried by a catalog payload.
    pub catalog_header_checkpoint_applied_lsn: Option<u64>,
    /// Checkpoint-applied LSN carried by a `CheckpointBoundary` payload.
    pub checkpoint_applied_lsn: Option<u64>,
    /// Checkpoint timestamp carried by a `CheckpointBoundary` header.
    pub checkpoint_last_ts: Option<(u64, u32)>,
}

#[cfg(any(test, feature = "test-hooks"))]
fn journal_log_record_kind(kind: crate::journal::log_file::LogRecordKind) -> JournalLogRecordKind {
    match kind {
        crate::journal::log_file::LogRecordKind::CrudCommit => JournalLogRecordKind::CrudCommit,
        crate::journal::log_file::LogRecordKind::CatalogCommit => {
            JournalLogRecordKind::CatalogCommit
        }
        crate::journal::log_file::LogRecordKind::CheckpointBoundary => {
            JournalLogRecordKind::CheckpointBoundary
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn journal_catalog_kind(
    kind: crate::journal::log_file::CatalogCommitKind,
) -> JournalCatalogCommitKind {
    match kind {
        crate::journal::log_file::CatalogCommitKind::NamespaceCreate => {
            JournalCatalogCommitKind::NamespaceCreate
        }
        crate::journal::log_file::CatalogCommitKind::NamespaceDrop => {
            JournalCatalogCommitKind::NamespaceDrop
        }
        crate::journal::log_file::CatalogCommitKind::IndexReserve => {
            JournalCatalogCommitKind::IndexReserve
        }
        crate::journal::log_file::CatalogCommitKind::IndexBuild => {
            JournalCatalogCommitKind::IndexBuild
        }
        crate::journal::log_file::CatalogCommitKind::IndexBuildCommit => {
            JournalCatalogCommitKind::IndexBuildCommit
        }
        crate::journal::log_file::CatalogCommitKind::IndexCleanup => {
            JournalCatalogCommitKind::IndexCleanup
        }
        crate::journal::log_file::CatalogCommitKind::IndexDrop => {
            JournalCatalogCommitKind::IndexDrop
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn journal_log_record_summary(
    record: crate::journal::log_file::LogRecord,
) -> crate::error::Result<JournalLogRecordSummary> {
    let mut catalog_kind = None;
    let mut catalog_generation_before = None;
    let mut catalog_generation_after = None;
    let mut catalog_header_checkpoint_applied_lsn = None;
    let mut checkpoint_applied_lsn = None;
    let mut checkpoint_last_ts = None;

    match &record.payload {
        crate::journal::log_file::LogRecordPayload::CatalogCommit(payload) => {
            let payload = crate::journal::log_file::CatalogCommitPayload::decode(payload)?;
            catalog_kind = Some(journal_catalog_kind(payload.kind));
            catalog_generation_before = Some(payload.catalog_generation_before);
            catalog_generation_after = Some(payload.catalog_generation_after);
            catalog_header_checkpoint_applied_lsn = Some(payload.header.checkpoint_applied_lsn);
        }
        crate::journal::log_file::LogRecordPayload::CheckpointBoundary(payload) => {
            let payload = crate::journal::log_file::CheckpointBoundaryPayload::decode(payload)?;
            checkpoint_applied_lsn = Some(payload.checkpoint_applied_lsn);
            checkpoint_last_ts = Some((
                payload.header.last_checkpoint_ts.physical_ms,
                payload.header.last_checkpoint_ts.logical,
            ));
        }
        crate::journal::log_file::LogRecordPayload::CrudCommit { .. } => {}
    }

    Ok(JournalLogRecordSummary {
        start_lsn: record.start_lsn,
        end_lsn: record.end_lsn,
        txn_id: record.txn_id,
        publish_seq: record.publish_seq,
        commit_ts: (record.commit_ts.physical_ms, record.commit_ts.logical),
        kind: journal_log_record_kind(record.kind),
        catalog_kind,
        catalog_generation_before,
        catalog_generation_after,
        catalog_header_checkpoint_applied_lsn,
        checkpoint_applied_lsn,
        checkpoint_last_ts,
    })
}

#[cfg(any(test, feature = "test-hooks"))]
fn read_journal_log_records(
    path: &std::path::Path,
) -> crate::error::Result<Vec<JournalLogRecordSummary>> {
    use crate::journal::log_file::{
        LogRecord, JOURNAL_HEADER_SIZE, LOG_RECORD_HEADER_LEN, LOG_RECORD_TOTAL_LEN_OFFSET,
        MAX_LOG_RECORD_BYTES,
    };

    let journal_path = crate::journal::journal_path_for(path);
    let mut file = match std::fs::File::open(&journal_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(crate::error::Error::Io(error)),
    };
    let len = file.metadata().map_err(crate::error::Error::Io)?.len();
    let mut cursor = JOURNAL_HEADER_SIZE as u64;
    let mut records = Vec::new();

    while cursor < len {
        if len.saturating_sub(cursor) < LOG_RECORD_HEADER_LEN as u64 {
            break;
        }
        file.seek(SeekFrom::Start(cursor))
            .map_err(crate::error::Error::Io)?;
        let mut header = [0u8; LOG_RECORD_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(crate::error::Error::Io)?;
        let total_len = u32::from_le_bytes(
            header[LOG_RECORD_TOTAL_LEN_OFFSET..LOG_RECORD_TOTAL_LEN_OFFSET + 4]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        if !(LOG_RECORD_HEADER_LEN..=MAX_LOG_RECORD_BYTES).contains(&total_len) {
            break;
        }
        let Some(record_end_lsn) = cursor.checked_add(total_len as u64) else {
            break;
        };
        if record_end_lsn > len {
            break;
        }
        let mut bytes = vec![0u8; total_len];
        bytes[..LOG_RECORD_HEADER_LEN].copy_from_slice(&header);
        file.read_exact(&mut bytes[LOG_RECORD_HEADER_LEN..])
            .map_err(crate::error::Error::Io)?;
        let record = LogRecord::decode(&bytes)?;
        if record.start_lsn != cursor {
            break;
        }
        cursor = record.end_lsn;
        records.push(journal_log_record_summary(record)?);
    }

    Ok(records)
}

impl Client {
    /// Test-only accessor for the MVCC `ReadViewRegistry` backing this
    /// client.
    ///
    /// Exposed for integration tests that need to register external
    /// `ReadView`s and watch them get force-expired on the engine's
    /// drop path. Returns `None` when the client has no attached
    /// buffer pool.
    #[doc(hidden)]
    #[must_use]
    pub fn __read_view_registry(&self) -> Option<Arc<crate::mvcc::ReadViewRegistry>> {
        self.inner.engine.read_view_registry()
    }

    /// Test-only accessor: sample the timestamp oracle's current
    /// `(physical_ms, logical)`.
    ///
    /// After each committed write the oracle's value is >= that
    /// write's `commit_ts`. Integration tests use this to capture a
    /// lower-bound on the highest issued `commit_ts` and verify that
    /// after reopen the oracle is floored above that value
    /// (Contract 3.4).
    #[doc(hidden)]
    #[must_use]
    pub fn __oracle_now(&self) -> (u64, u32) {
        self.inner.engine.oracle_now()
    }

    /// Test-only accessor: sample the current published
    /// `PublishedEpoch.visible_ts`.
    ///
    /// Used by §10.6 / US-010 open-time bootstrap tests to verify
    /// that the initial PublishedEpoch carries a Ts >= the pre-crash
    /// oracle floor.
    #[doc(hidden)]
    #[must_use]
    pub fn __published_visible_ts(&self) -> (u64, u32) {
        self.inner.engine.published_visible_ts()
    }

    /// Test-only accessor: sample the published-catalog rebuild generation.
    ///
    /// This does not depend on allocator address reuse behavior. It advances
    /// only when a commit publishes a new `Arc<PublishedCatalog>`.
    #[doc(hidden)]
    #[must_use]
    pub fn __published_catalog_gen(&self) -> u64 {
        self.inner.engine.published_catalog_gen()
    }

    /// Test-only accessor: sample the current published sequencer frontier.
    ///
    /// Phase 3 recovery tests use this to verify the post-open published
    /// epoch binds visibility and sequencer frontier to the same durable
    /// recovery timestamp.
    #[doc(hidden)]
    #[must_use]
    pub fn __published_sequencer_frontier(&self) -> (u64, u32) {
        self.inner.engine.published_sequencer_frontier()
    }

    /// Test-only accessor: number of post-open recovery epoch stores for
    /// this client engine.
    #[doc(hidden)]
    #[must_use]
    pub fn __recovery_open_published_store_count(&self) -> u64 {
        self.inner.engine.recovery_open_published_store_count()
    }

    /// Test-only accessor: return the highest `ChainCommit.commit_ts`
    /// seen during the most recent journal recovery, encoded as
    /// `Some((physical_ms, logical))`, or `None` when the journal was
    /// fresh or carried no `ChainCommit` frames.
    ///
    /// The US-002 crash harness reads this to verify that after a
    /// journal-tail truncation the recovered HLC floor drops
    /// accordingly.
    #[doc(hidden)]
    #[must_use]
    pub fn __recovered_max_commit_ts(&self) -> Option<(u64, u32)> {
        self.inner.engine.recovered_max_commit_ts()
    }

    /// Test-only accessor: decode complete Phase 8 log records currently
    /// present in this client's journal file.
    ///
    /// # Errors
    /// Returns an error if the client is in-memory, the journal cannot be
    /// read, or a complete record is structurally invalid.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn __journal_log_records(&self) -> crate::error::Result<Vec<JournalLogRecordSummary>> {
        let path =
            self.inner.path.as_ref().ok_or_else(|| {
                crate::error::Error::Internal("client has no database path".into())
            })?;
        read_journal_log_records(path)
    }

    /// Test-only US-019 fault injector: fail the next `failures`
    /// primary-install attempts inside the post-S7 commit window.
    #[doc(hidden)]
    pub fn __us019_set_primary_install_failures(&self, failures: u8) {
        self.inner
            .engine
            .us019_set_primary_install_failures(failures);
    }

    /// Test-only US-019 counter for primary-install attempts.
    #[doc(hidden)]
    #[must_use]
    pub fn __us019_primary_install_attempts(&self) -> u64 {
        self.inner.engine.us019_primary_install_attempts()
    }

    /// Test-only US-009 hook: inspect resident primary-chain states.
    ///
    /// # Errors
    /// Returns an internal error if the namespace or resident leaf cannot be
    /// inspected.
    #[doc(hidden)]
    pub fn __us009_primary_chain_states(
        &self,
        ns: &str,
        id: &crate::bson::Bson,
    ) -> crate::error::Result<Vec<String>> {
        self.inner.engine.us009_primary_chain_states(ns, id)
    }

    /// Test-only US-009 hook: inject a committed resident primary head.
    ///
    /// # Errors
    /// Returns an internal error if the namespace or resident leaf cannot be
    /// inspected.
    #[doc(hidden)]
    pub fn __us009_inject_primary_committed_head(
        &self,
        ns: &str,
        doc: &crate::bson::Document,
        commit_ts: crate::mvcc::Ts,
        txn_id: u64,
    ) -> crate::error::Result<()> {
        self.inner
            .engine
            .us009_inject_primary_committed_head(ns, doc, commit_ts, txn_id)
    }

    /// Test-only US-009 hook: inspect resident secondary-chain states.
    ///
    /// # Errors
    /// Returns an internal error if the index or resident leaf cannot be
    /// inspected.
    #[doc(hidden)]
    pub fn __us009_secondary_chain_states(
        &self,
        ns: &str,
        index_name: &str,
        doc: &crate::bson::Document,
        id: &crate::bson::Bson,
    ) -> crate::error::Result<Vec<String>> {
        self.inner
            .engine
            .us009_secondary_chain_states(ns, index_name, doc, id)
    }

    /// Test-only US-009 hook: reset pending-flip / publish ordering probes.
    #[doc(hidden)]
    pub fn __us009_reset_flip_publish_order(&self) {
        self.inner.engine.us009_reset_flip_publish_order();
    }

    /// Test-only US-009 hook: pending-flip order and publish-ready order.
    #[doc(hidden)]
    #[must_use]
    pub fn __us009_flip_publish_order(&self) -> (u64, u64) {
        self.inner.engine.us009_flip_publish_order()
    }

    /// Test-only US-009 hook: fail after committed flip before publish.
    #[doc(hidden)]
    pub fn __us009_fail_after_committed_flip_once(&self) {
        self.inner.engine.us009_fail_after_committed_flip_once();
    }

    /// Test-only US-028 hook: resolve the primary leaf page for `_id`.
    ///
    /// # Errors
    /// Returns an internal error if the namespace or primary leaf cannot be
    /// resolved.
    #[doc(hidden)]
    pub fn __us028_primary_leaf_for_id(
        &self,
        ns: &str,
        id: &crate::bson::Bson,
    ) -> crate::error::Result<u32> {
        self.inner.engine.us028_primary_leaf_for_id(ns, id)
    }

    /// Test-only US-022 hook: insert two documents in one storage write txn.
    ///
    /// # Errors
    /// Returns an engine error if either insert cannot be staged or committed.
    #[doc(hidden)]
    pub fn __us022_insert_two_docs_one_txn(
        &self,
        ns: &str,
        left: crate::bson::Document,
        right: crate::bson::Document,
    ) -> crate::error::Result<()> {
        self.inner
            .engine
            .us022_insert_two_docs_one_txn(ns, left, right)
    }

    /// Test-only US-028 hook: hold the reconcile exclusive latch for `_id`.
    ///
    /// # Errors
    /// Returns an internal error if the namespace or leaf cannot be resolved,
    /// or if the rendezvous channels close unexpectedly.
    #[doc(hidden)]
    pub fn __us028_hold_primary_leaf_reconcile_latch(
        &self,
        ns: &str,
        id: &crate::bson::Bson,
        ready: Sender<()>,
        release: Receiver<()>,
    ) -> crate::error::Result<()> {
        self.inner
            .engine
            .us028_hold_primary_leaf_reconcile_latch(ns, id, ready, release)
    }

    /// Test-only US-028 hook: hold the writer exclusive latch for `_id`.
    ///
    /// # Errors
    /// Returns an internal error if the namespace or leaf cannot be resolved,
    /// or if the rendezvous channels close unexpectedly.
    #[doc(hidden)]
    pub fn __us028_hold_primary_leaf_writer_latch(
        &self,
        ns: &str,
        id: &crate::bson::Bson,
        ready: Sender<()>,
        release: Receiver<()>,
    ) -> crate::error::Result<()> {
        self.inner
            .engine
            .us028_hold_primary_leaf_writer_latch(ns, id, ready, release)
    }

    /// Test-only US-025 hook: hold the reader shared latch for `_id`.
    ///
    /// # Errors
    /// Returns an internal error if the namespace or leaf cannot be resolved,
    /// or if the rendezvous channels close unexpectedly.
    #[doc(hidden)]
    pub fn __us025_hold_primary_leaf_reader_latch(
        &self,
        ns: &str,
        id: &crate::bson::Bson,
        ready: Sender<()>,
        release: Receiver<()>,
    ) -> crate::error::Result<()> {
        self.inner
            .engine
            .us025_hold_primary_leaf_reader_latch(ns, id, ready, release)
    }

    /// Test-only US-026 hook: arm one post-register cleanup failpoint.
    #[doc(hidden)]
    pub fn __us026_arm_post_register_failpoint(
        &self,
        failpoint: crate::Us026PostRegisterFailpoint,
    ) {
        self.inner
            .engine
            .us026_arm_post_register_failpoint(failpoint);
    }

    /// Test-only Phase 8 hook: snapshot `(next_lsn, ready_lsn, durable_lsn)`.
    ///
    /// # Errors
    /// Returns an internal error if the journal manager cannot be inspected.
    #[doc(hidden)]
    pub fn __journal_lsn_snapshot(&self) -> crate::error::Result<(u64, u64, u64)> {
        self.inner.engine.journal_lsn_snapshot()
    }

    /// Test-only Phase 8 hook: fail after log reservation before dirty LSN stamp.
    #[doc(hidden)]
    pub fn __fail_next_dirty_lsn_stamp(&self) {
        self.inner.engine.fail_next_dirty_lsn_stamp();
    }

    /// Test-only Phase 8 hook: fail after dirty LSN stamp before log write.
    #[doc(hidden)]
    pub fn __fail_next_after_dirty_lsn_stamp(&self) {
        self.inner.engine.fail_next_after_dirty_lsn_stamp();
    }

    /// Test-only Phase 8 hook: fail after durability before Pending flip.
    #[doc(hidden)]
    pub fn __fail_next_after_durable_before_flip(&self) {
        self.inner.engine.fail_next_after_durable_before_flip();
    }

    /// Test-only Phase 8 hook: pause after Pending install before reservation.
    #[doc(hidden)]
    #[must_use]
    pub fn __install_before_log_reservation_hook(&self) -> crate::BeforeLogReservationHookGuard {
        self.inner.engine.install_before_log_reservation_hook()
    }

    /// Test-only US-021c hook: pause the next write body for `ns`.
    #[doc(hidden)]
    #[must_use]
    pub fn __install_write_body_entry_hook(&self, ns: &str) -> crate::WriteBodyEntryHookGuard {
        self.inner.engine.install_write_body_entry_hook(ns, None)
    }

    /// Test-only US-021c hook: pause the next write body and capture `flag`.
    #[doc(hidden)]
    #[must_use]
    pub fn __install_write_body_entry_hook_observing(
        &self,
        ns: &str,
        flag: Arc<AtomicBool>,
    ) -> crate::WriteBodyEntryHookGuard {
        self.inner
            .engine
            .install_write_body_entry_hook(ns, Some(flag))
    }

    /// Test-only US-013 hook: pause `create_index_build` at scan entry.
    #[doc(hidden)]
    #[must_use]
    pub fn __install_create_index_build_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> crate::CreateIndexBuildHookGuard {
        self.inner
            .engine
            .install_create_index_build_hook(ns, index_name)
    }

    /// Test-only US-038 hook: pause `create_index_build`, then fail it after
    /// release so `create_index_cleanup` runs.
    #[doc(hidden)]
    #[must_use]
    pub fn __install_create_index_build_failure_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> crate::CreateIndexBuildHookGuard {
        self.inner
            .engine
            .install_create_index_build_failure_hook(ns, index_name)
    }

    /// Test-only US-039 hook: reset append/sync ownership counters.
    #[doc(hidden)]
    pub fn __us039_reset_append_sync_observations(&self) {
        self.inner.engine.us039_reset_append_sync_observations();
    }

    /// Test-only US-039 hook: snapshot append/sync ownership counters.
    #[doc(hidden)]
    #[must_use]
    pub fn __us039_append_sync_observations(&self) -> crate::Us039AppendSyncObservations {
        self.inner.engine.us039_append_sync_observations()
    }

    /// Test-only US-017 hook: reset group-commit probe state.
    #[doc(hidden)]
    pub fn __us017_reset_group_commit_probe(&self) {
        self.inner.engine.us017_reset_group_commit_probe();
    }

    /// Test-only US-017 hook: make the next leader wait for `expected`
    /// joined tickets before closing its cohort.
    #[doc(hidden)]
    pub fn __us017_expect_group_commit_cohort_size(&self, expected: u64) {
        self.inner
            .engine
            .us017_expect_group_commit_cohort_size(expected);
    }

    /// Test-only US-017 hook: fail the next group-commit leader fsync.
    #[doc(hidden)]
    pub fn __us017_fail_next_group_commit_fsync(&self) {
        self.inner.engine.us017_fail_next_group_commit_fsync();
    }

    /// Test-only US-017 hook: pause the next leader after cohort close.
    #[doc(hidden)]
    #[must_use]
    pub fn __us017_pause_next_group_commit_after_close(&self) -> crate::Us017GroupCommitPauseGuard {
        self.inner
            .engine
            .us017_pause_next_group_commit_after_close()
    }

    /// Test-only US-017 hook: snapshot group-commit observations.
    #[doc(hidden)]
    #[must_use]
    pub fn __us017_group_commit_observations(&self) -> crate::Us017GroupCommitObservations {
        self.inner.engine.us017_group_commit_observations()
    }

    /// Test-only US-008 hook: reset committed structural leaf-byte accounting.
    #[doc(hidden)]
    pub fn __us008_reset_structural_page_observations(&self) {
        self.inner.engine.us008_reset_structural_page_observations();
    }

    /// Test-only US-008 hook: committed structural leaf bytes since reset.
    #[doc(hidden)]
    #[must_use]
    pub fn __us008_committed_structural_leaf_bytes(&self) -> u64 {
        self.inner.engine.us008_committed_structural_leaf_bytes()
    }

    /// Test-only US-011 hook: install one pending unique email secondary entry.
    ///
    /// # Errors
    /// Returns [`crate::Error::WriteConflict`] when install-time unique-prefix
    /// scanning finds a live same-prefix entry.
    #[doc(hidden)]
    pub fn __us011_install_pending_unique_email(
        &self,
        ns: &str,
        index_name: &str,
        id: crate::bson::Bson,
        email: &str,
        txn_id: u64,
    ) -> crate::error::Result<()> {
        self.inner
            .engine
            .us011_install_pending_unique_email(ns, index_name, id, email, txn_id)
    }

    /// Test-only US-011 hook: return sibling pages selected by a crossing
    /// unique-prefix range.
    ///
    /// # Errors
    /// Returns an internal error if the synthetic probe leaf cannot be encoded.
    #[doc(hidden)]
    pub fn __us011_unique_prefix_sibling_pages(&self) -> crate::error::Result<Vec<u32>> {
        self.inner.engine.us011_unique_prefix_sibling_pages()
    }

    // §10.8 #19 publish-pause hook is `#[cfg(test)]`-gated inside
    // `src/storage/paged_engine/tests/hidden_accessors.rs` (no `Arc<Barrier>`
    // or atomic pointer in production builds). Its paired rendezvous
    // test therefore lives as a unit test, not an integration test —
    // see `src/storage/paged_engine/tests.rs::publish_happens_strictly_after_commit_txn`.

    // ----- US-036 hooks ---------------------------------------------------

    /// Test-only US-036 hook: poison the live engine with `reason`.
    #[doc(hidden)]
    pub fn __us036_poison_engine(&self, reason: crate::error::EngineFatalReason) {
        self.inner.engine.us036_test_poison_engine(reason);
    }

    /// Test-only US-036 hook: read the current poison reason.
    #[doc(hidden)]
    #[must_use]
    pub fn __us036_poisoned_reason(&self) -> Option<crate::error::EngineFatalReason> {
        self.inner.engine.us036_test_poisoned_reason()
    }

    /// Test-only US-036 hook: register a publish slot.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the sequencer is
    /// poisoned.
    #[doc(hidden)]
    pub fn __us036_register_publish_slot(
        &self,
    ) -> crate::error::Result<crate::storage::paged_engine::engine_fatal_harness::Us036PublishSlot>
    {
        self.inner.engine.us036_test_register_publish_slot()
    }

    /// Test-only US-036 hook: admit a writer ticket on `ns_id`.
    ///
    /// # Errors
    /// Returns [`crate::Error::WriterBusy`] when the lane is closed.
    #[doc(hidden)]
    pub fn __us036_admit_writer(
        &self,
        ns_id: i64,
        timeout_ms: u64,
    ) -> crate::error::Result<crate::storage::paged_engine::engine_fatal_harness::Us036WriterTicket>
    {
        self.inner.engine.us036_test_admit_writer(ns_id, timeout_ms)
    }

    /// Test-only US-036 hook: close-and-drain a namespace lane.
    ///
    /// # Errors
    /// Returns [`crate::Error::WriterBusy`] when the lane fails to drain
    /// within `timeout_ms`.
    #[doc(hidden)]
    pub fn __us036_close_and_drain(&self, ns_id: i64, timeout_ms: u64) -> crate::error::Result<()> {
        self.inner
            .engine
            .us036_test_close_and_drain(ns_id, timeout_ms)
    }

    /// Test-only US-036 hook: resolve `ns` to its durable namespace id.
    ///
    /// AC #7: fail-closed once the engine is poisoned.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the live engine is
    /// poisoned.
    #[doc(hidden)]
    pub fn __us036_namespace_id(&self, ns: &str) -> crate::error::Result<Option<i64>> {
        self.inner.engine.us036_test_namespace_id(ns)
    }
}
