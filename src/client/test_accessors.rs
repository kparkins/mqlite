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

use std::sync::{
    atomic::AtomicBool,
    mpsc::{Receiver, Sender},
    Arc,
};

use super::handle::Client;

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

    /// Test-only US-026 hook: snapshot cleanup observations.
    #[doc(hidden)]
    #[must_use]
    pub fn __us026_cleanup_observations(&self) -> crate::Us026CleanupObservations {
        self.inner.engine.us026_cleanup_observations()
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

    /// Test-only US-007 hook: pause after `begin_txn` while `journal_mutex`
    /// is held.
    #[doc(hidden)]
    #[must_use]
    pub fn __us007_install_journal_begin_hook(
        &self,
        fail_after_release: bool,
    ) -> crate::Us007JournalBeginHookGuard {
        self.inner
            .engine
            .us007_install_journal_begin_hook(fail_after_release)
    }

    /// Test-only US-007 hook: reset journal-envelope counters.
    #[doc(hidden)]
    pub fn __us007_reset_journal_observations(&self) {
        self.inner.engine.us007_reset_journal_observations();
    }

    /// Test-only US-007 hook: snapshot journal-envelope counters.
    #[doc(hidden)]
    #[must_use]
    pub fn __us007_journal_observations(&self) -> crate::Us007JournalObservations {
        self.inner.engine.us007_journal_observations()
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
    // `src/storage/paged_engine/test_accessors.rs` (no `Arc<Barrier>`
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
    ) -> crate::error::Result<crate::storage::paged_engine::engine_fatal_test_probe::Us036PublishSlot>
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
    ) -> crate::error::Result<
        crate::storage::paged_engine::engine_fatal_test_probe::Us036WriterTicket,
    > {
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
