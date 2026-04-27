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

use std::sync::{atomic::AtomicBool, Arc};

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

    // §10.8 #19 publish-pause hook is `#[cfg(test)]`-gated inside
    // `src/storage/paged_engine/test_accessors.rs` (no `Arc<Barrier>`
    // or atomic pointer in production builds). Its paired rendezvous
    // test therefore lives as a unit test, not an integration test —
    // see `src/storage/paged_engine/tests.rs::publish_happens_strictly_after_commit_txn`.
}
