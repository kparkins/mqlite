//! Hidden Phase 0 integration-test client hooks.

use crate::error::Result;
use crate::{Phase0ProbeCut, Phase0ProbeReport};

use super::Client;

impl Client {
    /// Hidden Phase 0 write-envelope probe.
    ///
    /// This exists only for integration tests that freeze the current
    /// `allocate_commit_ts -> install -> structural-batch commit -> flush
    /// -> ChainCommit -> commit_txn -> publish` ordering. It must not be used
    /// by application code.
    #[doc(hidden)]
    pub fn __crash_cut_probe_insert(
        &self,
        ns: &str,
        doc: bson::Document,
        cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        self.inner.engine.crash_cut_probe_insert(ns, doc, cut)
    }
}
