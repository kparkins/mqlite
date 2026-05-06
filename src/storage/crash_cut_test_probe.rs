//! Phase 0 integration-test probes.
//!
//! This module is deliberately separate from the production storage engine
//! contract. It exists only because Phase 0 integration tests need to freeze
//! specific crash points in the current write envelope.

/// Phase 0 write-envelope probe cut point.
///
/// This is a `#[doc(hidden)]` integration-test support type. It is not part of
/// the stable storage API.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0ProbeCut {
    /// Complete the write, but sample visibility immediately before publish.
    CompleteWithPrePublishProbe,
    /// Stop after S3 body staging, before S4 `allocate_commit_ts`.
    AfterStageBeforeCommitTs,
    /// Stop after S4 `allocate_commit_ts`, before S5 logical-frame build.
    AfterCommitTsBeforeLogicalFrame,
    /// Stop after S5 logical-frame build, before S6 logical append/fsync.
    AfterLogicalFrameBeforeAppend,
    /// Stop after S6 logical append/fsync, before S7 `ChainCommit`.
    AfterLogicalAppendBeforeChainCommit,
    /// Stop after S7 `ChainCommit`, before S8 secondary-index install.
    AfterChainCommitBeforeSecondaryInstall,
    /// Stop after S9 primary install, before S10 structural batch commit.
    AfterPrimaryInstallBeforeStructuralBatchCommit,
    /// Stop after S10 structural batch commit, before S11 flush.
    AfterStructuralBatchCommitBeforeFlush,
    /// Stop after S11 flush, before S12 publish.
    AfterStructuralFlushBeforePublish,
    /// Pre-Phase-3 cut: after `allocate_commit_ts`, before primary install.
    AfterAllocateCommitTs,
    /// Pre-Phase-3 cut: after primary install, before structural batch commit.
    AfterInstallPendingPrimary,
    /// Pre-Phase-3 cut: after structural batch commit, before journal flush.
    AfterStructuralBatchCommit,
    /// Pre-Phase-3 cut: after journal flush, before `ChainCommit`.
    AfterFlushBeforeChainCommit,
    /// Pre-Phase-3 cut: after `ChainCommit`, before `commit_txn`.
    AfterChainCommitBeforeCommitTxn,
    /// Pre-Phase-3 cut: after `commit_txn`, before publish.
    AfterCommitTxnBeforePublish,
}

/// Result returned by the hidden Phase 0 write-envelope probe.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Phase0ProbeReport {
    /// Commit timestamp allocated for the probed write.
    pub commit_ts: Option<(u64, u32)>,
    /// Publish timestamp used when the probe completed publication.
    pub publish_ts: Option<(u64, u32)>,
    /// Whether a reader on the currently published snapshot saw the document
    /// immediately before publication.
    pub pre_publish_visible: Option<bool>,
    /// Whether a reader on the new snapshot saw the document after publication.
    pub post_publish_visible: Option<bool>,
}
