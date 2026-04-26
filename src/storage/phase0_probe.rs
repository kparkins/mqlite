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
    /// Stop after `allocate_commit_ts`, before primary-chain installation.
    AfterAllocateCommitTs,
    /// Stop after primary-chain installation, before `overlay.commit`.
    AfterInstallPendingPrimary,
    /// Stop after `overlay.commit`, before flushing the journal.
    AfterOverlayCommit,
    /// Stop after flushing overlay bytes, before appending `ChainCommit`.
    AfterFlushBeforeChainCommit,
    /// Stop after appending `ChainCommit`, before `commit_txn`.
    AfterChainCommitBeforeCommitTxn,
    /// Stop after `commit_txn`, before `rebuild_and_publish_locked`.
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
