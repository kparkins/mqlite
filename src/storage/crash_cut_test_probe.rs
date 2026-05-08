//! Integration-test crash probes for the current write envelope.
//!
//! This module is deliberately separate from the production storage engine
//! contract. It exists only because integration tests need to freeze specific
//! crash points in the current write envelope.

/// Phase 0 write-envelope probe cut point.
///
/// This is a `#[doc(hidden)]` integration-test support type. It is not part of
/// the stable storage API.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase0ProbeCut {
    /// Stop after body staging, before `allocate_commit_ts`.
    AfterStageBeforeCommitTs,
    /// Stop after `allocate_commit_ts`, before logical-frame build.
    AfterCommitTsBeforeLogicalFrame,
    /// Stop after logical-frame build, before log-record reservation.
    AfterLogicalFrameBeforeReservation,
    /// Stop after pending primary/index install, before log-record reservation.
    AfterPendingInstallBeforeReservation,
    /// Stop after the log record is written, before waiting for durability.
    AfterLogRecordWriteBeforeDurabilityWait,
    /// Stop after the log record is durable, before publish.
    AfterDurabilityWaitBeforePublish,
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
