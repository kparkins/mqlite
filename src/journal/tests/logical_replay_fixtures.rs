//! US-018 test-only logical replay frame builder.
//!
//! The production recovery parser stays in `src/journal/recovery.rs`, while
//! integration tests use this module to append precise logical-frame tails
//! without making journal internals public API.

use std::fs;
use std::path::Path;

use bson::{doc, Bson};

use crate::error::Result;
use crate::journal::log_file::{
    ChainCommitFrame, LogRecordDraft, LogicalOp, LogicalOpKind, LogicalTxnFrame, OverflowRefWire,
    LOGICAL_TXN_FORMAT_VERSION,
};
use crate::journal::JournalManager;
use crate::keys::encode_key;
use crate::mvcc::Ts;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};

/// Test-only logical insert frame specification for US-018 recovery tests.
#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct Us018LogicalReplayFrame {
    /// Stable namespace id captured from the test catalog.
    pub ns_id: i64,
    /// Primary key for the synthetic insert.
    pub id: i32,
    /// Value stored in the synthetic BSON document.
    pub value: String,
    /// Physical component of the synthetic commit timestamp.
    pub commit_ts_physical_ms: u64,
    /// Logical component of the synthetic commit timestamp.
    pub commit_ts_logical: u32,
    /// Operation ordinal encoded into the single-op logical frame.
    pub op_ordinal: u32,
    /// When set, append an unsupported overflow op so replay fails mid-open.
    pub use_bad_overflow: bool,
}

/// Append synthetic durable logical inserts plus matching `ChainCommit` frames.
///
/// # Errors
///
/// Returns any I/O or journal-encoding error raised while opening the database
/// file, appending the logical frames, appending their matching chain commits,
/// or syncing the journal.
pub fn append_logical_replay_frames(
    db_path: &Path,
    frames: &[Us018LogicalReplayFrame],
) -> Result<()> {
    let mut main_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(db_path)?;
    let header = read_main_header(db_path)?;
    let mut journal = JournalManager::open_or_create(db_path, &header, &mut main_file)?;
    let (salt1, salt2) = journal.salts();
    let publish_seq_base = journal.recovered_max_publish_seq().unwrap_or(0);
    for (idx, frame) in frames.iter().enumerate() {
        let commit_ts = frame.commit_ts();
        let logical_payload = frame.to_logical_txn(salt1, salt2)?.encode()?;
        let chain_payload = ChainCommitFrame {
            salt1,
            salt2,
            commit_ts,
            refcount_deltas: Vec::new(),
            page_writes: Vec::new(),
        }
        .encode()?;
        let draft = LogRecordDraft::crud(
            u64::from(frame.op_ordinal),
            publish_seq_base + idx as u64 + 1,
            commit_ts,
            logical_payload,
            chain_payload,
        );
        let reserved = journal.reserve_log_record(draft)?;
        reserved.write_and_mark()?;
    }
    journal.sync_journal()
}

impl Us018LogicalReplayFrame {
    fn commit_ts(&self) -> Ts {
        Ts {
            physical_ms: self.commit_ts_physical_ms,
            logical: self.commit_ts_logical,
        }
    }

    fn to_logical_txn(&self, salt1: u32, salt2: u32) -> Result<LogicalTxnFrame> {
        Ok(LogicalTxnFrame {
            salt1,
            salt2,
            commit_ts: self.commit_ts(),
            diagnostic_txn_id: u64::from(self.op_ordinal),
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![self.to_logical_op()?],
        })
    }

    fn to_logical_op(&self) -> Result<LogicalOp> {
        let kind = if self.use_bad_overflow {
            LogicalOpKind::PrimaryInsert {
                ns_id: self.ns_id,
                key: encode_key(&Bson::Int32(self.id)),
                value: Vec::new(),
                overflow: Some(OverflowRefWire {
                    first_page: u32::MAX,
                    total_len: 1,
                }),
            }
        } else {
            LogicalOpKind::PrimaryInsert {
                ns_id: self.ns_id,
                key: encode_key(&Bson::Int32(self.id)),
                value: bson::to_vec(&doc! { "_id": self.id, "v": &self.value })
                    .map_err(crate::error::Error::BsonSerialization)?,
                overflow: None,
            }
        };
        Ok(LogicalOp {
            op_ordinal: self.op_ordinal,
            kind,
        })
    }
}

fn read_main_header(db_path: &Path) -> Result<FileHeader> {
    use std::io::{Read, Seek, SeekFrom};

    let mut main_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(db_path)?;
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    main_file.seek(SeekFrom::Start(0))?;
    main_file.read_exact(&mut buf)?;
    FileHeader::from_bytes(&buf)
}
