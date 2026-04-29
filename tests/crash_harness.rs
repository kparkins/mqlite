#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! US-002 crash-cut harness — journal-tail truncation and reopen-after-crash
//! inspection helpers for the current two-stage commit envelope.
//!
//! Shared test-support module imported via `#[path = ...]` by several
//! integration tests (crash_harness_smoke, crash_cut_matrix,
//! write_envelope_sequencing, recovery_timestamp_floor). Each consumer uses
//! a different subset, so dead-code lints are silenced at module scope.
#![allow(dead_code)]

//!
//! Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
//! mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
//! (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
//!
//! # Consumers
//!
//! The helpers in this module are used by US-005, US-006, US-007 crash-cut
//! tests and by `tests/crash_harness_smoke.rs`.
//!
//! # Journal format (summary)
//!
//! ```text
//! [Journal Header — 32 bytes]
//!   offset  0: magic "MQJL"
//!   offset  4: format_version u32 LE (1)
//!   offset  8: page_size_internal u32 LE (4096)
//!   offset 12: page_size_leaf u32 LE (32768)
//!   offset 16: salt1 u32 LE
//!   offset 20: salt2 u32 LE
//!   offset 24: checkpoint_seq u32 LE
//!   offset 28: header_checksum u32 LE (CRC32C of bytes 0–27)
//!
//! [Legacy Page Frame]
//!   header 24 bytes: page_number(4) db_page_count(4) salt1(4) salt2(4)
//!                    page_size(4) checksum(4)
//!   followed by page_size bytes of page data
//!
//! [ChainCommit Frame]
//!   offset  0: frame_kind u8 (0x02)
//!   offset  1: reserved [u8; 3]
//!   offset  4: total_frame_bytes u32 LE (length of entire frame incl. checksum)
//!   offset  8: salt1 u32 LE
//!   offset 12: salt2 u32 LE
//!   offset 16: commit_ts [u8; 12] (physical_ms u64 LE || logical u32 LE)
//!   ...  variable-length tail ...
//!   last 4 bytes: CRC32C checksum
//! ```

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use mqlite::{Client, OpenOptions as DbOpenOptions, Result};

// ---------------------------------------------------------------------------
// Journal format constants (duplicated from src/journal/log_file.rs which is
// pub(crate) and not reachable from integration tests)
// ---------------------------------------------------------------------------

/// Size in bytes of the journal file header.
pub const JOURNAL_HEADER_SIZE: u64 = 32;
const JOURNAL_FRAME_HEADER_SIZE: u64 = 24;
const FRAME_KIND_CHAIN_COMMIT: u8 = 0x02;
const FRAME_KIND_LOGICAL_TXN: u8 = 0x03;
const FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY: u8 = 0x04;
const PAGE_SIZE_INTERNAL: u64 = 4096;
const PAGE_SIZE_LEAF: u64 = 32768;
const CHAIN_COMMIT_MAX_FRAME_SIZE: u64 = 64 * 1024 * 1024;
const LOGICAL_TXN_MAX_FRAME_SIZE: u64 = 64 * 1024 * 1024;
const CHECKPOINT_COMMIT_BOUNDARY_TOTAL_BYTES: u64 = 56;

// ---------------------------------------------------------------------------
// FrameKind — the two kinds of frames produced by the current engine
// ---------------------------------------------------------------------------

/// Identifies the two kinds of journal frames written by the current engine.
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// A legacy page-write frame (JournalFrameHeader + page data).
    LegacyPage,
    /// An MVCC chain-commit frame (ChainCommitFrame).
    ChainCommit,
}

// ---------------------------------------------------------------------------
// RecoveryReport — output of reopen_inspect
// ---------------------------------------------------------------------------

/// Recovery statistics captured by [`reopen_inspect`] for one reopen.
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// Number of legacy page-replay frames (`JournalFrameHeader`) processed
    /// by the recovery loop, sourced from
    /// `mvcc::metrics::recovery_legacy_page_frames_snapshot()`.
    pub legacy_page_frame_count: u64,
    /// Number of `ChainCommit` frames processed by the recovery loop, sourced
    /// from `mvcc::metrics::recovery_chain_commit_frames_snapshot()`.
    pub chain_commit_frame_count: u64,
    /// Highest `ChainCommit.commit_ts` observed during recovery, encoded as
    /// `Some((physical_ms, logical))`. `None` when the journal was fresh or
    /// carried no `ChainCommit` frames.
    pub recovered_max_commit_ts: Option<(u64, u32)>,
}

// ---------------------------------------------------------------------------
// Helper: derive journal path from db path
// ---------------------------------------------------------------------------

/// Return the path of the journal file for a given database file path.
///
/// The journal path is `<db_path>-journal`, matching the convention in
/// `src/journal/mod.rs:journal_path_for`.
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
pub fn journal_path(db_path: &Path) -> std::path::PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-journal");
    std::path::PathBuf::from(s)
}

// ---------------------------------------------------------------------------
// truncate_journal_to_offset
// ---------------------------------------------------------------------------

/// Truncate the journal file to exactly `offset` bytes.
///
/// This is the direct-offset variant — use it when you know the exact byte
/// offset you want to cut at (e.g., the current journal size for a no-op
/// test, or an offset returned by a frame scan).
///
/// Panics if `offset` is less than [`JOURNAL_HEADER_SIZE`] (32) since
/// cutting into the header would produce an unrecoverable journal.
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
///
/// # Errors
///
/// Returns `Err` on any I/O error.
pub fn truncate_journal_to_offset(db_path: &Path, offset: u64) -> std::io::Result<()> {
    assert!(
        offset >= JOURNAL_HEADER_SIZE,
        "truncate_journal_to_offset: offset {offset} would cut into the 32-byte journal header"
    );
    let jpath = journal_path(db_path);
    let file = OpenOptions::new().write(true).open(&jpath)?;
    file.set_len(offset)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// truncate_journal_before_frame_kind
// ---------------------------------------------------------------------------

/// Truncate the journal to the offset immediately before the first frame of
/// `kind` found while scanning forward from the journal header.
///
/// The scan finds the first occurrence of a frame whose kind matches `kind`
/// and truncates to the byte offset at the start of that frame. All frames
/// before it are preserved; the named frame and everything after it are
/// discarded.
///
/// If no frame of the requested kind is found, the journal is left unchanged.
///
/// Returns the offset at which the truncation occurred (`None` when no
/// matching frame was found and the journal was not modified).
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
///
/// # Errors
///
/// Returns `Err` on any I/O error or on a malformed journal header.
pub fn truncate_journal_before_frame_kind(
    db_path: &Path,
    kind: FrameKind,
) -> std::io::Result<Option<u64>> {
    let jpath = journal_path(db_path);
    let mut file = OpenOptions::new().read(true).write(true).open(&jpath)?;

    // Read and validate the 32-byte journal header to extract the salts.
    let (salt1, salt2) = read_journal_salts(&mut file)?;

    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(JOURNAL_HEADER_SIZE))?;

    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < file_len {
        file.seek(SeekFrom::Start(cursor))?;

        // Peek the first byte to decide whether this is a ChainCommit frame.
        let mut first_byte = [0u8; 1];
        match file.read_exact(&mut first_byte) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let is_chain = first_byte[0] == FRAME_KIND_CHAIN_COMMIT;
        let is_logical = first_byte[0] == FRAME_KIND_LOGICAL_TXN;

        if is_chain {
            // ChainCommit frame: validate it and compute its total_frame_bytes.
            // We need at least 8 bytes (kind(1)+reserved(3)+total_frame_bytes(4)).
            let mut prefix = [0u8; 8];
            prefix[0] = first_byte[0];
            match file.read_exact(&mut prefix[1..]) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let total_frame_bytes =
                u32::from_le_bytes(prefix[4..8].try_into().expect("4 bytes")) as u64;

            if total_frame_bytes == 0
                || total_frame_bytes > CHAIN_COMMIT_MAX_FRAME_SIZE
                || cursor + total_frame_bytes > file_len
            {
                // Truncated or corrupt ChainCommit — stop here.
                break;
            }

            // Validate the salt fields (bytes 8–15 relative to frame start).
            file.seek(SeekFrom::Start(cursor + 8))?;
            let mut salt_buf = [0u8; 8];
            file.read_exact(&mut salt_buf)?;
            let frame_salt1 = u32::from_le_bytes(salt_buf[0..4].try_into().expect("4 bytes"));
            let frame_salt2 = u32::from_le_bytes(salt_buf[4..8].try_into().expect("4 bytes"));
            if frame_salt1 != salt1 || frame_salt2 != salt2 {
                break;
            }

            if kind == FrameKind::ChainCommit {
                // This is the first ChainCommit — truncate before it.
                file.set_len(cursor)?;
                return Ok(Some(cursor));
            }
            cursor += total_frame_bytes;
        } else if is_logical {
            // Phase 2 LogicalTxnFrame: read kind(1)+reserved(3)+total(4) and
            // skip past it. Same shape discipline as ChainCommit.
            let mut prefix = [0u8; 8];
            prefix[0] = first_byte[0];
            match file.read_exact(&mut prefix[1..]) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let total_frame_bytes =
                u32::from_le_bytes(prefix[4..8].try_into().expect("4 bytes")) as u64;
            if total_frame_bytes == 0
                || total_frame_bytes > LOGICAL_TXN_MAX_FRAME_SIZE
                || cursor + total_frame_bytes > file_len
            {
                break;
            }
            cursor += total_frame_bytes;
        } else {
            // Legacy page frame. The first byte is the low byte of page_number.
            // Re-read the full 24-byte header.
            file.seek(SeekFrom::Start(cursor))?;
            let mut hdr = [0u8; 24];
            match file.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let frame_salt1 = u32::from_le_bytes(hdr[8..12].try_into().expect("4 bytes"));
            let frame_salt2 = u32::from_le_bytes(hdr[12..16].try_into().expect("4 bytes"));
            if frame_salt1 != salt1 || frame_salt2 != salt2 {
                break;
            }

            let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"));
            let page_size = match page_size_u32 {
                4096 => PAGE_SIZE_INTERNAL,
                32768 => PAGE_SIZE_LEAF,
                _ => break, // unknown page size — stop
            };

            // Verify checksum: CRC32C of hdr[0..20] + page_data.
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE;
            if data_offset + page_size > file_len {
                break;
            }
            file.seek(SeekFrom::Start(data_offset))?;
            let mut page_data = vec![0u8; page_size as usize];
            match file.read_exact(&mut page_data) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let stored_cs = u32::from_le_bytes(hdr[20..24].try_into().expect("4 bytes"));
            let computed_cs = {
                let mut d = crc32c_compute(&hdr[..20]);
                d = crc32c_append(d, &page_data);
                d
            };
            if stored_cs != computed_cs {
                break;
            }

            if kind == FrameKind::LegacyPage {
                // First LegacyPage frame — truncate before it.
                file.set_len(cursor)?;
                return Ok(Some(cursor));
            }
            cursor += JOURNAL_FRAME_HEADER_SIZE + page_size;
        }
    }

    Ok(None)
}

// ---------------------------------------------------------------------------
// reopen_inspect
// ---------------------------------------------------------------------------

/// Open the database at `db_path`, collect recovery counters and the
/// recovered HLC floor, then return the [`RecoveryReport`].
///
/// The global recovery counters (`recovery_legacy_page_frames_total` and
/// `recovery_chain_commit_frames_total`) are **reset** immediately before
/// opening the database and **read** immediately after `Client::open_with_options`
/// returns, so the values in the report reflect only the recovery pass for
/// this reopen (not any prior test activity).
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
///
/// # Errors
///
/// Propagates any error returned by `Client::open_with_options`.
pub fn reopen_inspect(db_path: &Path) -> Result<(Client, RecoveryReport)> {
    // Reset counters immediately before open so readings are isolated to this
    // recovery pass.
    mqlite::mvcc::metrics::reset_recovery_legacy_page_frames();
    mqlite::mvcc::metrics::reset_recovery_chain_commit_frames();

    let client = Client::open_with_options(db_path, DbOpenOptions::new())?;

    let legacy_page_frame_count = mqlite::mvcc::metrics::recovery_legacy_page_frames_snapshot();
    let chain_commit_frame_count = mqlite::mvcc::metrics::recovery_chain_commit_frames_snapshot();
    let recovered_max_commit_ts = client.__recovered_max_commit_ts();

    Ok((
        client,
        RecoveryReport {
            legacy_page_frame_count,
            chain_commit_frame_count,
            recovered_max_commit_ts,
        },
    ))
}

// ---------------------------------------------------------------------------
// scan_chain_commits — enumerate all ChainCommit frames in the journal
// ---------------------------------------------------------------------------

/// Scan the journal and return `(frame_offset, commit_ts)` for every
/// validly-framed `ChainCommit` frame found, in journal order.
///
/// `commit_ts` is decoded as `(physical_ms, logical)` from the 12-byte
/// commit-timestamp field at offset 16 of the ChainCommit frame.
///
/// Stops at the first byte offset that does not look like a valid frame
/// (corrupt header, truncated tail, unknown kind). This mirrors the
/// recovery-loop halt criterion in `src/journal/recovery.rs:89-117`.
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's responsibility
/// (docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §2, §4.2).
///
/// # Errors
///
/// Returns `Err` on any I/O error or on a malformed journal header.
#[allow(dead_code)]
pub fn scan_chain_commits(db_path: &Path) -> std::io::Result<Vec<(u64, (u64, u32))>> {
    let jpath = journal_path(db_path);
    let mut file = OpenOptions::new().read(true).open(&jpath)?;

    let (salt1, salt2) = read_journal_salts(&mut file)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(JOURNAL_HEADER_SIZE))?;

    let mut out = Vec::new();
    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < file_len {
        // Both legacy frames and Phase 2 frames place (salt1, salt2) at
        // bytes 8-15 and a u32 at bytes 16-19 (page_size for legacy / body
        // bytes for Phase 2). Disambiguate via that u32: a legal page-size
        // (4096 or 32768) wins regardless of byte 0, otherwise dispatch on
        // byte 0 as a Phase 2 frame kind. This avoids the byte-0 alias
        // where legacy page_number ∈ {2, 3, 4} collides with Phase 2 frame
        // kinds (FRAME_KIND_CHAIN_COMMIT / LOGICAL_TXN / BOUNDARY).
        if cursor + JOURNAL_FRAME_HEADER_SIZE > file_len {
            break;
        }
        file.seek(SeekFrom::Start(cursor))?;
        let mut hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE as usize];
        match file.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let fs1 = u32::from_le_bytes(hdr[8..12].try_into().expect("4 bytes"));
        let fs2 = u32::from_le_bytes(hdr[12..16].try_into().expect("4 bytes"));
        if fs1 != salt1 || fs2 != salt2 {
            break;
        }
        let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"));
        let page_size = match page_size_u32 {
            4096 => Some(PAGE_SIZE_INTERNAL),
            32768 => Some(PAGE_SIZE_LEAF),
            _ => None,
        };
        if let Some(ps) = page_size {
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE;
            if data_offset + ps > file_len {
                break;
            }
            cursor = data_offset + ps;
            continue;
        }
        let kind = hdr[0];
        let total_frame_bytes = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes")) as u64;
        match kind {
            FRAME_KIND_CHAIN_COMMIT => {
                if total_frame_bytes == 0
                    || total_frame_bytes > CHAIN_COMMIT_MAX_FRAME_SIZE
                    || cursor + total_frame_bytes > file_len
                {
                    break;
                }
                let physical_ms = u64::from_le_bytes(hdr[16..24].try_into().expect("8 bytes"));
                file.seek(SeekFrom::Start(cursor + 24))?;
                let mut logical_buf = [0u8; 4];
                if file.read_exact(&mut logical_buf).is_err() {
                    break;
                }
                let logical = u32::from_le_bytes(logical_buf);
                out.push((cursor, (physical_ms, logical)));
                cursor += total_frame_bytes;
            }
            FRAME_KIND_LOGICAL_TXN => {
                if total_frame_bytes == 0
                    || total_frame_bytes > LOGICAL_TXN_MAX_FRAME_SIZE
                    || cursor + total_frame_bytes > file_len
                {
                    break;
                }
                cursor += total_frame_bytes;
            }
            FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY => {
                if total_frame_bytes != CHECKPOINT_COMMIT_BOUNDARY_TOTAL_BYTES
                    || cursor + total_frame_bytes > file_len
                {
                    break;
                }
                cursor += total_frame_bytes;
            }
            _ => break,
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// scan_legacy_commit_frames — enumerate legacy commit frames in the journal
// ---------------------------------------------------------------------------

/// Scan the journal and return `(frame_start_offset, frame_end_offset)` for
/// every legacy commit frame (a legacy page frame whose `db_page_count` field
/// is non-zero), in journal order.
///
/// `frame_end_offset` is the byte offset of the first byte AFTER the frame's
/// page data — i.e. the start of the next frame.
///
/// Stops at the first byte offset that does not look like a valid frame,
/// mirroring the recovery-loop halt criterion.
///
/// Phase 0 / Phase 2 boundary: this harness shape is allowed in Phase 0, but
/// mixed legacy+ChainCommit correctness assertions are Phase 2's
/// responsibility.
///
/// # Errors
///
/// Returns `Err` on any I/O error or on a malformed journal header.
#[allow(dead_code)]
pub fn scan_legacy_commit_frames(db_path: &Path) -> std::io::Result<Vec<(u64, u64)>> {
    let jpath = journal_path(db_path);
    let mut file = OpenOptions::new().read(true).open(&jpath)?;

    let (salt1, salt2) = read_journal_salts(&mut file)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(JOURNAL_HEADER_SIZE))?;

    let mut out = Vec::new();
    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < file_len {
        // Disambiguate legacy vs Phase 2 frames via page_size_u32 at bytes
        // 16-19 (legacy stores 4096/32768 there; Phase 2 stores body bytes
        // unlikely to collide with a legal page size). This avoids the
        // byte-0 alias where legacy page_number ∈ {2, 3, 4} collides with
        // Phase 2 frame kinds.
        if cursor + JOURNAL_FRAME_HEADER_SIZE > file_len {
            break;
        }
        file.seek(SeekFrom::Start(cursor))?;
        let mut hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE as usize];
        match file.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let fs1 = u32::from_le_bytes(hdr[8..12].try_into().expect("4 bytes"));
        let fs2 = u32::from_le_bytes(hdr[12..16].try_into().expect("4 bytes"));
        if fs1 != salt1 || fs2 != salt2 {
            break;
        }
        let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"));
        let page_size = match page_size_u32 {
            4096 => Some(PAGE_SIZE_INTERNAL),
            32768 => Some(PAGE_SIZE_LEAF),
            _ => None,
        };
        if let Some(ps) = page_size {
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE;
            if data_offset + ps > file_len {
                break;
            }
            let frame_end = data_offset + ps;
            let db_page_count = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes"));
            if db_page_count != 0 {
                out.push((cursor, frame_end));
            }
            cursor = frame_end;
            continue;
        }
        let kind = hdr[0];
        let total_frame_bytes = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes")) as u64;
        match kind {
            FRAME_KIND_CHAIN_COMMIT => {
                if total_frame_bytes == 0
                    || total_frame_bytes > CHAIN_COMMIT_MAX_FRAME_SIZE
                    || cursor + total_frame_bytes > file_len
                {
                    break;
                }
                cursor += total_frame_bytes;
            }
            FRAME_KIND_LOGICAL_TXN => {
                if total_frame_bytes == 0
                    || total_frame_bytes > LOGICAL_TXN_MAX_FRAME_SIZE
                    || cursor + total_frame_bytes > file_len
                {
                    break;
                }
                cursor += total_frame_bytes;
            }
            FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY => {
                if total_frame_bytes != CHECKPOINT_COMMIT_BOUNDARY_TOTAL_BYTES
                    || cursor + total_frame_bytes > file_len
                {
                    break;
                }
                cursor += total_frame_bytes;
            }
            _ => break,
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// scan_all_frame_counts — count both frame kinds in the journal
// ---------------------------------------------------------------------------

/// Scan the journal and return `(legacy_page_frame_count,
/// chain_commit_frame_count)` — totals of every validly-framed frame found,
/// in journal order. Stops at the first byte offset that does not look like
/// a valid frame, mirroring [`scan_chain_commits`] and the recovery-loop
/// halt criterion.
///
/// Used by US-001 counter tests that must assert the two recovery counters
/// equal the journal's actual frame counts (not merely `> 0`).
///
/// # Errors
///
/// Returns `Err` on any I/O error or on a malformed journal header.
#[allow(dead_code)]
pub fn scan_all_frame_counts(db_path: &Path) -> std::io::Result<(u64, u64)> {
    let jpath = journal_path(db_path);
    let mut file = OpenOptions::new().read(true).open(&jpath)?;

    let (salt1, salt2) = read_journal_salts(&mut file)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(JOURNAL_HEADER_SIZE))?;

    let mut legacy = 0u64;
    let mut chain = 0u64;
    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < file_len {
        if cursor + JOURNAL_FRAME_HEADER_SIZE > file_len {
            break;
        }
        file.seek(SeekFrom::Start(cursor))?;
        let mut hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE as usize];
        match file.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        // Both legacy and Phase 2 frames place (salt1, salt2) at bytes 8-15.
        let hdr_s1 = u32::from_le_bytes(hdr[8..12].try_into().expect("4 bytes"));
        let hdr_s2 = u32::from_le_bytes(hdr[12..16].try_into().expect("4 bytes"));
        if hdr_s1 != salt1 || hdr_s2 != salt2 {
            break;
        }
        // Disambiguate legacy vs Phase 2 via the page_size field at bytes 16-19.
        // Legacy encodes 4096 or 32768; Phase 2 frames stash arbitrary body
        // bytes there which won't collide with a legal page size.
        let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"));
        let page_size_opt = match page_size_u32 {
            4096 => Some(PAGE_SIZE_INTERNAL),
            32768 => Some(PAGE_SIZE_LEAF),
            _ => None,
        };
        if let Some(page_size) = page_size_opt {
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE;
            if data_offset + page_size > file_len {
                break;
            }
            legacy += 1;
            cursor = data_offset + page_size;
            continue;
        }
        // Phase 2 frame.
        let kind = hdr[0];
        let total_frame_bytes = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes")) as u64;
        let max = match kind {
            FRAME_KIND_CHAIN_COMMIT => CHAIN_COMMIT_MAX_FRAME_SIZE,
            FRAME_KIND_LOGICAL_TXN => LOGICAL_TXN_MAX_FRAME_SIZE,
            _ => break,
        };
        if total_frame_bytes == 0
            || total_frame_bytes > max
            || cursor + total_frame_bytes > file_len
        {
            break;
        }
        if kind == FRAME_KIND_CHAIN_COMMIT {
            chain += 1;
        }
        // Logical frames are advanced past without tallying.
        cursor += total_frame_bytes;
    }

    Ok((legacy, chain))
}

// ---------------------------------------------------------------------------
// Private helpers: pure CRC32C computation using the crc32c crate via the
// mqlite re-export is not possible from integration tests, so we implement
// the same algorithm using the same crate that mqlite already depends on.
// crc32c is a dev-dep (it's in [dependencies] of the crate) so we can call
// it directly only if it re-exports from the public surface.
//
// Since crc32c is a *library dependency* (not dev-only) it is accessible
// from integration tests via the `extern crate` mechanism implicitly
// available when listed in [dependencies].  We invoke it directly below.
// ---------------------------------------------------------------------------

fn crc32c_compute(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

fn crc32c_append(prev: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(prev, data)
}

// ---------------------------------------------------------------------------
// Private: read salt values from the journal header
// ---------------------------------------------------------------------------

fn read_journal_salts(file: &mut File) -> std::io::Result<(u32, u32)> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 32];
    file.read_exact(&mut header)?;
    // salt1 is at offset 16, salt2 at offset 20.
    let salt1 = u32::from_le_bytes(header[16..20].try_into().expect("4 bytes"));
    let salt2 = u32::from_le_bytes(header[20..24].try_into().expect("4 bytes"));
    Ok((salt1, salt2))
}

// ---------------------------------------------------------------------------
// scan_logical_txn_first_op_id — extract the first op's `ns_id` / `index_id`
// from each LogicalTxnFrame in the journal.
//
// Used by US-021 `rename_safe_logical_frame_uses_stage_time_id` to prove
// the on-disk LogicalTxnFrame carries the STAGE-TIME id (an i64 sourced
// from `CollectionEntry.id` at the moment the write was staged) regardless
// of subsequent catalog mutations.
//
// The §4.1 frame layout places ops at offset 48 (after the 48-byte fixed
// header). Each op's shared 8-byte prefix is `op_kind(1) + reserved(3) +
// op_ordinal(4)`. PrimaryInsert (0x01) / PrimaryUpdate (0x02) /
// PrimaryDelete (0x03) bodies start with the 8-byte `ns_id` (i64 LE).
// SecondaryInsert (0x11) / SecondaryDelete (0x12) bodies start with the
// 8-byte `index_id` (i64 LE). Either way the first 8 bytes of the op's
// body — at frame_offset + 48 + 8 = frame_offset + 56 — is the i64 we
// want.
// ---------------------------------------------------------------------------

const LOGICAL_TXN_FIXED_HEADER_LEN: u64 = 48;
const LOGICAL_OP_PREFIX_LEN: u64 = 8;

/// Scan the journal and return the i64 `ns_id` / `index_id` of the FIRST
/// op of every validly-framed `LogicalTxnFrame`, in journal order.
///
/// The op kind is also returned (`0x01`/`0x02`/`0x03` for primary
/// insert/update/delete; `0x11`/`0x12` for secondary insert/delete) so
/// the caller can distinguish primary vs secondary writes if needed.
///
/// Stops at the first byte offset that does not look like a valid frame,
/// mirroring the recovery-loop halt criterion. Empty `Vec` is returned
/// when the journal carries no logical frames.
///
/// # Errors
///
/// Returns `Err` on any I/O error or on a malformed journal header.
#[allow(dead_code)]
pub fn scan_logical_txn_first_op_id(db_path: &Path) -> std::io::Result<Vec<(u64, u8, i64)>> {
    let jpath = journal_path(db_path);
    let mut file = OpenOptions::new().read(true).open(&jpath)?;

    let (salt1, salt2) = read_journal_salts(&mut file)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(JOURNAL_HEADER_SIZE))?;

    let mut out = Vec::new();
    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < file_len {
        if cursor + JOURNAL_FRAME_HEADER_SIZE > file_len {
            break;
        }
        file.seek(SeekFrom::Start(cursor))?;
        let mut hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE as usize];
        match file.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let fs1 = u32::from_le_bytes(hdr[8..12].try_into().expect("4 bytes"));
        let fs2 = u32::from_le_bytes(hdr[12..16].try_into().expect("4 bytes"));
        if fs1 != salt1 || fs2 != salt2 {
            break;
        }
        // Disambiguate legacy via page_size at bytes 16-19.
        let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"));
        if matches!(page_size_u32, 4096 | 32768) {
            let page_size = u64::from(page_size_u32);
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE;
            if data_offset + page_size > file_len {
                break;
            }
            cursor = data_offset + page_size;
            continue;
        }
        let kind = hdr[0];
        let total_frame_bytes = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes")) as u64;
        let max = match kind {
            FRAME_KIND_CHAIN_COMMIT => CHAIN_COMMIT_MAX_FRAME_SIZE,
            FRAME_KIND_LOGICAL_TXN => LOGICAL_TXN_MAX_FRAME_SIZE,
            FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY => CHECKPOINT_COMMIT_BOUNDARY_TOTAL_BYTES,
            _ => break,
        };
        if total_frame_bytes == 0
            || total_frame_bytes > max
            || cursor + total_frame_bytes > file_len
        {
            break;
        }
        if kind == FRAME_KIND_LOGICAL_TXN {
            // Need at least: fixed header (48) + first op header (8) + i64 (8).
            let id_offset = cursor + LOGICAL_TXN_FIXED_HEADER_LEN + LOGICAL_OP_PREFIX_LEN;
            if id_offset + 8 > cursor + total_frame_bytes {
                // Unlikely: well-formed empty-ops frame OR malformed frame.
                cursor += total_frame_bytes;
                continue;
            }
            file.seek(SeekFrom::Start(cursor + LOGICAL_TXN_FIXED_HEADER_LEN))?;
            let mut op_prefix = [0u8; 8];
            if file.read_exact(&mut op_prefix).is_err() {
                break;
            }
            let op_kind = op_prefix[0];
            let mut id_bytes = [0u8; 8];
            if file.read_exact(&mut id_bytes).is_err() {
                break;
            }
            let id = i64::from_le_bytes(id_bytes);
            out.push((cursor, op_kind, id));
        }
        cursor += total_frame_bytes;
    }
    Ok(out)
}
