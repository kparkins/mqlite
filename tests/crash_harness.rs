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

//! Crash-cut harness: journal-tail truncation and reopen-after-crash
//! inspection helpers for the current write envelope.
//!
//! Shared test-support module imported via `#[path = ...]` by several
//! integration tests. Keep this file as narrow test plumbing; phase-specific
//! journal assertions belong in the tests that need them.
#![allow(
    dead_code,
    reason = "each integration target imports a different subset of this shared harness"
)]
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
//! [Typed Log Record]
//!   current Phase 8 records are parsed by production recovery; this helper
//!   only scans legacy frames for the older assertions that still need that
//!   distinction.
//! ```

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use mqlite::{Client, DurabilityMode, OpenOptions as DbOpenOptions, Result};

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
// RecoveryReport — output of reopen_inspect
// ---------------------------------------------------------------------------

/// Recovery statistics captured by [`reopen_inspect`] for one reopen.
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// Number of legacy page-replay frames (`JournalFrameHeader`) processed
    /// by the recovery loop, sourced from
    /// `mvcc::metrics::recovery_legacy_page_frames_snapshot()`.
    pub legacy_page_frame_count: u64,
    /// Number of `ChainCommit` payload frames processed by recovery.
    pub chain_commit_frame_count: u64,
    /// Highest commit timestamp recovered from durable journal state.
    pub recovered_max_commit_ts: Option<(u64, u32)>,
}

// ---------------------------------------------------------------------------
// Helper: derive journal path from db path
// ---------------------------------------------------------------------------

/// Return the path of the journal file for a given database file path.
///
/// The journal path is `<db_path>-journal`, matching the convention in
/// `src/journal/mod.rs:journal_path_for`.
#[must_use]
pub fn journal_path(db_path: &Path) -> std::path::PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-journal");
    std::path::PathBuf::from(s)
}

/// Return FullSync open options for crash/recovery integration tests.
#[must_use]
pub fn fullsync_options() -> DbOpenOptions {
    DbOpenOptions::new().durability(DurabilityMode::FullSync)
}

/// Open a database with FullSync durability.
pub fn open_fullsync(db_path: &Path) -> Result<Client> {
    Client::open_with_options(db_path, fullsync_options())
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
/// a valid frame, mirroring the recovery-loop halt criterion.
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
