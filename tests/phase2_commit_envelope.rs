#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Phase 2 US-011 integration — assert the commit-envelope frame sequence.
//!
//! Per §3.7 and §6.2, one successful CRUD commit must grow the journal by
//! exactly this ordered sequence:
//!
//!   1. One `LogicalTxnFrame` (frame_kind 0x03) — emitted at S5.
//!   2. One `ChainCommit` frame (frame_kind 0x02) — emitted at S7 by
//!      `WriteTxn::commit_with_ts`.
//!   3. One legacy page-0 commit frame (`db_page_count > 0`) — emitted by
//!      `handle.commit_txn` immediately after ChainCommit.
//!
//! This test drives one CRUD `insert_one` through the public client API,
//! uses `std::mem::forget` to keep the journal on disk past the Drop
//! checkpoint, and reads back the journal bytes to verify the exact
//! frame-kind sequence. Intentionally lives outside `src/` because the
//! read-back touches raw journal bytes (test-only scaffolding).

#![allow(clippy::expect_used)]

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions as DbOpts};
use tempfile::TempDir;

#[path = "crash_harness.rs"]
mod crash_harness;

const JOURNAL_HEADER_SIZE: u64 = 32;
const JOURNAL_FRAME_HEADER_SIZE: u64 = 24;
const FRAME_KIND_CHAIN_COMMIT: u8 = 0x02;
const FRAME_KIND_LOGICAL_TXN: u8 = 0x03;
const PAGE_SIZE_INTERNAL: u64 = 4096;
const PAGE_SIZE_LEAF: u64 = 32768;
const CHAIN_COMMIT_MAX_FRAME_SIZE: u64 = 64 * 1024 * 1024;
const LOGICAL_TXN_MAX_FRAME_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameKind {
    Logical,
    ChainCommit,
    LegacyNonCommit,
    LegacyCommit,
}

/// Walk the journal file forward from the 32-byte header and return every
/// frame kind in order. Stops at the first byte offset that does not parse
/// as a valid frame (same halt criterion as recovery).
fn scan_frame_kinds(db_path: &Path) -> Vec<FrameKind> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(crash_harness::journal_path(db_path))
        .expect("open journal");

    let (salt1, salt2) = {
        let mut hdr = [0u8; JOURNAL_HEADER_SIZE as usize];
        file.seek(SeekFrom::Start(0)).expect("seek header");
        file.read_exact(&mut hdr).expect("read header");
        let s1 = u32::from_le_bytes(hdr[16..20].try_into().expect("4 bytes"));
        let s2 = u32::from_le_bytes(hdr[20..24].try_into().expect("4 bytes"));
        (s1, s2)
    };

    let file_len = file.seek(SeekFrom::End(0)).expect("seek end");
    let mut out = Vec::new();
    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < file_len {
        // CRC-driven disambiguation: try Phase 2 first, then legacy.
        // Whichever interpretation's CRC32C checksum validates is
        // definitively that frame kind. This closes the physical_ms-
        // low-32-bits collision hole codex raised in round 3.
        match try_read_phase2_frame(&mut file, cursor, file_len, salt1, salt2) {
            Some((kind, advance)) => {
                out.push(kind);
                cursor += advance;
                continue;
            }
            None => {}
        }
        match try_read_legacy_frame(&mut file, cursor, file_len, salt1, salt2) {
            Some((kind, advance)) => {
                out.push(kind);
                cursor += advance;
                continue;
            }
            None => {}
        }
        break;
    }
    out
}

/// Attempt to read a Phase 2 (ChainCommit or LogicalTxn) frame at
/// `cursor`, verifying the CRC32C trailer. Returns `Some((kind, bytes_to_advance))`
/// on success, `None` if the frame doesn't validate at this offset.
fn try_read_phase2_frame(
    file: &mut std::fs::File,
    cursor: u64,
    file_len: u64,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Option<(FrameKind, u64)> {
    if cursor + 8 > file_len {
        return None;
    }
    file.seek(SeekFrom::Start(cursor)).ok()?;
    let mut prefix = [0u8; 8];
    file.read_exact(&mut prefix).ok()?;
    let kind = prefix[0];
    let (frame_kind, max) = match kind {
        FRAME_KIND_CHAIN_COMMIT => (FrameKind::ChainCommit, CHAIN_COMMIT_MAX_FRAME_SIZE),
        FRAME_KIND_LOGICAL_TXN => (FrameKind::Logical, LOGICAL_TXN_MAX_FRAME_SIZE),
        _ => return None,
    };
    let total = u32::from_le_bytes(prefix[4..8].try_into().ok()?) as u64;
    if total < 12 || total > max || cursor + total > file_len {
        return None;
    }
    // Read the full frame and verify CRC32C over bytes[..total-4] equals bytes[total-4..total].
    file.seek(SeekFrom::Start(cursor)).ok()?;
    let mut buf = vec![0u8; total as usize];
    file.read_exact(&mut buf).ok()?;
    // Validate salts stored at bytes 8-15 of the frame.
    let fs1 = u32::from_le_bytes(buf[8..12].try_into().ok()?);
    let fs2 = u32::from_le_bytes(buf[12..16].try_into().ok()?);
    if fs1 != expected_salt1 || fs2 != expected_salt2 {
        return None;
    }
    let body_end = (total as usize) - 4;
    let stored_crc = u32::from_le_bytes(buf[body_end..body_end + 4].try_into().ok()?);
    let computed_crc = crc32c::crc32c(&buf[..body_end]);
    if stored_crc != computed_crc {
        return None;
    }
    Some((frame_kind, total))
}

/// Attempt to read a legacy page frame at `cursor`, verifying the
/// CRC32C trailer. Returns `Some((kind, bytes_to_advance))` on success,
/// `None` if the frame doesn't validate at this offset.
fn try_read_legacy_frame(
    file: &mut std::fs::File,
    cursor: u64,
    file_len: u64,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Option<(FrameKind, u64)> {
    if cursor + JOURNAL_FRAME_HEADER_SIZE > file_len {
        return None;
    }
    file.seek(SeekFrom::Start(cursor)).ok()?;
    let mut hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE as usize];
    file.read_exact(&mut hdr).ok()?;
    let fs1 = u32::from_le_bytes(hdr[8..12].try_into().ok()?);
    let fs2 = u32::from_le_bytes(hdr[12..16].try_into().ok()?);
    if fs1 != expected_salt1 || fs2 != expected_salt2 {
        return None;
    }
    let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().ok()?);
    let page_size = match page_size_u32 {
        4096 => PAGE_SIZE_INTERNAL,
        32768 => PAGE_SIZE_LEAF,
        _ => return None,
    };
    let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE;
    if data_offset + page_size > file_len {
        return None;
    }
    // Verify CRC: crc32c over bytes[0..20] + page_data, stored at bytes[20..24].
    let stored_crc = u32::from_le_bytes(hdr[20..24].try_into().ok()?);
    let mut page_data = vec![0u8; page_size as usize];
    file.seek(SeekFrom::Start(data_offset)).ok()?;
    file.read_exact(&mut page_data).ok()?;
    let mut computed_crc = crc32c::crc32c(&hdr[..20]);
    computed_crc = crc32c::crc32c_append(computed_crc, &page_data);
    if stored_crc != computed_crc {
        return None;
    }
    let db_page_count = u32::from_le_bytes(hdr[4..8].try_into().ok()?);
    let kind = if db_page_count == 0 {
        FrameKind::LegacyNonCommit
    } else {
        FrameKind::LegacyCommit
    };
    Some((kind, JOURNAL_FRAME_HEADER_SIZE + page_size))
}

/// Per §3.7/§6.2: one successful CRUD commit grows the journal by exactly
/// `...[Logical][ChainCommit][LegacyCommit]`. Non-commit legacy page frames
/// may precede the logical frame (dirty pages flushed during the body),
/// and may also appear between the logical frame and the ChainCommit (dirty
/// pages flushed during the commit-envelope's `handle.flush`). The test
/// asserts the ORDER of the three key frames, not a strict three-frame-total
/// count.
#[test]
fn commit_envelope_emits_logical_between_allocate_and_chain_commit() {
    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("env.mqlite");

    {
        let client =
            Client::open_with_options(&db_path, DbOpts::new().durability(DurabilityMode::FullSync))
                .expect("open client");
        let col = client.database("envdb").collection::<Document>("people");
        col.insert_one(&doc! { "_id": 1i32, "name": "alice" })
            .expect("insert");
        // Forget rather than drop so the journal survives to disk for the
        // read-back scan below.
        std::mem::forget(client);
    }

    let kinds = scan_frame_kinds(&db_path);
    assert!(
        !kinds.is_empty(),
        "expected at least one journal frame after one CRUD commit"
    );

    let logical_idx = kinds
        .iter()
        .position(|k| *k == FrameKind::Logical)
        .expect("expected exactly one Logical frame in the journal");
    let chain_idx = kinds
        .iter()
        .position(|k| *k == FrameKind::ChainCommit)
        .expect("expected at least one ChainCommit frame in the journal");
    let commit_idx = kinds
        .iter()
        .skip(chain_idx)
        .position(|k| *k == FrameKind::LegacyCommit)
        .map(|off| chain_idx + off)
        .expect("expected a legacy commit frame after the ChainCommit");

    assert!(
        logical_idx < chain_idx,
        "§3.7 S5 → S7 ordering violated: Logical frame at {logical_idx} must precede ChainCommit at {chain_idx}; kinds={kinds:?}"
    );
    assert!(
        chain_idx < commit_idx,
        "ChainCommit at {chain_idx} must precede legacy commit frame at {commit_idx}; kinds={kinds:?}"
    );

    // Per US-011 AC#4 / AC#6: exactly one logical frame and exactly one
    // ChainCommit per CRUD commit (§3.7 "one logical frame per successful
    // write-txn commit"). Because insert_one on a fresh DB auto-creates
    // the collection (a metadata-only commit that emits one legacy commit
    // frame but NO logical frame), the journal may contain additional
    // legacy commit frames from that DDL step. The CRUD-specific
    // invariant we assert is:
    //   - exactly ONE Logical frame (the insert_one commit)
    //   - exactly ONE ChainCommit frame (paired with the Logical frame)
    //   - AT LEAST ONE LegacyCommit frame AFTER the ChainCommit
    let logical_count = kinds.iter().filter(|k| **k == FrameKind::Logical).count();
    let chain_count = kinds
        .iter()
        .filter(|k| **k == FrameKind::ChainCommit)
        .count();
    let commit_count_after_chain = kinds[chain_idx..]
        .iter()
        .filter(|k| **k == FrameKind::LegacyCommit)
        .count();
    assert_eq!(
        logical_count, 1,
        "expected exactly one Logical frame per CRUD commit; kinds={kinds:?}"
    );
    assert_eq!(
        chain_count, 1,
        "expected exactly one ChainCommit frame per CRUD commit; kinds={kinds:?}"
    );
    assert!(
        commit_count_after_chain >= 1,
        "expected at least one legacy commit frame after the ChainCommit; kinds={kinds:?}"
    );
}
