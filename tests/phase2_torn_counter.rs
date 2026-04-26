#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Phase 2 §7 / US-024 — `logical_txn_torn_frames_total` counter ticks
//! when the recovery scan encounters a torn `LogicalTxnFrame` (structural
//! signature matches but the body is truncated or fails CRC).
//!
//! The intrusive byte-level truncation needed to construct a torn frame
//! lives in this dedicated test file, separate from the production code
//! and its `#[cfg(test)]` unit modules.

#[path = "crash_harness.rs"]
mod crash_harness;

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Mutex;

use bson::doc;
use bson::Document;
use mqlite::mvcc::metrics::{logical_txn_torn_frames_snapshot, reset_logical_txn_torn_frames};
use mqlite::{Client, DurabilityMode, OpenOptions as MqOpenOptions};
use tempfile::TempDir;

/// Serialize the (reset → tick → snapshot) window so other parallel tests
/// that touch the same global counter cannot race with this assertion.
static TORN_COUNTER_LOCK: Mutex<()> = Mutex::new(());

const FRAME_KIND_LOGICAL_TXN: u8 = 0x03;
const PAGE_SIZE_INTERNAL_U32: u32 = 4096;
const PAGE_SIZE_LEAF_U32: u32 = 32768;
const JOURNAL_FRAME_HEADER_SIZE: u64 = 24;
const LOGICAL_TXN_FIXED_HEADER_LEN: u64 = 48;

/// Walk the journal from `start_offset` and return `(offset, total_bytes)`
/// for the first `LogicalTxnFrame` found. Disambiguates legacy frames whose
/// `page_number` LSB collides with `FRAME_KIND_LOGICAL_TXN` (=0x03) by
/// inspecting the `page_size` u32 at bytes 16-19 — same approach as the
/// production-side disambiguation in `tests/crash_harness.rs`.
fn find_first_logical_txn_frame(
    journal_path: &std::path::Path,
    start_offset: u64,
) -> Option<(u64, u64)> {
    let mut f = OpenOptions::new().read(true).open(journal_path).ok()?;
    let total_len = f.seek(SeekFrom::End(0)).ok()?;
    let mut cursor = start_offset;
    while cursor + JOURNAL_FRAME_HEADER_SIZE <= total_len {
        f.seek(SeekFrom::Start(cursor)).ok()?;
        let mut hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE as usize];
        if f.read_exact(&mut hdr).is_err() {
            return None;
        }
        let page_size_u32 = u32::from_le_bytes(hdr[16..20].try_into().ok()?);
        let legacy_size = match page_size_u32 {
            PAGE_SIZE_INTERNAL_U32 => Some(PAGE_SIZE_INTERNAL_U32 as u64),
            PAGE_SIZE_LEAF_U32 => Some(PAGE_SIZE_LEAF_U32 as u64),
            _ => None,
        };
        if let Some(ps) = legacy_size {
            cursor += JOURNAL_FRAME_HEADER_SIZE + ps;
            continue;
        }
        if hdr[0] == FRAME_KIND_LOGICAL_TXN {
            let total = u32::from_le_bytes(hdr[4..8].try_into().ok()?) as u64;
            if total >= LOGICAL_TXN_FIXED_HEADER_LEN + 4 && cursor + total <= total_len {
                return Some((cursor, total));
            }
        }
        let total = u32::from_le_bytes(hdr[4..8].try_into().ok()?) as u64;
        if total == 0 {
            return None;
        }
        cursor += total;
    }
    None
}

#[test]
fn logical_txn_torn_frames_total_ticks_on_torn_recovery() {
    let _guard = TORN_COUNTER_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("torn.mqlite");

    let baseline_journal_len = {
        let client = Client::open_with_options(
            &db_path,
            MqOpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .expect("open");
        client
            .database("db")
            .create_collection("c")
            .expect("create");
        client.checkpoint().expect("checkpoint baseline");
        std::fs::metadata(crash_harness::journal_path(&db_path))
            .map(|m| m.len())
            .unwrap_or(32)
    };

    {
        let client = Client::open_with_options(
            &db_path,
            MqOpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .expect("reopen");
        let col = client.database("db").collection::<Document>("c");
        col.insert_one(&doc! { "_id": 1, "v": "torn-target" })
            .expect("insert");
        std::mem::forget(client);
    }

    let journal_path = crash_harness::journal_path(&db_path);
    let (logical_offset, logical_total) =
        find_first_logical_txn_frame(&journal_path, baseline_journal_len)
            .expect("logical frame must be present after one insert");

    let truncate_to = logical_offset + LOGICAL_TXN_FIXED_HEADER_LEN + 1;
    assert!(
        truncate_to < logical_offset + logical_total,
        "truncate_to must cut inside the logical frame body"
    );

    {
        let f = OpenOptions::new()
            .write(true)
            .open(&journal_path)
            .expect("open journal for truncate");
        f.set_len(truncate_to).expect("set_len");
        f.sync_all().expect("sync_all");
    }

    reset_logical_txn_torn_frames();

    let _client = Client::open_with_options(
        &db_path,
        MqOpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("reopen after torn truncation");

    let torn = logical_txn_torn_frames_snapshot();
    assert!(
        torn >= 1,
        "logical_txn_torn_frames_total must tick on torn LogicalTxnFrame; got {torn}"
    );
}

#[test]
fn logical_txn_torn_frames_total_zero_on_clean_recovery() {
    let _guard = TORN_COUNTER_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    let dir = TempDir::new().expect("tempdir");
    let db_path = dir.path().join("clean.mqlite");

    {
        let client = Client::open_with_options(
            &db_path,
            MqOpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .expect("open");
        client
            .database("db")
            .create_collection("c")
            .expect("create");
        let col = client.database("db").collection::<Document>("c");
        col.insert_one(&doc! { "_id": 1, "v": "clean" })
            .expect("insert");
    }

    reset_logical_txn_torn_frames();

    let _client = Client::open_with_options(
        &db_path,
        MqOpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("reopen clean");

    let torn = logical_txn_torn_frames_snapshot();
    assert_eq!(
        torn, 0,
        "torn counter must remain 0 on a clean reopen; got {torn}"
    );
    let _ = std::io::stdout().flush();
}
