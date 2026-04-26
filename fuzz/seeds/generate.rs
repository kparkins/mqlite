//! Phase 2 §9.2 / US-023 seed-corpus generator.
//!
//! Run via:
//! ```text
//! cd fuzz && cargo run --bin generate_seeds
//! ```
//!
//! Writes the twelve §9.2 named seeds to `corpus/logical_txn_recovery/`.
//! Reproducible — every seed is a deterministic byte sequence; no RNG.

use std::path::PathBuf;

const CORPUS_DIR: &str = "corpus/logical_txn_recovery";

const SALT1: u32 = 0xDEAD_BEEF;
const SALT2: u32 = 0xCAFE_BABE;
const OP_KIND_PRIMARY_DELETE: u8 = 0x03;
const UNRESOLVED_NS_ID: i64 = i64::MAX;

/// Minimal journal-header bytes (32 B) — magic + version + page sizes +
/// salts + checkpoint_seq + CRC. Reused as the prefix of every seed.
fn journal_header() -> Vec<u8> {
    let mut buf = vec![0u8; 32];
    buf[0..4].copy_from_slice(b"mwjl");
    buf[4..8].copy_from_slice(&1u32.to_le_bytes());
    buf[8..12].copy_from_slice(&4096u32.to_le_bytes());
    buf[12..16].copy_from_slice(&32768u32.to_le_bytes());
    buf[16..20].copy_from_slice(&SALT1.to_le_bytes());
    buf[20..24].copy_from_slice(&SALT2.to_le_bytes());
    buf[24..28].copy_from_slice(&0u32.to_le_bytes()); // checkpoint_seq
    let crc = crc32c::crc32c(&buf[..28]);
    buf[28..32].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Synthesize a legacy commit frame for `page_number`, `db_page_count`.
/// Uses 4 KB internal page size with `fill_byte` body.
fn legacy_frame(page_number: u32, db_page_count: u32, fill_byte: u8) -> Vec<u8> {
    let mut hdr = [0u8; 24];
    hdr[0..4].copy_from_slice(&page_number.to_le_bytes());
    hdr[4..8].copy_from_slice(&db_page_count.to_le_bytes());
    hdr[8..12].copy_from_slice(&SALT1.to_le_bytes());
    hdr[12..16].copy_from_slice(&SALT2.to_le_bytes());
    hdr[16..20].copy_from_slice(&4096u32.to_le_bytes());
    let payload = vec![fill_byte; 4096];
    let mut crc = crc32c::crc32c(&hdr[..20]);
    crc = crc32c::crc32c_append(crc, &payload);
    hdr[20..24].copy_from_slice(&crc.to_le_bytes());
    let mut out = Vec::with_capacity(24 + 4096);
    out.extend_from_slice(&hdr);
    out.extend_from_slice(&payload);
    out
}

/// ChainCommit frame body — minimal (no page_writes, no refcount_deltas).
fn chain_commit_frame(physical_ms: u64, logical: u32) -> Vec<u8> {
    // Layout: kind(1) reserved(3) total_len(4) salt1(4) salt2(4)
    // commit_ts(12) write_count(4) ref_delta_count(4) ... CRC(4)
    let total: u32 = 40; // header(32) + write_count(4) + ref_delta_count(0) + CRC(4)
    let mut buf = Vec::with_capacity(total as usize);
    buf.push(0x02); // kind = ChainCommit
    buf.extend_from_slice(&[0u8; 3]); // reserved
    buf.extend_from_slice(&total.to_le_bytes());
    buf.extend_from_slice(&SALT1.to_le_bytes());
    buf.extend_from_slice(&SALT2.to_le_bytes());
    buf.extend_from_slice(&physical_ms.to_le_bytes());
    buf.extend_from_slice(&logical.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // write_count
    buf.extend_from_slice(&0u32.to_le_bytes()); // ref_delta_count (4 B reserved)
    let body_end = buf.len();
    let crc = crc32c::crc32c(&buf[..body_end]);
    buf.extend_from_slice(&crc.to_le_bytes());
    debug_assert_eq!(buf.len(), total as usize);
    buf
}

/// LogicalTxnFrame with zero ops — the smallest valid logical frame.
fn logical_txn_empty(physical_ms: u64, logical: u32) -> Vec<u8> {
    // Fixed header is 48 bytes per §4 layout, plus 4-byte CRC trailer.
    // Body fields: kind(1) res(3) total(4) salt1(4) salt2(4) commit_ts(12)
    // diag_txn_id(8) format_ver(2) flags(2) op_count(4) reserved_b(4)
    let total: u32 = 52;
    let mut buf = Vec::with_capacity(total as usize);
    buf.push(0x03); // kind = LogicalTxn
    buf.extend_from_slice(&[0u8; 3]);
    buf.extend_from_slice(&total.to_le_bytes());
    buf.extend_from_slice(&SALT1.to_le_bytes());
    buf.extend_from_slice(&SALT2.to_le_bytes());
    buf.extend_from_slice(&physical_ms.to_le_bytes());
    buf.extend_from_slice(&logical.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // diagnostic_txn_id
    buf.extend_from_slice(&1u16.to_le_bytes()); // format_version
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    buf.extend_from_slice(&0u32.to_le_bytes()); // op_count
    buf.extend_from_slice(&[0u8; 4]); // reserved_b
    let body_end = buf.len();
    let crc = crc32c::crc32c(&buf[..body_end]);
    buf.extend_from_slice(&crc.to_le_bytes());
    debug_assert_eq!(buf.len(), total as usize);
    buf
}

/// LogicalTxnFrame with a single PrimaryDelete op targeting an absent ns_id.
fn logical_txn_primary_delete(physical_ms: u64, logical: u32, ns_id: i64, key: &[u8]) -> Vec<u8> {
    let op_len = 8 + 8 + 4 + key.len();
    let total = (48 + op_len + 4) as u32;
    let mut buf = Vec::with_capacity(total as usize);
    buf.push(0x03); // kind = LogicalTxn
    buf.extend_from_slice(&[0u8; 3]);
    buf.extend_from_slice(&total.to_le_bytes());
    buf.extend_from_slice(&SALT1.to_le_bytes());
    buf.extend_from_slice(&SALT2.to_le_bytes());
    buf.extend_from_slice(&physical_ms.to_le_bytes());
    buf.extend_from_slice(&logical.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // diagnostic_txn_id
    buf.extend_from_slice(&1u16.to_le_bytes()); // format_version
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    buf.extend_from_slice(&1u32.to_le_bytes()); // op_count
    buf.extend_from_slice(&[0u8; 4]); // reserved_b

    buf.push(OP_KIND_PRIMARY_DELETE);
    buf.extend_from_slice(&[0u8; 3]); // op reserved
    buf.extend_from_slice(&0u32.to_le_bytes()); // op_ordinal
    buf.extend_from_slice(&ns_id.to_le_bytes());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);

    let body_end = buf.len();
    let crc = crc32c::crc32c(&buf[..body_end]);
    buf.extend_from_slice(&crc.to_le_bytes());
    debug_assert_eq!(buf.len(), total as usize);
    buf
}

fn write_seed(name: &str, body: &[u8]) {
    let mut path = PathBuf::from(CORPUS_DIR);
    path.push(name);
    if let Some(p) = path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    std::fs::write(&path, body).expect("write seed");
    println!("wrote {} ({} bytes)", path.display(), body.len());
}

fn main() {
    let header = journal_header();

    // 1. empty_journal.bin — header only.
    write_seed("empty_journal.bin", &header);

    // 2. single_legacy_commit.bin — header + one legacy commit frame.
    let mut s2 = header.clone();
    s2.extend_from_slice(&legacy_frame(1, 5, 0xAA));
    write_seed("single_legacy_commit.bin", &s2);

    // 3. legacy_plus_chaincommit.bin — header + legacy commit + ChainCommit.
    let mut s3 = header.clone();
    s3.extend_from_slice(&legacy_frame(1, 5, 0x55));
    s3.extend_from_slice(&chain_commit_frame(100, 0));
    write_seed("legacy_plus_chaincommit.bin", &s3);

    // 4. legacy_chaincommit_logical.bin — full envelope.
    let mut s4 = header.clone();
    s4.extend_from_slice(&legacy_frame(1, 5, 0x33));
    s4.extend_from_slice(&logical_txn_empty(200, 0));
    s4.extend_from_slice(&chain_commit_frame(200, 0));
    write_seed("legacy_chaincommit_logical.bin", &s4);

    // 5. torn_logical_tail.bin — valid header + truncated logical frame.
    let mut s5 = header.clone();
    let logical = logical_txn_empty(300, 0);
    s5.extend_from_slice(&logical[..logical.len() / 2]);
    write_seed("torn_logical_tail.bin", &s5);

    // 6. orphan_logical.bin — logical frame without a matching ChainCommit.
    let mut s6 = header.clone();
    s6.extend_from_slice(&logical_txn_empty(400, 0));
    write_seed("orphan_logical.bin", &s6);

    // 7. duplicate_commit_ts_logical.bin — two logical frames with the
    //    same commit_ts, both followed by ChainCommit.
    let mut s7 = header.clone();
    s7.extend_from_slice(&logical_txn_empty(500, 0));
    s7.extend_from_slice(&chain_commit_frame(500, 0));
    s7.extend_from_slice(&logical_txn_empty(500, 0));
    s7.extend_from_slice(&chain_commit_frame(500, 0));
    write_seed("duplicate_commit_ts_logical.bin", &s7);

    // 8. unknown_op_kind.bin — corrupt op-kind byte inside a logical body.
    //    We synthesize a logical frame with op_count=1 then poison the
    //    op-kind byte. Keep it short — just enough to fail decode.
    let mut s8 = header.clone();
    let mut body = vec![0u8; 60];
    body[0] = 0x03; // logical kind
    body[4..8].copy_from_slice(&60u32.to_le_bytes());
    body[8..12].copy_from_slice(&SALT1.to_le_bytes());
    body[12..16].copy_from_slice(&SALT2.to_le_bytes());
    body[40..44].copy_from_slice(&1u32.to_le_bytes()); // op_count = 1
    body[48] = 0xFF; // unknown op kind
    let crc = crc32c::crc32c(&body[..56]);
    body[56..60].copy_from_slice(&crc.to_le_bytes());
    s8.extend_from_slice(&body);
    write_seed("unknown_op_kind.bin", &s8);

    // 9. oversized_op_count.bin — op_count past the §4 cap.
    let mut s9 = header.clone();
    let mut body = vec![0u8; 52];
    body[0] = 0x03;
    body[4..8].copy_from_slice(&52u32.to_le_bytes());
    body[8..12].copy_from_slice(&SALT1.to_le_bytes());
    body[12..16].copy_from_slice(&SALT2.to_le_bytes());
    body[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
    let crc = crc32c::crc32c(&body[..48]);
    body[48..52].copy_from_slice(&crc.to_le_bytes());
    s9.extend_from_slice(&body);
    write_seed("oversized_op_count.bin", &s9);

    // 10. mixed_sequence_long.bin — legacy + logical + CC + legacy + CC.
    let mut s10 = header.clone();
    for i in 0..3u32 {
        s10.extend_from_slice(&legacy_frame(10 + i, 0, (0x10 * i) as u8));
    }
    s10.extend_from_slice(&logical_txn_empty(600, 0));
    s10.extend_from_slice(&chain_commit_frame(600, 0));
    s10.extend_from_slice(&legacy_frame(20, 25, 0x77));
    s10.extend_from_slice(&chain_commit_frame(700, 0));
    write_seed("mixed_sequence_long.bin", &s10);

    // 11. gap_ordinal.bin — single op with ordinal != 0 (non-dense).
    let mut s11 = header.clone();
    let mut body = vec![0u8; 60];
    body[0] = 0x03;
    body[4..8].copy_from_slice(&60u32.to_le_bytes());
    body[8..12].copy_from_slice(&SALT1.to_le_bytes());
    body[12..16].copy_from_slice(&SALT2.to_le_bytes());
    body[40..44].copy_from_slice(&1u32.to_le_bytes());
    body[48] = 0x01; // some op kind byte
    body[52..56].copy_from_slice(&7u32.to_le_bytes()); // ordinal = 7 (gap)
    let crc = crc32c::crc32c(&body[..56]);
    body[56..60].copy_from_slice(&crc.to_le_bytes());
    s11.extend_from_slice(&body);
    write_seed("gap_ordinal.bin", &s11);

    // 12. unresolved_ns_id.bin — logical op references an ns_id absent
    //     from the catalog. We emit a minimal logical frame with a
    //     single PrimaryDelete op pointing at ns_id = i64::MAX.
    //     Pass 2 must log + tick the unresolved counter without
    //     failing the open.
    let mut s12 = header.clone();
    s12.extend_from_slice(&logical_txn_primary_delete(
        800,
        0,
        UNRESOLVED_NS_ID,
        b"missing",
    ));
    s12.extend_from_slice(&chain_commit_frame(800, 0));
    write_seed("unresolved_ns_id.bin", &s12);

    println!("\nGenerated 12 seeds in {CORPUS_DIR}/");
}
