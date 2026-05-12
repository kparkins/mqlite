//! Cross-platform file format compatibility tool.
//!
//! Validates that the `.mqlite` file format is byte-identical across CPU
//! architectures (x86_64 <-> aarch64). This binary writes 100 BSON documents
//! with deterministic content into a minimal `.mqlite`-structured file, then
//! reads and verifies them.
//!
//! The test exercises the two areas most likely to produce cross-platform
//! byte-order problems:
//!
//! 1. **File header** - all multi-byte numeric fields must be little-endian.
//! 2. **BSON payloads** - the BSON specification mandates little-endian
//!    throughout, so this acts as a regression guard on BSON encoding.
//!
//! # File layout
//!
//! ```text
//! Offset 0        : File header page (4 096 bytes, .mqlite format)
//! Offset 4 096    : Document count (4 bytes, u32 LE)
//! Offset 4 100    : Document 0 - length (4 bytes, u32 LE) + BSON bytes
//!                 : Document 1 - length (4 bytes, u32 LE) + BSON bytes
//!                 : ...
//! ```
//!
//! # Usage
//!
//! ```text
//! format_compat write <path>   - write 100 known documents to <path>
//! format_compat read  <path>   - open <path> and verify the 100 documents
//! ```
//!
//! Both commands exit with code 0 on success, non-zero on any failure.
//!
//! # CI cross-platform test
//!
//! The GitHub Actions `cross-platform-compat` job runs this binary on both
//! `ubuntu-latest` (x86_64) and `ubuntu-24.04-arm` (aarch64), passing a
//! binary artifact between them. Both directions are tested:
//!
//! - x86_64 writes  -> aarch64 reads
//! - aarch64 writes -> x86_64 reads

use bson::{doc, Document};
use crc32c::crc32c;
use std::{
    env,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    process,
};

// ---------------------------------------------------------------------------
// File format constants - must stay in sync with storage/header.rs
// ---------------------------------------------------------------------------

/// `.mqlite` magic bytes (ASCII "MQLT").
const FILE_MAGIC: [u8; 4] = *b"MQLT";

/// Current file format version.
const FORMAT_VERSION: u32 = 1;

/// Internal node page size (bytes).
const PAGE_SIZE_INTERNAL: u32 = 4_096;

/// Leaf node page size (bytes).
const PAGE_SIZE_LEAF: u32 = 32_768;

/// Total size of the header page (== PAGE_SIZE_INTERNAL).
const HEADER_SIZE: usize = PAGE_SIZE_INTERNAL as usize;

/// Checksum algorithm code for CRC32C.
const CHECKSUM_ALGO_CRC32C: u32 = 1;

/// Number of bytes covered by the header checksum (bytes 0-63).
const HEADER_CHECKSUM_RANGE: usize = 64;

/// Byte offset of the header checksum field.
const HEADER_CHECKSUM_OFFSET: usize = 64;

// ---------------------------------------------------------------------------
// Deterministic test data constants
// ---------------------------------------------------------------------------

/// Number of documents written/verified.
const NUM_DOCS: u32 = 100;

/// Fixed creation timestamp - keeps written bytes reproducible across runs.
const FIXED_CREATED_AT: u64 = 1_700_000_000_000;

/// Fixed WAL salt 1 used in the compat header.
const FIXED_SALT1: u32 = 0x1234_5678;

/// Fixed WAL salt 2 used in the compat header.
const FIXED_SALT2: u32 = 0x9ABC_DEF0;

// ---------------------------------------------------------------------------
// Test document generation
// ---------------------------------------------------------------------------

/// Build the Nth test document with fully deterministic content.
///
/// Content is derived from `n` using only wrapping arithmetic so there is
/// no platform dependence; the resulting BSON bytes must be byte-identical
/// everywhere.
fn make_doc(n: u32) -> Document {
    doc! {
        "seq": n as i32,
        "name": format!("doc_{n:03}"),
        "value": n.wrapping_mul(31).wrapping_add(7) as i32,
        "even": (n % 2 == 0),
        "nested": {
            "x": n as i32,
            "y": n.wrapping_mul(n) as i32,
        },
    }
}

// ---------------------------------------------------------------------------
// Header I/O
// ---------------------------------------------------------------------------

/// Serialise a minimal `.mqlite` file header page (4 096 bytes) and write it
/// to `file` at offset 0.
///
/// All multi-byte fields use explicit little-endian byte order.
/// Bytes 60–63 hold a CRC32C checksum of the first 60 bytes.
fn write_header(file: &mut File) -> io::Result<()> {
    let mut buf = [0u8; HEADER_SIZE];

    buf[0..4].copy_from_slice(&FILE_MAGIC);
    buf[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf[8..12].copy_from_slice(&PAGE_SIZE_INTERNAL.to_le_bytes());
    buf[12..16].copy_from_slice(&PAGE_SIZE_LEAF.to_le_bytes());
    buf[16..24].copy_from_slice(&FIXED_CREATED_AT.to_le_bytes());
    // Offsets 24..36: last_checkpoint_ts (Ts-LE, 12 B) - left zero for this
    // static test fixture.
    // Offsets 36-59: zero (catalog root, free lists, page counts)
    buf[60..64].copy_from_slice(&CHECKSUM_ALGO_CRC32C.to_le_bytes());
    // Offset 64-67: CRC32C of bytes 0-63
    let checksum = crc32c(&buf[..HEADER_CHECKSUM_RANGE]);
    buf[HEADER_CHECKSUM_OFFSET..HEADER_CHECKSUM_OFFSET + 4]
        .copy_from_slice(&checksum.to_le_bytes());
    // Offset 68-71: WAL salt 1
    buf[68..72].copy_from_slice(&FIXED_SALT1.to_le_bytes());
    // Offset 72-75: WAL salt 2
    buf[72..76].copy_from_slice(&FIXED_SALT2.to_le_bytes());
    // Offsets 76-4095: zero (catalog root backup, reserved, padding)

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&buf)?;
    Ok(())
}

/// Read and validate the `.mqlite` file header from `file`.
///
/// Checks magic, format version, page sizes, CRC32C checksum, and the fixed
/// WAL salts written by this tool.
fn verify_header(file: &mut File) -> Result<(), String> {
    let mut buf = [0u8; HEADER_SIZE];
    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;

    // Magic
    if buf[0..4] != FILE_MAGIC {
        return Err(format!(
            "bad magic bytes: {:?} (expected {:?})",
            &buf[0..4],
            FILE_MAGIC
        ));
    }

    // Format version
    let version = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if version != FORMAT_VERSION {
        return Err(format!(
            "bad format version {version} (expected {FORMAT_VERSION})"
        ));
    }

    // Page sizes
    let psi = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
    if psi != PAGE_SIZE_INTERNAL {
        return Err(format!(
            "bad internal page size {psi} (expected {PAGE_SIZE_INTERNAL})"
        ));
    }
    let psl = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    if psl != PAGE_SIZE_LEAF {
        return Err(format!(
            "bad leaf page size {psl} (expected {PAGE_SIZE_LEAF})"
        ));
    }

    // created_at
    let created_at = u64::from_le_bytes([
        buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
    ]);
    if created_at != FIXED_CREATED_AT {
        return Err(format!(
            "bad created_at {created_at} (expected {FIXED_CREATED_AT})"
        ));
    }

    // CRC32C over bytes 0-63
    let computed_crc = crc32c(&buf[..HEADER_CHECKSUM_RANGE]);
    let stored_crc = u32::from_le_bytes([buf[64], buf[65], buf[66], buf[67]]);
    if computed_crc != stored_crc {
        return Err(format!(
            "header checksum mismatch: stored 0x{stored_crc:08X}, \
             computed 0x{computed_crc:08X}"
        ));
    }

    // WAL salts (offsets 68–75).
    let salt1 = u32::from_le_bytes([buf[68], buf[69], buf[70], buf[71]]);
    if salt1 != FIXED_SALT1 {
        return Err(format!(
            "bad wal_salt1 0x{salt1:08X} (expected 0x{FIXED_SALT1:08X})"
        ));
    }
    let salt2 = u32::from_le_bytes([buf[72], buf[73], buf[74], buf[75]]);
    if salt2 != FIXED_SALT2 {
        return Err(format!(
            "bad wal_salt2 0x{salt2:08X} (expected 0x{FIXED_SALT2:08X})"
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Document I/O
// ---------------------------------------------------------------------------

/// Write [`NUM_DOCS`] BSON test documents to `file` starting after the header.
///
/// Layout (starting at byte 4096):
/// ```text
/// [doc_count: u32 LE]
/// [doc_len: u32 LE][bson_bytes...]  x  doc_count
/// ```
fn write_docs(file: &mut File) -> io::Result<()> {
    file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
    file.write_all(&NUM_DOCS.to_le_bytes())?;

    for i in 0..NUM_DOCS {
        let doc = make_doc(i);
        let bson_bytes = bson::to_vec(&doc)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let len = bson_bytes.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&bson_bytes)?;
    }

    file.flush()
}

/// Read and verify [`NUM_DOCS`] BSON documents from `file` starting after the
/// header.
///
/// Each document is byte-decoded and compared to the expected value produced
/// by [`make_doc`].
fn verify_docs(file: &mut File) -> Result<(), String> {
    file.seek(SeekFrom::Start(HEADER_SIZE as u64))
        .map_err(|e| e.to_string())?;

    let mut count_buf = [0u8; 4];
    file.read_exact(&mut count_buf).map_err(|e| e.to_string())?;
    let count = u32::from_le_bytes(count_buf);
    if count != NUM_DOCS {
        return Err(format!("bad document count {count} (expected {NUM_DOCS})"));
    }

    for i in 0..NUM_DOCS {
        // Read document length
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)
            .map_err(|e| format!("doc {i}: read length: {e}"))?;
        let len = u32::from_le_bytes(len_buf) as usize;

        // Read BSON bytes
        let mut bson_bytes = vec![0u8; len];
        file.read_exact(&mut bson_bytes)
            .map_err(|e| format!("doc {i}: read bytes: {e}"))?;

        // Deserialise
        let got: Document =
            bson::from_slice(&bson_bytes).map_err(|e| format!("doc {i}: BSON deserialize: {e}"))?;

        // Compare with expected
        let expected = make_doc(i);
        if got != expected {
            return Err(format!(
                "doc {i} mismatch:\n  expected: {expected:?}\n  got:      {got:?}"
            ));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Write a compat file to `path`.
fn cmd_write(path: &Path) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| format!("open {path:?} for write: {e}"))?;

    write_header(&mut file).map_err(|e| format!("write_header: {e}"))?;
    write_docs(&mut file).map_err(|e| format!("write_docs: {e}"))?;

    println!("Wrote {NUM_DOCS} documents to {path:?}");
    Ok(())
}

/// Read and verify a compat file from `path`.
fn cmd_read(path: &Path) -> Result<(), String> {
    let mut file = File::open(path).map_err(|e| format!("open {path:?} for read: {e}"))?;

    verify_header(&mut file)?;
    verify_docs(&mut file)?;

    println!("Verified {NUM_DOCS} documents in {path:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: format_compat write|read <path>");
        process::exit(2);
    }

    let result = match args[1].as_str() {
        "write" => cmd_write(Path::new(&args[2])),
        "read" => cmd_read(Path::new(&args[2])),
        other => Err(format!("unknown command {other:?}; expected write or read")),
    };

    match result {
        Ok(()) => {}
        Err(e) => {
            eprintln!("ERROR: {e}");
            process::exit(1);
        }
    }
}
