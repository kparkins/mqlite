//! File header — Page 0 of every `.mqlite` database file.
//!
//! Page 0 is always exactly 4 096 bytes (one internal-node page). The first
//! 132 bytes hold structured header fields; the remaining bytes are
//! zero-filled padding reserved for future use.

// All expect() calls in this module operate on fixed-size slice conversions
// (e.g. `buf[0..4].try_into().expect("4 bytes")`). The slices are guaranteed
// correct by construction — the buffer is always exactly PAGE_SIZE bytes.
// These are safe in practice; they are allowed here. The slices are guaranteed
// correct by construction — see the fixed-size buffer invariant above.
#![allow(clippy::expect_used)]
//!
//! ## On-disk layout (format version 1)
//!
//! ```text
//! Offset  Size  Field
//!   0      4    Magic bytes: "MQLT" (0x4D514C54)
//!   4      4    Format version: u32 LE (current = 1)
//!   8      4    Page size internal: u32 LE (4096)
//!  12      4    Page size leaf: u32 LE (32768)
//!  16      8    DB creation timestamp: u64 LE Unix milliseconds
//!  24     12    Last checkpoint HLC Ts: Ts-LE (physical_ms u64 || logical u32)
//!  36      4    Catalog root page: u32 LE
//!  40      4    Free list head 4 KB: u32 LE (0 = empty)
//!  44      4    Free list head 32 KB: u32 LE (0 = empty)
//!  48      4    Total page count: u32 LE
//!  52      4    Free page count 4 KB: u32 LE
//!  56      4    Free page count 32 KB: u32 LE
//!  60      4    Checksum algorithm: u32 LE (1 = CRC32C)
//!  64      4    Header checksum: CRC32C of bytes 0–63
//!  68      4    WAL salt 1: u32 LE
//!  72      4    WAL salt 2: u32 LE
//!  76      4    Catalog root backup: u32 LE (redundant copy of offset 36)
//!  80      1    Catalog root level: u8
//!  81      8    Next namespace id: u64 LE (Phase 1 §10.7 durable id counter; reserved = 0)
//!  89      8    Next index id: u64 LE (Phase 1 §10.7 durable id counter; reserved = 0)
//!  97      4    History store root page: u32 LE (Phase 1 §10.7; persisted root of HistoryStore)
//! 101     27    Reserved (zero-filled; future: encryption metadata, etc.)
//! 128   3968    Unused padding to 4096 bytes
//! ```
//!
//! The **header checksum** at offset 64 is a CRC32C over bytes 0–63 **only**.
//! Fields at offsets 68 and beyond (WAL salts, catalog root backup) are not
//! included and may be updated without recomputing the checksum.
//!
//! ## WAL stale detection
//!
//! `wal_salt1` and `wal_salt2` are random u32 values chosen at file creation
//! time. The WAL file header stores copies of both salts. On open, if the
//! WAL salts don't match the main file salts, the WAL is stale (left from a
//! different database file) and must be deleted before proceeding.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::mvcc::Ts;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

/// Magic bytes that identify a valid `.mqlite` database file.
pub(crate) const FILE_MAGIC: [u8; 4] = *b"MQLT";

/// Current file format version. Increment on backward-incompatible changes.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// Checksum algorithm code for CRC32C.
pub(crate) const CHECKSUM_ALGO_CRC32C: u32 = 1;

/// Size of the file header page in bytes (equal to one internal page = 4 KiB).
pub(crate) const HEADER_PAGE_SIZE: usize = PAGE_SIZE_INTERNAL as usize;

/// Number of bytes covered by the header checksum (offsets 0–63 inclusive).
const CHECKSUM_RANGE_END: usize = 64;

/// Byte offset of the header checksum field.
const CHECKSUM_OFFSET: usize = 64;

/// Structured representation of the `.mqlite` file header (Page 0).
///
/// Use [`FileHeader::to_bytes`] to serialize and [`FileHeader::from_bytes`] to
/// deserialize. The CRC32C header checksum is computed automatically by
/// `to_bytes` and verified by `from_bytes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileHeader {
    // offset 0
    /// Must equal [`FILE_MAGIC`].
    pub magic: [u8; 4],
    // offset 4
    /// File format version. Must equal [`FORMAT_VERSION`] (1) for this build.
    pub format_version: u32,
    // offset 8
    /// Internal node page size in bytes (always 4096).
    pub page_size_internal: u32,
    // offset 12
    /// Leaf node page size in bytes (always 32768).
    pub page_size_leaf: u32,
    // offset 16
    /// Unix milliseconds when this database file was first created.
    pub created_at: u64,
    // offset 24 (12 bytes — Ts-LE)
    /// HLC timestamp of the last successful checkpoint.
    pub last_checkpoint_ts: Ts,
    // offset 36
    /// Page number of the catalog B+ tree root. 0 = catalog not yet written.
    pub catalog_root_page: u32,
    // offset 40
    /// Head of the 4 KB free-page list. 0 = list is empty.
    pub free_list_head_4k: u32,
    // offset 44
    /// Head of the 32 KB free-page list. 0 = list is empty.
    pub free_list_head_32k: u32,
    // offset 48
    /// Total number of pages in the file (including header page 0).
    pub total_page_count: u32,
    // offset 52
    /// Number of free 4 KB pages available for allocation.
    pub free_page_count_4k: u32,
    // offset 56
    /// Number of free 32 KB pages available for allocation.
    pub free_page_count_32k: u32,
    // offset 60
    /// Checksum algorithm used for page checksums. Must be
    /// [`CHECKSUM_ALGO_CRC32C`] (1).
    pub checksum_algo: u32,
    // offset 64: header_checksum — not stored here; computed on write
    // offset 68
    /// Random u32 used to associate the WAL file with this database file.
    pub wal_salt1: u32,
    // offset 72
    /// Second WAL association salt.
    pub wal_salt2: u32,
    // offset 76
    /// Redundant copy of `catalog_root_page` (offset 36).
    /// Used as a fallback if the primary catalog root page fails checksum
    /// validation on open.
    pub catalog_root_backup: u32,
    // offset 80
    /// Root-level byte of the catalog B+ tree (0 = leaf-only; >0 = internal at that level).
    ///
    /// Stored at offset 80 in the reserved region.  Together with
    /// [`catalog_root_page`](Self::catalog_root_page) this allows reopening the
    /// catalog without a full tree scan.
    pub catalog_root_level: u8,
    // offset 81
    /// Monotonic namespace id counter (Phase 1 §10.7). The next allocated
    /// `NamespaceId` equals this value; on allocation the counter advances
    /// by 1 and the new value is persisted atomically with the owning
    /// catalog commit. Id `0` is reserved and never allocated.
    ///
    /// Initialized to `1` on fresh-DB creation so the first allocated id is
    /// strictly positive. Outside the 0–63 checksum range, so updates do
    /// not require recomputing `header_checksum`.
    pub next_namespace_id: u64,
    // offset 89
    /// Monotonic index id counter (Phase 1 §10.7). Same protocol as
    /// `next_namespace_id` but for `IndexId`.
    pub next_index_id: u64,
    // offset 97
    /// Durable root page of the [`HistoryStore`](crate::storage::history_store::HistoryStore)
    /// (Phase 1 §10.7). On fresh DB, initialized to the page id returned
    /// by `HistoryStore::create_empty_root`. On reopen, `HistoryStore::open`
    /// reads this value and opens the existing tree.
    pub history_store_root_page: u32,
    // offsets 101–127: reserved, zero-filled
    // offsets 128–4095: unused padding
}

impl FileHeader {
    /// Create a fresh header for a new database file.
    ///
    /// - `created_at`: Unix milliseconds (usually from `SystemTime::now()`).
    /// - `wal_salt1`, `wal_salt2`: random values that bind the WAL file to
    ///   this specific database open session. Use [`derive_wal_salts`] if you
    ///   don't have a cryptographic RNG.
    pub(crate) fn new(created_at: u64, wal_salt1: u32, wal_salt2: u32) -> Self {
        Self {
            magic: FILE_MAGIC,
            format_version: FORMAT_VERSION,
            page_size_internal: PAGE_SIZE_INTERNAL,
            page_size_leaf: PAGE_SIZE_LEAF,
            created_at,
            last_checkpoint_ts: Ts::PENDING,
            catalog_root_page: 0,
            free_list_head_4k: 0,
            free_list_head_32k: 0,
            // Page 0 (the header itself) always exists.
            total_page_count: 1,
            free_page_count_4k: 0,
            free_page_count_32k: 0,
            checksum_algo: CHECKSUM_ALGO_CRC32C,
            wal_salt1,
            wal_salt2,
            catalog_root_backup: 0,
            catalog_root_level: 0,
            // Phase 1 §10.7: durable id counters start at 1 so the first
            // `allocate_namespace_id` / `allocate_index_id` returns 1.
            // Id `0` is reserved and must never be allocated.
            next_namespace_id: 1,
            next_index_id: 1,
            // `0` signals "no history store persisted yet"; state.rs creates
            // the empty root on fresh-DB init and writes the id back.
            history_store_root_page: 0,
        }
    }

    /// Create a fresh header stamped with the current wall-clock time.
    ///
    /// WAL salts are derived from `created_at` and the process ID using a
    /// [`DefaultHasher`](std::collections::hash_map::DefaultHasher). Not
    /// cryptographically secure but sufficient for stale-WAL detection.
    pub(crate) fn new_now() -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let (salt1, salt2) = derive_wal_salts(created_at);
        Self::new(created_at, salt1, salt2)
    }

    /// Compute the header checksum: CRC32C of the first 64 bytes of the
    /// serialized header.
    ///
    /// The checksum field at offset 64–67 is **not** included.
    pub(crate) fn compute_checksum(header_prefix: &[u8; CHECKSUM_RANGE_END]) -> u32 {
        crc32c::crc32c(header_prefix)
    }

    /// Serialize the header to a full [`HEADER_PAGE_SIZE`]-byte buffer.
    ///
    /// The checksum at offset 64 is computed and written during serialization.
    /// All reserved bytes (81–127) and padding (128–4095) are zero-filled.
    pub(crate) fn to_bytes(&self) -> [u8; HEADER_PAGE_SIZE] {
        let mut buf = [0u8; HEADER_PAGE_SIZE];
        self.write_fields(&mut buf);

        let prefix: [u8; CHECKSUM_RANGE_END] =
            buf[..CHECKSUM_RANGE_END].try_into().expect("64-byte slice");
        let checksum = Self::compute_checksum(&prefix);
        buf[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4].copy_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Deserialize a [`FileHeader`] from a [`HEADER_PAGE_SIZE`]-byte buffer.
    ///
    /// Validates:
    /// 1. Magic bytes equal `"MQLT"`.
    /// 2. Format version is `1` (the only version this build supports).
    /// 3. Page sizes match the compile-time constants.
    /// 4. Header checksum (CRC32C over bytes 0–63) matches stored value.
    ///
    /// Returns [`Error::CorruptDatabase`] on any validation failure.
    pub(crate) fn from_bytes(buf: &[u8; HEADER_PAGE_SIZE]) -> Result<Self> {
        // 1. Magic
        let magic: [u8; 4] = buf[..4].try_into().expect("4 bytes");
        if magic != FILE_MAGIC {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!("invalid magic: expected {FILE_MAGIC:?} ('MQLT'), found {magic:?}"),
                recoverable: false,
            });
        }

        // 2. Format version
        let format_version = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        if format_version != FORMAT_VERSION {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "unsupported format version {format_version} \
                     (this build supports version {FORMAT_VERSION})"
                ),
                recoverable: false,
            });
        }

        // 3. Page sizes
        let page_size_internal = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let page_size_leaf = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));

        if page_size_internal != PAGE_SIZE_INTERNAL {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "unexpected internal page size {page_size_internal} \
                     (expected {PAGE_SIZE_INTERNAL})"
                ),
                recoverable: false,
            });
        }
        if page_size_leaf != PAGE_SIZE_LEAF {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "unexpected leaf page size {page_size_leaf} \
                     (expected {PAGE_SIZE_LEAF})"
                ),
                recoverable: false,
            });
        }

        // 4. Header checksum: CRC32C of bytes 0–63
        let prefix: [u8; CHECKSUM_RANGE_END] =
            buf[..CHECKSUM_RANGE_END].try_into().expect("64 bytes");
        let computed = Self::compute_checksum(&prefix);
        let stored = u32::from_le_bytes(
            buf[CHECKSUM_OFFSET..CHECKSUM_OFFSET + 4]
                .try_into()
                .expect("4 bytes"),
        );
        if stored != computed {
            #[cfg(feature = "tracing")]
            tracing::error!(
                target: "mqlite",
                stored,
                computed,
                "mqlite::corrupt_page"
            );
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "header checksum mismatch: stored 0x{stored:08X}, computed 0x{computed:08X}"
                ),
                recoverable: true,
            });
        }

        let last_checkpoint_ts = Ts::from_le_bytes(buf[24..36].try_into().expect("12 bytes"));

        Ok(Self {
            magic,
            format_version,
            page_size_internal,
            page_size_leaf,
            created_at: u64::from_le_bytes(buf[16..24].try_into().expect("8 bytes")),
            last_checkpoint_ts,
            catalog_root_page: u32::from_le_bytes(buf[36..40].try_into().expect("4 bytes")),
            free_list_head_4k: u32::from_le_bytes(buf[40..44].try_into().expect("4 bytes")),
            free_list_head_32k: u32::from_le_bytes(buf[44..48].try_into().expect("4 bytes")),
            total_page_count: u32::from_le_bytes(buf[48..52].try_into().expect("4 bytes")),
            free_page_count_4k: u32::from_le_bytes(buf[52..56].try_into().expect("4 bytes")),
            free_page_count_32k: u32::from_le_bytes(buf[56..60].try_into().expect("4 bytes")),
            checksum_algo: u32::from_le_bytes(buf[60..64].try_into().expect("4 bytes")),
            wal_salt1: u32::from_le_bytes(buf[68..72].try_into().expect("4 bytes")),
            wal_salt2: u32::from_le_bytes(buf[72..76].try_into().expect("4 bytes")),
            catalog_root_backup: u32::from_le_bytes(buf[76..80].try_into().expect("4 bytes")),
            catalog_root_level: buf[80],
            // Phase 1 §10.7 — durable id counters + history-store root page.
            next_namespace_id: u64::from_le_bytes(buf[81..89].try_into().expect("8 bytes")),
            next_index_id: u64::from_le_bytes(buf[89..97].try_into().expect("8 bytes")),
            history_store_root_page: u32::from_le_bytes(buf[97..101].try_into().expect("4 bytes")),
        })
    }

    /// Perform semantic validation beyond what [`from_bytes`](Self::from_bytes)
    /// checks.
    ///
    /// Call this after a successful `from_bytes` to catch inconsistent state
    /// (e.g., unknown checksum algorithm, zero page count).
    pub(crate) fn validate(&self) -> Result<()> {
        if self.magic != FILE_MAGIC {
            return Err(Error::Internal("magic mismatch after parse".into()));
        }
        if self.checksum_algo != CHECKSUM_ALGO_CRC32C {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "unknown checksum algorithm {} (only CRC32C = 1 is supported)",
                    self.checksum_algo
                ),
                recoverable: false,
            });
        }
        if self.total_page_count == 0 {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: "total_page_count is 0; page 0 (header) must always exist".into(),
                recoverable: false,
            });
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Write all structured fields into `buf`.
    ///
    /// Does **not** write the checksum at offset 64 — that is the caller's
    /// responsibility (done in `to_bytes`).
    fn write_fields(&self, buf: &mut [u8; HEADER_PAGE_SIZE]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size_internal.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size_leaf.to_le_bytes());
        buf[16..24].copy_from_slice(&self.created_at.to_le_bytes());
        buf[24..36].copy_from_slice(&self.last_checkpoint_ts.to_le_bytes());
        buf[36..40].copy_from_slice(&self.catalog_root_page.to_le_bytes());
        buf[40..44].copy_from_slice(&self.free_list_head_4k.to_le_bytes());
        buf[44..48].copy_from_slice(&self.free_list_head_32k.to_le_bytes());
        buf[48..52].copy_from_slice(&self.total_page_count.to_le_bytes());
        buf[52..56].copy_from_slice(&self.free_page_count_4k.to_le_bytes());
        buf[56..60].copy_from_slice(&self.free_page_count_32k.to_le_bytes());
        buf[60..64].copy_from_slice(&self.checksum_algo.to_le_bytes());
        // offset 64–67: checksum — written by to_bytes after this call
        buf[68..72].copy_from_slice(&self.wal_salt1.to_le_bytes());
        buf[72..76].copy_from_slice(&self.wal_salt2.to_le_bytes());
        buf[76..80].copy_from_slice(&self.catalog_root_backup.to_le_bytes());
        buf[80] = self.catalog_root_level;
        // Phase 1 §10.7 — durable id counters + history-store root page.
        buf[81..89].copy_from_slice(&self.next_namespace_id.to_le_bytes());
        buf[89..97].copy_from_slice(&self.next_index_id.to_le_bytes());
        buf[97..101].copy_from_slice(&self.history_store_root_page.to_le_bytes());
        // offsets 101–127: reserved, already zero-filled by array init
        // offsets 128–4095: padding, already zero-filled
    }
}

/// Derive WAL salt values from a creation timestamp and the process ID.
///
/// Not cryptographically secure; uses
/// [`DefaultHasher`](std::collections::hash_map::DefaultHasher). Sufficient
/// for distinguishing WAL files left by different database open sessions.
pub(crate) fn derive_wal_salts(created_at_millis: u64) -> (u32, u32) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    created_at_millis.hash(&mut h);
    std::process::id().hash(&mut h);
    let v1 = h.finish();

    v1.hash(&mut h);
    let v2 = h.finish();

    (v1 as u32, ((v1 >> 32) as u32) ^ (v2 as u32))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TS: u64 = 1_700_000_000_000; // arbitrary fixed timestamp

    fn fresh_header() -> FileHeader {
        FileHeader::new(TEST_TS, 0xDEAD_BEEF, 0xCAFE_BABE)
    }

    // -----------------------------------------------------------------------
    // Roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_preserves_all_fields() {
        let original = fresh_header();
        let bytes = original.to_bytes();
        let decoded = FileHeader::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.magic, FILE_MAGIC);
        assert_eq!(decoded.format_version, FORMAT_VERSION);
        assert_eq!(decoded.page_size_internal, 4096);
        assert_eq!(decoded.page_size_leaf, 32768);
        assert_eq!(decoded.created_at, TEST_TS);
        assert_eq!(decoded.last_checkpoint_ts, Ts::PENDING);
        assert_eq!(decoded.catalog_root_page, 0);
        assert_eq!(decoded.free_list_head_4k, 0);
        assert_eq!(decoded.free_list_head_32k, 0);
        assert_eq!(decoded.total_page_count, 1);
        assert_eq!(decoded.free_page_count_4k, 0);
        assert_eq!(decoded.free_page_count_32k, 0);
        assert_eq!(decoded.checksum_algo, CHECKSUM_ALGO_CRC32C);
        assert_eq!(decoded.wal_salt1, 0xDEAD_BEEF);
        assert_eq!(decoded.wal_salt2, 0xCAFE_BABE);
        assert_eq!(decoded.catalog_root_backup, 0);
    }

    #[test]
    fn roundtrip_nonzero_counters() {
        let mut h = fresh_header();
        h.catalog_root_page = 99;
        h.free_list_head_4k = 12;
        h.free_list_head_32k = 33;
        h.total_page_count = 500;
        h.free_page_count_4k = 10;
        h.free_page_count_32k = 5;
        h.catalog_root_backup = 99;
        h.last_checkpoint_ts = Ts {
            physical_ms: 1_700_000_050_000,
            logical: 42,
        };

        let bytes = h.to_bytes();
        let decoded = FileHeader::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.catalog_root_page, 99);
        assert_eq!(decoded.free_list_head_4k, 12);
        assert_eq!(decoded.total_page_count, 500);
        assert_eq!(decoded.catalog_root_backup, 99);
        assert_eq!(
            decoded.last_checkpoint_ts,
            Ts {
                physical_ms: 1_700_000_050_000,
                logical: 42
            }
        );
    }

    // -----------------------------------------------------------------------
    // Size
    // -----------------------------------------------------------------------

    #[test]
    fn serialized_size_is_4096_bytes() {
        let bytes = fresh_header().to_bytes();
        assert_eq!(bytes.len(), 4096);
        assert_eq!(HEADER_PAGE_SIZE, 4096);
    }

    #[test]
    fn bytes_beyond_128_are_zero() {
        let bytes = fresh_header().to_bytes();
        assert!(
            bytes[128..].iter().all(|&b| b == 0),
            "padding bytes must be zero"
        );
    }

    #[test]
    fn reserved_bytes_101_to_127_are_zero() {
        // Phase 1 §10.7 — offsets 81..101 now carry `next_namespace_id`,
        // `next_index_id`, and `history_store_root_page`. The reserved
        // zero-fill region now spans 101..128.
        let bytes = fresh_header().to_bytes();
        assert!(
            bytes[101..128].iter().all(|&b| b == 0),
            "reserved region 101..128 must be zero-filled"
        );
    }

    #[test]
    fn durable_id_counters_roundtrip() {
        // Phase 1 §10.7 — persist and recover `next_namespace_id`,
        // `next_index_id`, `history_store_root_page` via the header.
        let mut h = fresh_header();
        h.next_namespace_id = 42;
        h.next_index_id = 99;
        h.history_store_root_page = 314;

        let bytes = h.to_bytes();
        let decoded = FileHeader::from_bytes(&bytes).expect("parse");
        assert_eq!(decoded.next_namespace_id, 42);
        assert_eq!(decoded.next_index_id, 99);
        assert_eq!(decoded.history_store_root_page, 314);
    }

    #[test]
    fn fresh_header_initializes_durable_counters_to_one() {
        // Phase 1 §10.7 — `0` is reserved, so the first allocation must
        // return 1. Both counters must start at 1 on fresh-DB creation.
        let h = fresh_header();
        assert_eq!(h.next_namespace_id, 1);
        assert_eq!(h.next_index_id, 1);
        assert_eq!(h.history_store_root_page, 0);
    }

    // -----------------------------------------------------------------------
    // Checksum
    // -----------------------------------------------------------------------

    #[test]
    fn checksum_is_written_at_offset_64() {
        let h = fresh_header();
        let bytes = h.to_bytes();
        // Checksum at offset 64
        let stored = u32::from_le_bytes(bytes[64..68].try_into().unwrap());
        let computed = FileHeader::compute_checksum(bytes[..64].try_into().unwrap());
        assert_eq!(stored, computed);
    }

    #[test]
    fn checksum_verification_succeeds_for_valid_header() {
        let bytes = fresh_header().to_bytes();
        FileHeader::from_bytes(&bytes).expect("should parse without error");
    }

    #[test]
    fn checksum_verification_fails_on_body_corruption() {
        let mut bytes = fresh_header().to_bytes();
        bytes[10] ^= 0xFF; // corrupt within checksum range (0–63)
        assert!(FileHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn checksum_does_not_cover_wal_salts() {
        // The checksum covers only bytes 0–63. WAL salts are at 68+.
        // Corrupting them AFTER re-writing the checksum should still parse.
        let original = fresh_header();
        let mut bytes = original.to_bytes();
        bytes[68] ^= 0xFF; // corrupt wal_salt1 LSB
                           // from_bytes should succeed (checksum still valid; corruption is outside range)
        let decoded = FileHeader::from_bytes(&bytes).expect("should parse");
        // The decoded salt reflects the corruption
        assert_ne!(decoded.wal_salt1, original.wal_salt1);
    }

    // -----------------------------------------------------------------------
    // Magic and version rejection
    // -----------------------------------------------------------------------

    #[test]
    fn bad_magic_is_rejected() {
        let original = fresh_header();
        let mut bytes = original.to_bytes();
        // Overwrite magic + recompute checksum so only the magic is wrong
        bytes[0] = b'X';
        let new_checksum = FileHeader::compute_checksum(bytes[..64].try_into().unwrap());
        bytes[64..68].copy_from_slice(&new_checksum.to_le_bytes());
        assert!(FileHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn unknown_format_version_is_rejected() {
        let original = fresh_header();
        let mut bytes = original.to_bytes();
        // Overwrite format_version with 99
        bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
        let new_checksum = FileHeader::compute_checksum(bytes[..64].try_into().unwrap());
        bytes[64..68].copy_from_slice(&new_checksum.to_le_bytes());
        assert!(FileHeader::from_bytes(&bytes).is_err());
    }

    // -----------------------------------------------------------------------
    // validate()
    // -----------------------------------------------------------------------

    #[test]
    fn validate_passes_for_well_formed_header() {
        fresh_header().validate().expect("should pass");
    }

    #[test]
    fn validate_fails_for_zero_page_count() {
        let mut h = fresh_header();
        h.total_page_count = 0;
        assert!(h.validate().is_err());
    }

    #[test]
    fn validate_fails_for_unknown_checksum_algo() {
        let mut h = fresh_header();
        h.checksum_algo = 99;
        assert!(h.validate().is_err());
    }

    // -----------------------------------------------------------------------
    // new_now()
    // -----------------------------------------------------------------------

    #[test]
    fn new_now_produces_parseable_header() {
        let h = FileHeader::new_now();
        let bytes = h.to_bytes();
        let decoded = FileHeader::from_bytes(&bytes).unwrap();
        decoded.validate().unwrap();
        assert!(decoded.created_at > 0);
    }

    // -----------------------------------------------------------------------
    // derive_wal_salts
    // -----------------------------------------------------------------------

    #[test]
    fn wal_salts_are_deterministic_for_same_input() {
        let (s1a, s2a) = derive_wal_salts(12345);
        let (s1b, s2b) = derive_wal_salts(12345);
        assert_eq!(s1a, s1b);
        assert_eq!(s2a, s2b);
    }

    #[test]
    fn wal_salts_differ_for_different_timestamps() {
        let (s1a, _) = derive_wal_salts(10000);
        let (s1b, _) = derive_wal_salts(20000);
        // Different timestamps should (with very high probability) produce
        // different salts.
        assert_ne!(s1a, s1b);
    }

    // -----------------------------------------------------------------------
    // Golden bytes — cross-platform endianness contract
    // -----------------------------------------------------------------------

    /// Verify exact byte layout of the serialized header.
    ///
    /// This test proves that every multi-byte field is written in
    /// **explicit little-endian** order.  A file created on x86_64
    /// must be byte-identical and readable on aarch64 (and vice-versa).
    ///
    /// If native-endian writes were ever introduced, this test would fail
    /// on big-endian targets and the cross-platform CI job would catch the
    /// regression on aarch64 hardware.
    #[test]
    fn golden_bytes_little_endian_layout() {
        let h = FileHeader::new(TEST_TS, 0xDEAD_BEEF, 0xCAFE_BABE);
        let bytes = h.to_bytes();

        // Magic "MQLT" is ASCII — byte order is irrelevant but must be exact.
        assert_eq!(&bytes[0..4], b"MQLT", "magic mismatch");

        // Format version = 1  →  LE bytes [0x01, 0x00, 0x00, 0x00]
        assert_eq!(bytes[4], 0x01, "version LSB at offset 4");
        assert_eq!(bytes[5], 0x00);
        assert_eq!(bytes[6], 0x00);
        assert_eq!(bytes[7], 0x00, "version MSB at offset 7");

        // Internal page size = 4096 = 0x0000_1000  →  LE [0x00, 0x10, 0x00, 0x00]
        assert_eq!(bytes[8], 0x00);
        assert_eq!(bytes[9], 0x10, "page_size_internal byte 1");
        assert_eq!(bytes[10], 0x00);
        assert_eq!(bytes[11], 0x00);

        // Leaf page size = 32768 = 0x0000_8000  →  LE [0x00, 0x80, 0x00, 0x00]
        assert_eq!(bytes[12], 0x00);
        assert_eq!(bytes[13], 0x80, "page_size_leaf byte 1");
        assert_eq!(bytes[14], 0x00);
        assert_eq!(bytes[15], 0x00);

        // created_at = TEST_TS = 1_700_000_000_000  →  exact LE bytes
        assert_eq!(
            &bytes[16..24],
            &TEST_TS.to_le_bytes(),
            "created_at LE bytes"
        );

        // last_checkpoint_ts = Ts::PENDING (zeroes) for a fresh header — 12 bytes
        assert_eq!(&bytes[24..36], &[0u8; 12], "last_checkpoint_ts Ts-LE bytes");

        // Checksum algorithm = 1 (CRC32C)  →  LE bytes [0x01, 0x00, 0x00, 0x00] at offset 60
        assert_eq!(bytes[60], 0x01, "checksum_algo LSB");
        assert_eq!(bytes[61], 0x00);
        assert_eq!(bytes[62], 0x00);
        assert_eq!(bytes[63], 0x00, "checksum_algo MSB");

        // WAL salt 1 = 0xDEAD_BEEF  →  LE bytes [0xEF, 0xBE, 0xAD, 0xDE] at offset 68
        assert_eq!(bytes[68], 0xEF, "wal_salt1 byte 0 (LSB)");
        assert_eq!(bytes[69], 0xBE);
        assert_eq!(bytes[70], 0xAD);
        assert_eq!(bytes[71], 0xDE, "wal_salt1 byte 3 (MSB)");

        // WAL salt 2 = 0xCAFE_BABE  →  LE bytes [0xBE, 0xBA, 0xFE, 0xCA] at offset 72
        assert_eq!(bytes[72], 0xBE, "wal_salt2 byte 0 (LSB)");
        assert_eq!(bytes[73], 0xBA);
        assert_eq!(bytes[74], 0xFE);
        assert_eq!(bytes[75], 0xCA, "wal_salt2 byte 3 (MSB)");
    }
}
