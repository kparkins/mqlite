//! Shared memory WAL index (.mqlite-shm).
//!
//! The SHM file is a fixed-size file (~49.7 KB) that maps page numbers to
//! WAL frame byte offsets.  It allows readers to locate the latest committed
//! frame for a given page in O(1) without scanning the entire WAL.
//!
//! ## SHM Layout
//!
//! ```text
//! Offset  Size   Field
//!   0      4     reader_count: u32 LE
//!   4      4     writer_lock: u32 LE (PID of current writer, 0 = none)
//!   8     512    reader_slots: [ReaderSlot; 64]  (8 bytes each)
//!  520   49152   WAL index hash table: [HashBucket; 4096]  (12 bytes each)
//! Total: 49672 bytes
//! ```
//!
//! ## Hash Table
//!
//! Open-addressing hash table with linear probing.  Each bucket stores:
//! - `page_number: u32` — the indexed page number
//! - `wal_offset: u64` — byte offset in the WAL file where the frame starts
//!
//! A `page_number` of `u32::MAX` signals an empty bucket.
//! A `page_number` of `u32::MAX - 1` signals a tombstone (deleted entry).
//!
//! **Load factor**: When 3072 of the 4096 buckets are occupied (≥ 75%), an
//! emergency checkpoint must be triggered to reduce WAL size before accepting
//! new writes.
//!
//! ## Reader Slots
//!
//! Each of the 64 reader slots records the snapshot WAL position at which that
//! reader started its read.  This prevents checkpoint from advancing past a
//! committed frame that an active reader may still need.
//!
//! ## Writer Lock
//!
//! The `writer_lock` field holds the OS PID of the process currently writing
//! to the WAL.  PID 0 means no active writer.  The writer sets this field
//! before appending frames and clears it after committing and updating the
//! hash table.
//!
//! ## Phase 1 Notes
//!
//! Phase 1 targets single-process operation.  The SHM file is read from and
//! written to synchronously via regular I/O (not `mmap`).  Multi-process
//! SHM coordination (`mmap` + memory barriers) is deferred to Phase 2.

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Layout constants
// ---------------------------------------------------------------------------

/// Number of reader slots in the SHM file.
pub(crate) const SHM_MAX_READERS: usize = 64;

/// Size of one reader slot in bytes (snapshot_id: u32 + pid: u32 = 8).
const SHM_READER_SLOT_SIZE: usize = 8;

/// Number of hash table buckets.
pub(crate) const SHM_HASH_BUCKETS: usize = 4096;

/// Size of one hash table bucket in bytes (page_number: u32 + wal_offset: u64 = 12).
const SHM_BUCKET_SIZE: usize = 12;

/// Byte offset of the reader_count field.
const SHM_READER_COUNT_OFFSET: usize = 0;

/// Byte offset of the writer_lock field.
const SHM_WRITER_LOCK_OFFSET: usize = 4;

/// Byte offset of the first reader slot.
const SHM_READER_SLOTS_OFFSET: usize = 8;

/// Byte offset of the hash table.
const SHM_HASH_TABLE_OFFSET: usize = SHM_READER_SLOTS_OFFSET + SHM_MAX_READERS * SHM_READER_SLOT_SIZE;

/// Total SHM file size in bytes.
pub(crate) const SHM_FILE_SIZE: usize =
    SHM_HASH_TABLE_OFFSET + SHM_HASH_BUCKETS * SHM_BUCKET_SIZE;

/// Sentinel page_number indicating an empty bucket.
const EMPTY_BUCKET: u32 = u32::MAX;

/// Sentinel page_number indicating a deleted (tombstone) bucket.
const TOMBSTONE_BUCKET: u32 = u32::MAX - 1;

/// Emergency checkpoint threshold: trigger when occupied buckets reach this.
pub(crate) const SHM_EMERGENCY_CHECKPOINT_THRESHOLD: usize = (SHM_HASH_BUCKETS * 3) / 4; // 3072

// ---------------------------------------------------------------------------
// In-memory SHM representation
// ---------------------------------------------------------------------------

/// In-memory representation of the SHM WAL index.
///
/// On startup the SHM file is loaded into this struct.  All reads/writes
/// go through the in-memory state; the caller flushes to disk after each
/// committed transaction.
pub(crate) struct ShmIndex {
    /// Backing buffer: exactly [`SHM_FILE_SIZE`] bytes.
    data: Vec<u8>,
    /// Number of non-empty, non-tombstone buckets (occupied entries).
    occupied: usize,
}

impl ShmIndex {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new, empty SHM index (all buckets empty).
    pub(crate) fn new() -> Self {
        let mut data = vec![0u8; SHM_FILE_SIZE];
        // Initialize all hash table buckets with the empty sentinel.
        for i in 0..SHM_HASH_BUCKETS {
            let off = SHM_HASH_TABLE_OFFSET + i * SHM_BUCKET_SIZE;
            data[off..off + 4].copy_from_slice(&EMPTY_BUCKET.to_le_bytes());
        }
        Self { data, occupied: 0 }
    }

    /// Load the SHM index from an existing SHM file.
    ///
    /// If the file is shorter than expected or doesn't exist, returns a fresh
    /// empty index (the caller should persist it).
    pub(crate) fn load(path: &Path) -> Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) if bytes.len() == SHM_FILE_SIZE => {
                let occupied = Self::count_occupied(&bytes);
                Ok(Self {
                    data: bytes,
                    occupied,
                })
            }
            Ok(_) => {
                // Wrong size — treat as corrupt/stale; start fresh.
                Ok(Self::new())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Persist the SHM index to disk, creating the file if necessary.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(Error::Io)?;
        f.write_all(&self.data).map_err(Error::Io)?;
        f.flush().map_err(Error::Io)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // WAL index operations
    // -----------------------------------------------------------------------

    /// Look up `page_number` in the WAL index.
    ///
    /// Returns `Some(wal_frame_offset)` if the page has a committed frame in
    /// the WAL, or `None` if the page should be read from the main file.
    pub(crate) fn lookup(&self, page_number: u32) -> Option<u64> {
        let mut slot = self.bucket_for(page_number);
        loop {
            let pn = self.bucket_page_number(slot);
            if pn == EMPTY_BUCKET {
                return None; // not in WAL
            }
            if pn == page_number {
                return Some(self.bucket_offset(slot));
            }
            // TOMBSTONE or collision — probe next
            slot = (slot + 1) % SHM_HASH_BUCKETS;
            // Safety: load factor ≤ 75% guarantees we'll hit an EMPTY before
            // wrapping back to start.  At 100% load the search would loop
            // forever, so emergency checkpoint prevents that.
        }
    }

    /// Insert or update the WAL frame offset for `page_number`.
    ///
    /// Returns `true` if an emergency checkpoint should be triggered (load
    /// factor has reached [`SHM_EMERGENCY_CHECKPOINT_THRESHOLD`]).
    pub(crate) fn insert(&mut self, page_number: u32, wal_offset: u64) -> bool {
        debug_assert!(page_number != EMPTY_BUCKET && page_number != TOMBSTONE_BUCKET);

        let mut slot = self.bucket_for(page_number);
        let mut first_tombstone: Option<usize> = None;

        loop {
            let pn = self.bucket_page_number(slot);
            if pn == page_number {
                // Update existing entry (no change to occupied count).
                self.set_bucket(slot, page_number, wal_offset);
                return false;
            }
            if pn == TOMBSTONE_BUCKET && first_tombstone.is_none() {
                first_tombstone = Some(slot);
            }
            if pn == EMPTY_BUCKET {
                // Insert at first tombstone (if any) or current empty slot.
                let target = first_tombstone.unwrap_or(slot);
                self.set_bucket(target, page_number, wal_offset);
                self.occupied += 1;
                return self.occupied >= SHM_EMERGENCY_CHECKPOINT_THRESHOLD;
            }
            slot = (slot + 1) % SHM_HASH_BUCKETS;
        }
    }

    /// Remove the WAL index entry for `page_number` (tombstone it).
    ///
    /// Called during checkpoint to indicate that a page no longer needs to be
    /// read from the WAL.  Tombstones are reclaimed when the index is cleared.
    pub(crate) fn remove(&mut self, page_number: u32) {
        let mut slot = self.bucket_for(page_number);
        loop {
            let pn = self.bucket_page_number(slot);
            if pn == EMPTY_BUCKET {
                return; // not found — nothing to remove
            }
            if pn == page_number {
                // Write tombstone
                let off = SHM_HASH_TABLE_OFFSET + slot * SHM_BUCKET_SIZE;
                self.data[off..off + 4].copy_from_slice(&TOMBSTONE_BUCKET.to_le_bytes());
                if self.occupied > 0 {
                    self.occupied -= 1;
                }
                return;
            }
            slot = (slot + 1) % SHM_HASH_BUCKETS;
        }
    }

    /// Clear all WAL index entries (after a full checkpoint).
    ///
    /// Resets every bucket to the empty sentinel and the occupied count to 0.
    pub(crate) fn clear_index(&mut self) {
        for i in 0..SHM_HASH_BUCKETS {
            let off = SHM_HASH_TABLE_OFFSET + i * SHM_BUCKET_SIZE;
            self.data[off..off + 4].copy_from_slice(&EMPTY_BUCKET.to_le_bytes());
            self.data[off + 4..off + 12].fill(0);
        }
        self.occupied = 0;
    }

    /// Return the number of occupied entries in the WAL index.
    pub(crate) fn occupied_count(&self) -> usize {
        self.occupied
    }

    /// Iterate over all occupied entries in the hash table.
    ///
    /// Yields `(page_number, wal_offset)` pairs in unspecified order.
    pub(crate) fn iter_entries(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        (0..SHM_HASH_BUCKETS).filter_map(|slot| {
            let pn = self.bucket_page_number(slot);
            if pn != EMPTY_BUCKET && pn != TOMBSTONE_BUCKET {
                Some((pn, self.bucket_offset(slot)))
            } else {
                None
            }
        })
    }

    // -----------------------------------------------------------------------
    // Writer lock (single-process phase 1)
    // -----------------------------------------------------------------------

    /// Record the current process as the active WAL writer.
    pub(crate) fn acquire_writer_lock(&mut self) {
        let pid = std::process::id();
        self.data[SHM_WRITER_LOCK_OFFSET..SHM_WRITER_LOCK_OFFSET + 4]
            .copy_from_slice(&pid.to_le_bytes());
    }

    /// Clear the writer lock.
    pub(crate) fn release_writer_lock(&mut self) {
        self.data[SHM_WRITER_LOCK_OFFSET..SHM_WRITER_LOCK_OFFSET + 4]
            .copy_from_slice(&0u32.to_le_bytes());
    }

    /// Return the PID stored in the writer lock field (0 = no writer).
    pub(crate) fn writer_lock_pid(&self) -> u32 {
        u32::from_le_bytes(
            self.data[SHM_WRITER_LOCK_OFFSET..SHM_WRITER_LOCK_OFFSET + 4]
                .try_into()
                .expect("4 bytes"),
        )
    }

    // -----------------------------------------------------------------------
    // Reader slots
    // -----------------------------------------------------------------------

    /// Claim a reader slot for the current process, recording `snapshot_wal_end`
    /// as the WAL position beyond which this reader will not read.
    ///
    /// Returns the slot index, or `None` if all slots are in use.
    pub(crate) fn acquire_reader_slot(&mut self, snapshot_wal_end: u64) -> Option<usize> {
        for i in 0..SHM_MAX_READERS {
            let off = SHM_READER_SLOTS_OFFSET + i * SHM_READER_SLOT_SIZE;
            let pid = u32::from_le_bytes(
                self.data[off + 4..off + 8].try_into().expect("4 bytes"),
            );
            if pid == 0 {
                // Free slot — claim it.
                let snap = snapshot_wal_end as u32; // low 32 bits (sufficient for Phase 1)
                self.data[off..off + 4].copy_from_slice(&snap.to_le_bytes());
                self.data[off + 4..off + 8]
                    .copy_from_slice(&std::process::id().to_le_bytes());
                // Increment reader count
                let count = self.reader_count() + 1;
                self.data[SHM_READER_COUNT_OFFSET..SHM_READER_COUNT_OFFSET + 4]
                    .copy_from_slice(&count.to_le_bytes());
                return Some(i);
            }
        }
        None
    }

    /// Release a reader slot, allowing checkpoint to advance past the snapshot.
    pub(crate) fn release_reader_slot(&mut self, slot: usize) {
        let off = SHM_READER_SLOTS_OFFSET + slot * SHM_READER_SLOT_SIZE;
        self.data[off..off + 8].fill(0);
        let count = self.reader_count().saturating_sub(1);
        self.data[SHM_READER_COUNT_OFFSET..SHM_READER_COUNT_OFFSET + 4]
            .copy_from_slice(&count.to_le_bytes());
    }

    /// Return the current reader count.
    pub(crate) fn reader_count(&self) -> u32 {
        u32::from_le_bytes(
            self.data[SHM_READER_COUNT_OFFSET..SHM_READER_COUNT_OFFSET + 4]
                .try_into()
                .expect("4 bytes"),
        )
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn bucket_for(&self, page_number: u32) -> usize {
        // FNV-1a-inspired mix for better distribution
        let h = page_number.wrapping_mul(2654435761); // Knuth multiplicative hash
        (h as usize) % SHM_HASH_BUCKETS
    }

    fn bucket_page_number(&self, slot: usize) -> u32 {
        let off = SHM_HASH_TABLE_OFFSET + slot * SHM_BUCKET_SIZE;
        u32::from_le_bytes(self.data[off..off + 4].try_into().expect("4 bytes"))
    }

    fn bucket_offset(&self, slot: usize) -> u64 {
        let off = SHM_HASH_TABLE_OFFSET + slot * SHM_BUCKET_SIZE;
        u64::from_le_bytes(self.data[off + 4..off + 12].try_into().expect("8 bytes"))
    }

    fn set_bucket(&mut self, slot: usize, page_number: u32, wal_offset: u64) {
        let off = SHM_HASH_TABLE_OFFSET + slot * SHM_BUCKET_SIZE;
        self.data[off..off + 4].copy_from_slice(&page_number.to_le_bytes());
        self.data[off + 4..off + 12].copy_from_slice(&wal_offset.to_le_bytes());
    }

    fn count_occupied(data: &[u8]) -> usize {
        let mut count = 0;
        for i in 0..SHM_HASH_BUCKETS {
            let off = SHM_HASH_TABLE_OFFSET + i * SHM_BUCKET_SIZE;
            let pn = u32::from_le_bytes(data[off..off + 4].try_into().expect("4 bytes"));
            if pn != EMPTY_BUCKET && pn != TOMBSTONE_BUCKET {
                count += 1;
            }
        }
        count
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shm_file_size_constant() {
        // 8 (header) + 512 (reader slots) + 49152 (hash table) = 49672
        assert_eq!(SHM_HASH_TABLE_OFFSET, 520);
        assert_eq!(SHM_FILE_SIZE, 49672);
    }

    #[test]
    fn new_index_is_empty() {
        let idx = ShmIndex::new();
        assert_eq!(idx.occupied_count(), 0);
        assert!(idx.lookup(42).is_none());
        assert_eq!(idx.data.len(), SHM_FILE_SIZE);
    }

    #[test]
    fn insert_and_lookup() {
        let mut idx = ShmIndex::new();
        let emergency = idx.insert(10, 1024);
        assert!(!emergency);
        assert_eq!(idx.lookup(10), Some(1024));
        assert!(idx.lookup(99).is_none());
    }

    #[test]
    fn update_existing_entry() {
        let mut idx = ShmIndex::new();
        idx.insert(5, 100);
        idx.insert(5, 200); // update
        assert_eq!(idx.lookup(5), Some(200));
        assert_eq!(idx.occupied_count(), 1); // still 1 entry
    }

    #[test]
    fn remove_entry() {
        let mut idx = ShmIndex::new();
        idx.insert(7, 512);
        idx.remove(7);
        assert!(idx.lookup(7).is_none());
        assert_eq!(idx.occupied_count(), 0);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut idx = ShmIndex::new();
        idx.remove(999); // should not panic
    }

    #[test]
    fn clear_index_resets_everything() {
        let mut idx = ShmIndex::new();
        idx.insert(1, 10);
        idx.insert(2, 20);
        idx.insert(3, 30);
        idx.clear_index();
        assert_eq!(idx.occupied_count(), 0);
        assert!(idx.lookup(1).is_none());
        assert!(idx.lookup(2).is_none());
    }

    #[test]
    fn multiple_inserts_all_recoverable() {
        let mut idx = ShmIndex::new();
        for i in 0..100u32 {
            idx.insert(i, (i as u64) * 4096);
        }
        for i in 0..100u32 {
            assert_eq!(idx.lookup(i), Some((i as u64) * 4096));
        }
    }

    #[test]
    fn iter_entries_covers_all_inserted() {
        let mut idx = ShmIndex::new();
        for i in 0..10u32 {
            idx.insert(i, (i as u64) * 1000);
        }
        let mut found: Vec<u32> = idx.iter_entries().map(|(pn, _)| pn).collect();
        found.sort();
        let expected: Vec<u32> = (0..10).collect();
        assert_eq!(found, expected);
    }

    #[test]
    fn writer_lock_pid() {
        let mut idx = ShmIndex::new();
        assert_eq!(idx.writer_lock_pid(), 0);
        idx.acquire_writer_lock();
        assert_eq!(idx.writer_lock_pid(), std::process::id());
        idx.release_writer_lock();
        assert_eq!(idx.writer_lock_pid(), 0);
    }

    #[test]
    fn reader_slot_acquire_release() {
        let mut idx = ShmIndex::new();
        let slot = idx.acquire_reader_slot(1000).expect("should get a slot");
        assert_eq!(idx.reader_count(), 1);
        idx.release_reader_slot(slot);
        assert_eq!(idx.reader_count(), 0);
    }

    #[test]
    fn emergency_threshold_constant() {
        assert_eq!(SHM_EMERGENCY_CHECKPOINT_THRESHOLD, 3072);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.shm");

        let mut idx = ShmIndex::new();
        idx.insert(100, 99999);
        idx.insert(200, 88888);
        idx.save(&path).unwrap();

        let loaded = ShmIndex::load(&path).unwrap();
        assert_eq!(loaded.lookup(100), Some(99999));
        assert_eq!(loaded.lookup(200), Some(88888));
        assert_eq!(loaded.occupied_count(), 2);
    }

    #[test]
    fn load_missing_file_returns_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.shm");
        let idx = ShmIndex::load(&path).unwrap();
        assert_eq!(idx.occupied_count(), 0);
    }

    #[test]
    fn load_wrong_size_returns_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.shm");
        std::fs::write(&path, b"too short").unwrap();
        let idx = ShmIndex::load(&path).unwrap();
        assert_eq!(idx.occupied_count(), 0);
    }
}
