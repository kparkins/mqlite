//! Storage engine — B+ tree, page manager, buffer pool.
//!
//! This is a private internal module. The public API is exposed through
//! [`Collection`](crate::collection::Collection) and [`Database`](crate::database::Database).
//!
//! ## Module Structure
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`page`] | Page type constants, header structs, and CRC32C checksum helpers |
//! | [`header`] | File header (Page 0 — 4KB) read/write |
//! | [`oid`] | MongoDB-compatible ObjectId generation with `AtomicU32` counter |
//! | [`allocator`] | Page allocator — dual free lists for 4 KB and 32 KB pages |
//!
//! ## Phase 1 implementation tracking
//!
//! - hq-9vo: File format — header, page formats, ObjectId generation
//! - hq-vk2: Buffer pool (CLOCK sweep eviction, pin/unpin)
//! - hq-1t3: Page allocator (dual free lists for 4KB/32KB pages)
//! - hq-bhon: Buffer pool + page allocator + file I/O wiring (R1.1)

// Phase 0/1: storage engine structs and functions.
// Allow dead_code during the Phase 1 transition — each bead wires more
// of this infrastructure into the live query path.
#[allow(dead_code)]
pub(crate) mod allocator;
#[allow(dead_code)]
pub(crate) mod btree;
/// R1.1: BTreePageStore adapter that bridges BTree and BufferPool (RISK-01).
#[allow(dead_code)]
pub(crate) mod btree_store;
pub(crate) mod buffer_pool;
#[allow(dead_code)]
pub(crate) mod catalog;
/// R1.1: File-backed PageIo implementation.
#[allow(dead_code)]
pub(crate) mod file_io;
/// R1.1: BufferPoolHandle — high-level page I/O (fetch_page / alloc_page / free_page / flush).
#[allow(dead_code)]
pub(crate) mod handle;
#[allow(dead_code)]
pub(crate) mod header;
pub(crate) mod lock;
#[allow(dead_code)]
pub(crate) mod oid;
#[allow(dead_code)]
pub(crate) mod page;
/// Phase 1 `StorageEngine` implementation (currently a stub).
pub(crate) mod paged_engine;
#[allow(dead_code)]
pub(crate) mod secondary_index;
