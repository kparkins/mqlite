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

// Phase 0: storage engine structs and functions are defined but not yet wired
// into the query/write paths (Phase 1). Allow dead_code for the whole module.
#[allow(dead_code)]
pub(crate) mod header;
#[allow(dead_code)]
pub(crate) mod oid;
#[allow(dead_code)]
pub(crate) mod page;
pub(crate) mod buffer_pool;
#[allow(dead_code)]
pub(crate) mod allocator;
pub(crate) mod lock;
#[allow(dead_code)]
pub(crate) mod btree;
#[allow(dead_code)]
pub(crate) mod catalog;
