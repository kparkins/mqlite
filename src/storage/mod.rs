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
//!
//! ## Phase 1 implementation tracking
//!
//! - hq-9vo: File format — header, page formats, ObjectId generation

// Phase 0: storage engine structs and functions are defined but not yet wired
// into the query/write paths (Phase 1). Allow dead_code for the whole module.
#[allow(dead_code)]
pub(crate) mod header;
#[allow(dead_code)]
pub(crate) mod oid;
#[allow(dead_code)]
pub(crate) mod page;
