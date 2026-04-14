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

pub(crate) mod page;
pub(crate) mod header;
pub(crate) mod oid;
