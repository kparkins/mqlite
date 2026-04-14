//! Storage engine — B+ tree, page manager, buffer pool.
//!
//! This is a private internal module. The public API is exposed through
//! [`Collection`](crate::collection::Collection) and [`Database`](crate::database::Database).
//!
//! Phase 1 implementation:
//! - hq-9vo: File format — header, page formats, ObjectId generation
