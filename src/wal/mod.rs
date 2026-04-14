//! Write-ahead log (WAL) — durability, recovery, checkpoint.
//!
//! This is a private internal module. The public API is exposed through
//! [`Database`](crate::database::Database) (checkpoint, close, durability configuration).
//!
//! Phase 1 implementation:
//! - hq-9vo: File format — header, page formats (includes WAL header format)
