//! Query engine — filter evaluation, query planning, operator dispatch.
//!
//! This is a private internal module. The public API is exposed through
//! [`Collection`](crate::collection::Collection).
//!
//! Phase 1 implementation:
//! - hq-apk: BSON key encoding (MongoDB comparison ordering)
//! - hq-mx1: Error taxonomy and MongoDB error code mapping
