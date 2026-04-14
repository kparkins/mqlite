//! Query engine — filter evaluation, query planning, operator dispatch.
//!
//! This is a private internal module. The public API is exposed through
//! [`Collection`](crate::collection::Collection).
//!
//! Phase 1 implementation:
//! - hq-apk: BSON key encoding (MongoDB comparison ordering)
//! - hq-mx1: Error taxonomy and MongoDB error code mapping
//! - hq-uii: Filter evaluation engine (comparison, logical, element operators)
//! - hq-ca5: Array operators ($elemMatch, $all, $size) and evaluation ($regex)

mod filter;

// Filter evaluation is the primary query-engine entry point.
// Suppressed unused-import warnings: these will be used when the storage
// engine calls into the query engine (Phase 1 follow-up work).
#[allow(unused_imports)]
pub(crate) use filter::eval_filter;
#[allow(unused_imports)]
pub(crate) use filter::get_nested_field;
