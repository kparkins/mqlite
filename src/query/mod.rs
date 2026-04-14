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
//! - hq-1gt: Query planner (index selection, sort, projection)

mod filter;
pub(crate) mod planner;

pub(crate) use filter::eval_filter;
pub(crate) use filter::get_nested_field;
