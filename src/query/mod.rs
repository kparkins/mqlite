//! Query engine — filter evaluation, query planning, operator dispatch.
//!
//! This is a private internal module. The public API is exposed through
//! [`Collection`](crate::collection::Collection).
//!
//! Provides BSON key encoding (MongoDB comparison ordering), error mapping,
//! filter evaluation (comparison, logical, element, array operators, `$regex`),
//! and query planning (index selection, sort, projection).

pub(crate) mod explain;
mod filter;
pub(crate) mod planner;

pub(crate) use filter::eval_filter;
pub(crate) use filter::get_nested_field;
