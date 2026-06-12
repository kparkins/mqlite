//! Parsed-node facade.
//!
//! The leaf and internal node representations were split into
//! [`super::leaf_node`] and [`super::internal_node`] (R14). This module
//! re-exports them under the historical `node::` path so the insert / delete /
//! scan / chain helpers keep importing `super::node::{...}` unchanged.

pub(super) use super::internal_node::InternalNode;
pub(crate) use super::leaf_node::CellValue;
pub(super) use super::leaf_node::{LeafCell, LeafNode, SplitResult};
