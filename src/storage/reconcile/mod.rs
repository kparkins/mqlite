//! Phase 4 reconcile scaffolding.
//!
//! This module is intentionally narrow for US-001. Later stories fill in the
//! planner, driver, and page synthesis behavior behind this module boundary.

pub(crate) mod driver;
pub(crate) mod plan;
pub(crate) mod synth;

#[cfg(test)]
#[path = "tests/synthesize_page_visibility.rs"]
mod synthesize_page_visibility;
