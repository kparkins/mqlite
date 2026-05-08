//! Reconcile planning, driving, and page synthesis.

pub(crate) mod driver;
pub(crate) mod synth;

#[cfg(test)]
#[path = "tests/synthesize_page_visibility.rs"]
mod synthesize_page_visibility;
