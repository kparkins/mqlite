use super::*;

#[test]
fn publish_dirty_default_is_clear() {
    let d = PublishDirty::default();
    assert!(!d.published_catalog_dirty);
    assert!(!d.catalog_header_dirty);
}

#[test]
fn mark_published_sets_only_published_bit() {
    let mut d = PublishDirty::default();
    d.mark_published();
    assert!(d.published_catalog_dirty);
    assert!(!d.catalog_header_dirty);
}

#[test]
fn mark_header_sets_only_header_bit() {
    let mut d = PublishDirty::default();
    d.mark_header();
    assert!(!d.published_catalog_dirty);
    assert!(d.catalog_header_dirty);
}
