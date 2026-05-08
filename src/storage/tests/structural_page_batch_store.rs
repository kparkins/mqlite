use std::sync::Arc;

use super::*;
use crate::storage::btree::{BTreePageStore, LeafPageImage};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

fn make_store() -> BufferPoolPageStore {
    let io = MockIo::new();
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    BufferPoolPageStore::new(handle)
}

#[test]
fn structural_batch_read_leaf_returns_private_owned_image() {
    let mut base = make_store();
    let page = base.alloc_leaf().expect("allocate leaf page");
    let mut base_data = [0u8; LEAF_SIZE];
    base_data[0] = 0x11;
    base.write_leaf_structural(page, &base_data)
        .expect("write base leaf");

    let mut writes = StructuralPageWrites::default();
    let mut lifetime = AllocatorLifetimeBatch::default();
    let mut batch_store = StructuralBatchStore::new(base, &mut writes, &mut lifetime);
    let mut private_data = [0u8; LEAF_SIZE];
    private_data[0] = 0x35;
    batch_store
        .write_leaf_structural(page, &private_data)
        .expect("stage private leaf");

    let (image, _) = batch_store
        .read_leaf(page)
        .expect("read staged structural leaf");

    match image {
        LeafPageImage::Owned(data) => assert_eq!(data[0], 0x35),
        LeafPageImage::Shared(_) => panic!("structural staged reads must stay private"),
    }
}
