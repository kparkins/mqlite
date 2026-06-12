use std::collections::VecDeque;
use std::sync::Arc;

use super::*;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::{BTree, BTreePageStore, LeafPageImage};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

fn make_handle() -> Arc<BufferPoolHandle> {
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
    Arc::new(BufferPoolHandle::new(pool, history_pool, header))
}

fn make_store() -> BufferPoolPageStore {
    BufferPoolPageStore::new(make_handle())
}

/// A committed resident chain head for `key` carrying `payload` bytes.
fn resident_chain(payload: u8) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(VecDeque::from([VersionEntry {
        start_ts: Ts {
            physical_ms: payload as u64,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: payload as u64,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![payload]),
        is_tombstone: false,
    }]))
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
    let mut migrated = false;
    let mut batch_store = StructuralBatchStore::new(base, &mut writes, &mut lifetime, &mut migrated);
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

/// (i) A chain-free `read_leaf` returns the SAME page image as the
/// chain-carrying read, but with a `None` snapshot — even when the leaf has
/// both base cells and resident MVCC chains. This is the base-fallback branch:
/// no staged bytes, so the read goes through `read_leaf_image_only`.
#[test]
fn chain_free_read_leaf_matches_chain_carrying_image_with_none_snapshot() {
    let handle = make_handle();

    // Seed a real leaf with base cells via a BTree over the base store.
    let root_page = {
        let mut tree = BTree::create(BufferPoolPageStore::new(Arc::clone(&handle)))
            .expect("create base btree");
        tree.insert(b"alpha", b"one").expect("insert alpha");
        tree.insert(b"beta", b"two").expect("insert beta");
        tree.root_page
    };

    // Seed resident MVCC chains on that same leaf page. The page is resident
    // because the BTree just wrote it; `with_chain_under_latch` pins+latches it.
    handle
        .pool()
        .with_chain_under_latch(root_page, b"alpha", LatchMode::Exclusive, |slot| {
            *slot = Some(resident_chain(0xAA));
        })
        .expect("install resident chain for alpha");
    handle
        .pool()
        .with_chain_under_latch(root_page, b"beta", LatchMode::Exclusive, |slot| {
            *slot = Some(resident_chain(0xBB));
        })
        .expect("install resident chain for beta");

    // Chain-carrying read (default store): snapshot must be Some.
    let (carry_image, carry_snap) = {
        let mut writes = StructuralPageWrites::default();
        let mut lifetime = AllocatorLifetimeBatch::default();
        let mut migrated = false;
        let store = StructuralBatchStore::new(
            BufferPoolPageStore::new(Arc::clone(&handle)),
            &mut writes,
            &mut lifetime,
            &mut migrated,
        );
        store.read_leaf(root_page).expect("chain-carrying read")
    };
    assert!(
        carry_snap.is_some(),
        "chain-carrying read must snapshot resident chains"
    );

    // Chain-free read: snapshot must be None, image must be byte-identical.
    let (free_image, free_snap) = {
        let mut writes = StructuralPageWrites::default();
        let mut lifetime = AllocatorLifetimeBatch::default();
        let mut migrated = false;
        let store = StructuralBatchStore::new(
            BufferPoolPageStore::new(Arc::clone(&handle)),
            &mut writes,
            &mut lifetime,
            &mut migrated,
        )
        .with_chain_free_reads();
        store.read_leaf(root_page).expect("chain-free read")
    };
    assert!(
        free_snap.is_none(),
        "chain-free read must not snapshot resident chains"
    );
    assert_eq!(
        free_image.as_slice(),
        carry_image.as_slice(),
        "chain-free read must return the same page image as the chain-carrying read"
    );
}

/// (ii) A `replace_existing` + `delete` sequence through a chain-free store
/// produces byte-identical staged results to the same sequence through a
/// default store. The rebuild ops parse only base + staged page bytes; whether
/// the read clones resident chains or not must not change the staged output.
///
/// This exercises both `read_leaf` branches under the chain-free flag: the
/// base-fallback branch (first op) and the staged-bytes branch (second op,
/// which reads back the leaf staged by the first op).
#[test]
fn chain_free_replace_then_delete_staged_bytes_match_default() {
    let handle = make_handle();

    // Seed a base leaf with two keys via a BTree over the base store.
    let root_page = {
        let mut tree = BTree::create(BufferPoolPageStore::new(Arc::clone(&handle)))
            .expect("create base btree");
        tree.insert(b"alpha", b"one").expect("insert alpha");
        tree.insert(b"beta", b"two").expect("insert beta");
        tree.root_page
    };

    // Install resident chains so the chain-carrying path has real work to skip.
    // Seed BOTH keys: alpha is hit by replace_existing and beta by delete, so
    // each op's read exercises the skipped-dead-work coverage on a key that
    // actually carries a resident chain.
    handle
        .pool()
        .with_chain_under_latch(root_page, b"alpha", LatchMode::Exclusive, |slot| {
            *slot = Some(resident_chain(0xAA));
        })
        .expect("install resident chain for alpha");
    handle
        .pool()
        .with_chain_under_latch(root_page, b"beta", LatchMode::Exclusive, |slot| {
            *slot = Some(resident_chain(0xBB));
        })
        .expect("install resident chain for beta");

    // Run replace_existing + delete through the DEFAULT (chain-carrying) store.
    let default_staged: std::collections::HashMap<u32, Box<[u8; LEAF_SIZE]>> = {
        let mut writes = StructuralPageWrites::default();
        let mut lifetime = AllocatorLifetimeBatch::default();
        let mut migrated = false;
        let store = StructuralBatchStore::new(
            BufferPoolPageStore::new(Arc::clone(&handle)),
            &mut writes,
            &mut lifetime,
            &mut migrated,
        );
        let mut tree = BTree::open(store, root_page, 0);
        assert!(
            tree.replace_existing(b"alpha", b"replaced")
                .expect("replace alpha"),
            "alpha must exist for replace"
        );
        assert!(tree.delete(b"beta").expect("delete beta"), "beta must exist");
        drop(tree);
        writes.staged_32k.clone()
    };

    // Run the identical sequence through the CHAIN-FREE store.
    let chain_free_staged: std::collections::HashMap<u32, Box<[u8; LEAF_SIZE]>> = {
        let mut writes = StructuralPageWrites::default();
        let mut lifetime = AllocatorLifetimeBatch::default();
        let mut migrated = false;
        let store = StructuralBatchStore::new(
            BufferPoolPageStore::new(Arc::clone(&handle)),
            &mut writes,
            &mut lifetime,
            &mut migrated,
        )
        .with_chain_free_reads();
        let mut tree = BTree::open(store, root_page, 0);
        assert!(
            tree.replace_existing(b"alpha", b"replaced")
                .expect("replace alpha"),
            "alpha must exist for replace"
        );
        assert!(tree.delete(b"beta").expect("delete beta"), "beta must exist");
        drop(tree);
        writes.staged_32k.clone()
    };

    assert_eq!(
        default_staged, chain_free_staged,
        "chain-free replace+delete must stage byte-identical leaf images to the default store"
    );
}
