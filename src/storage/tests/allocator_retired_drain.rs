//! `AllocatorHandle` drain-split and retired-overflow-note tests (F8 / F9).
//!
//! - Hot drains (`drain_free_queue`) must never release `RetiredTree*`
//!   entries; only the checkpoint drain (`drain_free_queue_with_retired`)
//!   may, gated on the fence AND the installed reader floor.
//! - A pending retired-overflow note makes the final decref's enqueue
//!   inherit the drop's reader fence (`RetiredTree32k`) instead of pushing
//!   a fence-less `OverflowDeferredFree` entry.

use super::*;
use crate::mvcc::timestamp::Ts;

fn ts(ms: u64) -> Ts {
    Ts {
        physical_ms: ms,
        logical: 0,
    }
}

fn handle_with_pages(total_page_count: u32) -> AllocatorHandle {
    let mut hdr = FileHeader::new(0, 0, 0);
    hdr.total_page_count = total_page_count;
    AllocatorHandle::new(hdr)
}

struct NullIo;

impl PageSource for NullIo {
    fn read_page(&self, _page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        assert_eq!(buf.len(), size.bytes());
        buf.fill(0);
        Ok(())
    }

    fn write_page(&self, _page_number: u32, _size: PageSize, _buf: &[u8]) -> Result<()> {
        Ok(())
    }
}

#[test]
fn hot_drain_never_releases_retired_entries() {
    let io = NullIo;
    let handle = handle_with_pages(8);
    handle
        .install_retired_page_reader_floor(|| Ts::MAX)
        .expect("install floor");

    handle.enqueue_retired_tree_page(2, PageSize::Small4k, ts(10));
    handle.enqueue_retired_tree_page(3, PageSize::Large32k, ts(10));
    handle.advance_page_lifetime_checkpoint_fence();

    // Fence passed AND floor clear — the hot drain must still not touch them.
    let freed = handle.drain_free_queue(&io).expect("hot drain");
    assert_eq!(freed, 0, "hot drain must never release retired entries");
    assert_eq!(handle.page_lifetime_queue().depth(), 2);
    assert_eq!(handle.with_header(|h| h.free_list_head_4k).unwrap(), 0);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 0);

    // The checkpoint drain releases them.
    let freed = handle
        .drain_free_queue_with_retired(&io)
        .expect("checkpoint drain");
    assert_eq!(freed, 2);
    assert_eq!(handle.page_lifetime_queue().depth(), 0);
    assert_eq!(handle.with_header(|h| h.free_list_head_4k).unwrap(), 2);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 3);
}

#[test]
fn checkpoint_drain_blocks_retired_on_reader_floor() {
    let io = NullIo;
    let handle = handle_with_pages(8);
    handle
        .install_retired_page_reader_floor(|| ts(9))
        .expect("install floor");

    handle.enqueue_retired_tree_page(4, PageSize::Large32k, ts(10));
    handle.advance_page_lifetime_checkpoint_fence();

    let freed = handle
        .drain_free_queue_with_retired(&io)
        .expect("checkpoint drain");
    assert_eq!(freed, 0, "a reader below the drop fence must block the release");
    assert_eq!(handle.page_lifetime_queue().depth(), 1);
}

#[test]
fn checkpoint_drain_keeps_retired_without_floor_provider() {
    let io = NullIo;
    let handle = handle_with_pages(8);

    handle.enqueue_retired_tree_page(4, PageSize::Large32k, ts(10));
    handle.advance_page_lifetime_checkpoint_fence();

    let freed = handle
        .drain_free_queue_with_retired(&io)
        .expect("checkpoint drain");
    assert_eq!(
        freed, 0,
        "without a reader-floor provider retired entries must stay queued"
    );
    assert_eq!(handle.page_lifetime_queue().depth(), 1);
}

#[test]
fn checkpoint_drain_still_releases_overflow_entries() {
    let io = NullIo;
    let handle = handle_with_pages(8);

    handle.incref_overflow(5).unwrap();
    assert_eq!(handle.decref_overflow(5), 0);
    handle.enqueue_overflow_deferred_free(5);
    handle.advance_page_lifetime_checkpoint_fence();

    let freed = handle
        .drain_free_queue_with_retired(&io)
        .expect("checkpoint drain");
    assert_eq!(freed, 1);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 5);
}

#[test]
fn final_decref_enqueue_inherits_pending_retired_fence() {
    let io = NullIo;
    let handle = handle_with_pages(8);
    handle
        .install_retired_page_reader_floor(|| ts(9))
        .expect("install floor");

    // Drop's retire walk: refcount still positive -> note only.
    handle.incref_overflow(6).unwrap();
    handle.note_retired_overflow_pending(6, ts(10));

    // Final RAII decref consumes the note: the entry must be RetiredTree32k
    // with the drop's fence, not a fence-less overflow entry.
    assert_eq!(handle.decref_overflow(6), 0);
    handle.enqueue_overflow_deferred_free(6);
    assert_eq!(handle.page_lifetime_queue().depth(), 1);
    assert_eq!(
        handle.take_retired_overflow_pending(6),
        None,
        "the enqueue must consume the note"
    );

    handle.advance_page_lifetime_checkpoint_fence();

    // Hot drain: fence passed, but a retired entry never surfaces here.
    let freed = handle.drain_free_queue(&io).expect("hot drain");
    assert_eq!(freed, 0, "inherited-fence entry must be checkpoint-owned");

    // Checkpoint drain with the floor below the drop fence: still blocked.
    let freed = handle
        .drain_free_queue_with_retired(&io)
        .expect("checkpoint drain (blocked)");
    assert_eq!(freed, 0, "pre-drop reader floor must block the release");

    // Floor clears: released.
    handle
        .install_retired_page_reader_floor(|| Ts::MAX)
        .expect("reinstall floor");
    let freed = handle
        .drain_free_queue_with_retired(&io)
        .expect("checkpoint drain (clear)");
    assert_eq!(freed, 1);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 6);
}

#[test]
fn enqueue_without_pending_note_stays_fence_only() {
    let io = NullIo;
    let handle = handle_with_pages(8);

    handle.incref_overflow(7).unwrap();
    assert_eq!(handle.decref_overflow(7), 0);
    handle.enqueue_overflow_deferred_free(7);
    handle.advance_page_lifetime_checkpoint_fence();

    // No note -> plain overflow entry, released by the hot drain on the
    // fence alone (no reader floor involved).
    let freed = handle.drain_free_queue(&io).expect("hot drain");
    assert_eq!(freed, 1);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 7);
}

#[test]
fn retired_overflow_note_roundtrip() {
    let handle = handle_with_pages(8);
    assert_eq!(handle.take_retired_overflow_pending(9), None);

    handle.note_retired_overflow_pending(9, ts(42));
    assert_eq!(handle.take_retired_overflow_pending(9), Some(ts(42)));
    assert_eq!(
        handle.take_retired_overflow_pending(9),
        None,
        "a note is single-consume"
    );

    // Re-noting the same page replaces the fence rather than double-counting.
    handle.note_retired_overflow_pending(9, ts(1));
    handle.note_retired_overflow_pending(9, ts(2));
    assert_eq!(handle.take_retired_overflow_pending(9), Some(ts(2)));
    assert_eq!(handle.take_retired_overflow_pending(9), None);
}
