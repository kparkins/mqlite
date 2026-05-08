use super::*;
use std::collections::VecDeque;

use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};

struct ZeroIo;

impl PageSource for ZeroIo {
    fn read_page(&self, _page_number: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
        buf.fill(0);
        Ok(())
    }

    fn write_page(&self, _page_number: u32, _size: PageSize, _buf: &[u8]) -> Result<()> {
        Ok(())
    }
}

fn entry(payload: u8) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: payload as u64,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: payload as u64,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![payload]),
        is_tombstone: false,
    }
}

fn chain(payload: u8) -> Arc<VecDeque<VersionEntry>> {
    let mut entries = VecDeque::new();
    entries.push_front(entry(payload));
    Arc::new(entries)
}

#[test]
fn test_delta_map_iterates_in_key_order() {
    let pool = BufferPool::new(PageSize::Large32k.bytes() * 4, Box::new(ZeroIo));
    let page_number = 42;
    drop(pool.pin(page_number, PageSize::Large32k).unwrap());

    for key in [b"m".as_slice(), b"a".as_slice(), b"k".as_slice()] {
        pool.put_chain(page_number, key.to_vec(), chain(key[0]))
            .unwrap();
    }

    let guard = pool.inner_32k.lock().unwrap();
    let frame_index = *guard.page_map.get(&page_number).unwrap();
    let frame = guard.frames[frame_index].as_ref().unwrap();
    let keys = frame.deltas.keys().cloned().collect::<Vec<_>>();

    assert_eq!(keys, vec![b"a".to_vec(), b"k".to_vec(), b"m".to_vec()]);
    assert!(frame.deltas.values().all(|entries| !entries.is_empty()));
    assert!(frame
        .deltas
        .values()
        .all(|entries| entries.front().is_some()));
}
