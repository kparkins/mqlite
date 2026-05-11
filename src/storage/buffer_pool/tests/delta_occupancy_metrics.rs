#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use crate::mvcc::metrics;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};

const PAGE_A: u32 = 201;
const PAGE_B: u32 = 202;
const PAGE_C: u32 = 203;
const KEY_A: &[u8] = b"delta-a";
const KEY_B: &[u8] = b"delta-b";
const KEY_C: &[u8] = b"delta-c";
const LARGE_PAGE_BYTES: usize = 32 * 1024;
const POOL_BYTES: usize = LARGE_PAGE_BYTES * 4;
const EPSILON: f64 = 0.000_001;

struct ZeroIo;

impl PageSource for ZeroIo {
    fn read_page(&self, _page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        assert_eq!(buf.len(), size.bytes());
        buf.fill(0);
        Ok(())
    }

    fn write_page(&self, _page_number: u32, _size: PageSize, _buf: &[u8]) -> Result<()> {
        Ok(())
    }
}

fn ts(physical_ms: u64) -> Ts {
    Ts {
        physical_ms,
        logical: 0,
    }
}

fn entry(state: VersionState, tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: ts(10),
        stop_ts: Ts::MAX,
        txn_id: 1,
        state,
        data: VersionData::Inline(Vec::from(&b"value"[..])),
        is_tombstone: tombstone,
    }
}

fn committed_head() -> VersionEntry {
    entry(VersionState::Committed, false)
}

fn committed_tombstone_head() -> VersionEntry {
    entry(VersionState::Committed, true)
}

fn pending_head() -> VersionEntry {
    entry(VersionState::Pending { txn_id: 1 }, false)
}

fn load_and_unpin(pool: &BufferPool, page: u32) {
    drop(pool.pin(page, PageSize::Large32k).unwrap());
}

fn install_chain(pool: &BufferPool, page: u32, key: &[u8], entry: VersionEntry) {
    pool.with_chain_under_latch(page, key, LatchMode::Exclusive, |slot| {
        *slot = Some(Arc::new(VecDeque::from([entry])));
    })
    .unwrap();
}

fn install_two_delta_bearing_frames(pool: &BufferPool) {
    install_chain(pool, PAGE_A, KEY_A, committed_head());
    install_chain(pool, PAGE_B, KEY_B, committed_tombstone_head());
}

#[test]
fn occupancy_snapshot_records_delta_bearing_frames_and_ratio() {
    let pool = BufferPool::new(POOL_BYTES, Box::new(ZeroIo));
    for page in [PAGE_A, PAGE_B, PAGE_C] {
        load_and_unpin(&pool, page);
    }
    install_two_delta_bearing_frames(&pool);
    install_chain(&pool, PAGE_C, KEY_C, pending_head());

    metrics::reset_delta_bearing_frames_count();
    metrics::reset_delta_bearing_frames_ratio();
    assert_eq!(metrics::delta_bearing_frames_count_snapshot(), 0);

    let snapshot = pool.occupancy_snapshot().unwrap();
    let expected_count = 2u64;
    let expected_ratio = expected_count as f64 / snapshot.total_pool_frames as f64;

    assert_eq!(snapshot.delta_bearing_frames_count, expected_count);
    assert_eq!(
        metrics::delta_bearing_frames_count_snapshot(),
        expected_count
    );
    assert!(
        (snapshot.delta_bearing_frames_ratio - expected_ratio).abs() < EPSILON,
        "expected ratio {expected_ratio}, got {}",
        snapshot.delta_bearing_frames_ratio
    );
    assert!(
        (metrics::delta_bearing_frames_ratio_snapshot() - expected_ratio).abs() < EPSILON,
        "metric ratio must match occupancy snapshot"
    );
}

#[cfg(feature = "tracing")]
mod tracing_tests {
    use super::*;

    use std::fmt;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Level, Metadata, Subscriber};

    const WARN_THRESHOLD: f64 = 0.01;
    const EXPECTED_WARNINGS: usize = 2;

    #[derive(Clone, Debug, Default)]
    struct WarningFields {
        count: Option<u64>,
        total: Option<u64>,
        ratio: Option<f64>,
    }

    #[derive(Clone)]
    struct WarningCapture {
        warnings: Arc<Mutex<Vec<WarningFields>>>,
    }

    impl Subscriber for WarningCapture {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() == Level::WARN && metadata.target() == "mqlite"
        }

        fn new_span(&self, _span: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            if *event.metadata().level() != Level::WARN || event.metadata().target() != "mqlite" {
                return;
            }
            let mut fields = WarningFields::default();
            event.record(&mut fields);
            self.warnings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(fields);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    impl Visit for WarningFields {
        fn record_u64(&mut self, field: &Field, value: u64) {
            match field.name() {
                "delta_bearing_frames_count" => self.count = Some(value),
                "total_pool_frames" => self.total = Some(value),
                _ => {}
            }
        }

        fn record_f64(&mut self, field: &Field, value: f64) {
            if field.name() == "delta_bearing_frames_ratio" {
                self.ratio = Some(value);
            }
        }

        fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
    }

    #[test]
    fn warn_log_emits_once_per_threshold_crossing() {
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let subscriber = WarningCapture {
            warnings: Arc::clone(&warnings),
        };
        let pool = BufferPool::new_with_delta_bearing_frames_warn_threshold(
            POOL_BYTES,
            Box::new(ZeroIo),
            WARN_THRESHOLD,
        );
        for page in [PAGE_A, PAGE_B] {
            load_and_unpin(&pool, page);
        }
        install_two_delta_bearing_frames(&pool);

        tracing::subscriber::with_default(subscriber, || {
            pool.occupancy_snapshot().unwrap();
            pool.occupancy_snapshot().unwrap();

            pool.with_all_chains_under_latch(PAGE_A, LatchMode::Exclusive, |c| c.clear())
                .unwrap();
            pool.with_all_chains_under_latch(PAGE_B, LatchMode::Exclusive, |c| c.clear())
                .unwrap();
            pool.occupancy_snapshot().unwrap();

            install_two_delta_bearing_frames(&pool);
            pool.occupancy_snapshot().unwrap();
        });

        let captured = warnings.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(
            captured.len(),
            EXPECTED_WARNINGS,
            "warning must fire only on low-to-high threshold crossings"
        );
        for event in captured.iter() {
            assert_eq!(event.count, Some(2));
            assert!(event.total.is_some());
            assert!(event.ratio.unwrap_or(0.0) >= WARN_THRESHOLD);
        }
    }
}
