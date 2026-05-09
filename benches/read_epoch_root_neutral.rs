//! Root-neutral CRUD bench.
//!
//! Exercises 1/2/4 concurrent writers on a single already-bootstrapped
//! namespace performing root-neutral CRUD. Compared against the
//! baseline the publish-path CPU should drop proportionally
//! to the rebuild-elision rate (catalog Arc reused on root-neutral
//! commits, §4.1 / §10.3).
//!
//! Run:
//!   cargo bench --bench read_epoch_root_neutral -- --save-baseline current
//!   cargo bench --bench read_epoch_root_neutral -- --baseline current

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use bson::doc;
use bson::Document;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mqlite::mvcc::metrics::{
    published_catalog_rebuild_count_snapshot, read_epoch_publish_count_snapshot,
    reset_published_catalog_rebuild_count, reset_read_epoch_publish_count,
};
use tempfile::TempDir;

mod common;

const PAYLOAD_BYTES: usize = 256;
const DOCS_PER_WRITER: usize = 20;

fn metadata(writer_count: usize) -> String {
    format!(
        "writers={writer_count} ns=1 payload=~256B bytes={PAYLOAD_BYTES} \
         durability={} {}",
        common::INTERVAL_100MS_LABEL,
        common::host_metadata()
    )
}

fn bench_root_neutral(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_epoch_root_neutral");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for writer_count in [1usize, 2, 4] {
        let total_docs = (DOCS_PER_WRITER * writer_count) as u64;
        group.throughput(Throughput::Elements(total_docs));

        let param_str = format!("writers={writer_count}");
        let id = BenchmarkId::from_parameter(&param_str);

        eprintln!("[read_epoch_root_neutral] {}", metadata(writer_count));

        group.bench_with_input(id, &writer_count, |b, &wc| {
            let dir = TempDir::new().expect("tempdir");
            let client = common::open_interval_client(&dir);

            // Pre-create the collection so the first iteration is
            // already on the root-neutral hot path (no bootstrap).
            {
                let col = client.database("bench").collection::<Document>("ns0");
                col.insert_one(&doc! { "_id": -1i32, "init": true })
                    .expect("init insert");
            }

            let payload = "x".repeat(PAYLOAD_BYTES);

            // Reset counters once per sample to get a per-iteration
            // rebuild-elision rate witness. Counters are process-global
            // atomics; Criterion sample loops are single-threaded here
            // at the macro level.
            reset_read_epoch_publish_count();
            reset_published_catalog_rebuild_count();

            b.iter(|| {
                let barrier = Arc::new(Barrier::new(wc));
                let handles: Vec<_> = (0..wc)
                    .map(|t| {
                        let c = client.clone();
                        let b = barrier.clone();
                        let p = payload.clone();
                        thread::spawn(move || {
                            b.wait();
                            let col = c.database("bench").collection::<Document>("ns0");
                            for i in 0..DOCS_PER_WRITER as i32 {
                                col.insert_one(&doc! {
                                    "writer": t as i32,
                                    "seq": i,
                                    "payload": &p,
                                })
                                .expect("insert must not fail");
                            }
                        })
                    })
                    .collect();
                for h in handles {
                    h.join().expect("thread panicked");
                }
            });

            let publishes = read_epoch_publish_count_snapshot();
            let rebuilds = published_catalog_rebuild_count_snapshot();
            let elision_pct = if publishes > 0 {
                100.0 - (rebuilds as f64 * 100.0 / publishes as f64)
            } else {
                0.0
            };
            eprintln!(
                "[read_epoch_root_neutral counters] writers={wc} publishes={publishes} \
                 rebuilds={rebuilds} elision_pct={:.2}%",
                elision_pct
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_root_neutral);
criterion_main!(benches);
