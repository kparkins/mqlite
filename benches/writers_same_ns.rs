//! Same-namespace writers (1, 2, 4 concurrent writers, one namespace).
//!
//! Captures the lane-bottleneck baseline: all writers serialize through a single
//! namespace lane, so contention scales with writer count.
//!
//! Run:
//!   cargo bench --bench writers_same_ns -- --save-baseline current

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
use tempfile::TempDir;

mod common;

/// Small but realistic payload (~256 B) for contention benchmarks.
const PAYLOAD_BYTES: usize = 256;
const DOCS_PER_WRITER: usize = 20;

/// Return metadata describing this machine shape and bench parameters.
fn metadata(writer_count: usize) -> String {
    let payload_class = "~256B";

    format!(
        "writers={writer_count} ns=1 payload={payload_class} bytes={PAYLOAD_BYTES} \
         durability={} {}",
        common::INTERVAL_100MS_LABEL,
        common::host_metadata()
    )
}

fn bench_writers_same_ns(c: &mut Criterion) {
    let mut group = c.benchmark_group("writers_same_ns");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    // Total docs inserted per iteration = DOCS_PER_WRITER * writer_count.
    for writer_count in [1usize, 2, 4] {
        let total_docs = (DOCS_PER_WRITER * writer_count) as u64;
        group.throughput(Throughput::Elements(total_docs));

        let param_str = format!("writers={writer_count}");
        let id = BenchmarkId::from_parameter(&param_str);

        // Print metadata once per parameter before the Criterion loop.
        eprintln!("[writers_same_ns] {}", metadata(writer_count));

        group.bench_with_input(id, &writer_count, |b, &wc| {
            let dir = TempDir::new().expect("tempdir");
            let client = common::open_interval_client(&dir);

            // Pre-create the collection so the first write doesn't include DDL cost.
            {
                let col = client.database("bench").collection::<Document>("ns0");
                col.insert_one(&doc! { "_id": -1i32, "init": true })
                    .expect("init insert");
            }

            let payload = "x".repeat(PAYLOAD_BYTES);

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
        });
    }

    group.finish();
}

criterion_group!(benches, bench_writers_same_ns);
criterion_main!(benches);
