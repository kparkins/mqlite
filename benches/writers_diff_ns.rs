//! Different-namespace writers (2, 4 writers across distinct namespaces).
//!
//! Writers on separate namespaces do NOT share a lane, so lane contention is
//! near-zero. The remaining serialization point is the global `journal_mutex`.
//! This isolates journal-envelope overhead from lane overhead.
//!
//! Run:
//!   cargo bench --bench writers_diff_ns -- --save-baseline current

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

const PAYLOAD_BYTES: usize = 256;
const DOCS_PER_WRITER: usize = 20;

fn metadata(writer_count: usize) -> String {
    let payload_class = "~256B";

    format!(
        "writers={writer_count} ns_count={writer_count} payload={payload_class} \
         bytes={PAYLOAD_BYTES} durability={} {}",
        common::INTERVAL_100MS_LABEL,
        common::host_metadata()
    )
}

fn bench_writers_diff_ns(c: &mut Criterion) {
    let mut group = c.benchmark_group("writers_diff_ns");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for writer_count in [2usize, 4] {
        let total_docs = (DOCS_PER_WRITER * writer_count) as u64;
        group.throughput(Throughput::Elements(total_docs));

        let param_str = format!("writers={writer_count}_ns={writer_count}");
        let id = BenchmarkId::from_parameter(&param_str);

        eprintln!("[writers_diff_ns] {}", metadata(writer_count));

        group.bench_with_input(id, &writer_count, |b, &wc| {
            let dir = TempDir::new().expect("tempdir");
            let client = common::open_interval_client(&dir);

            // Pre-create one collection per namespace.
            for ns in 0..wc {
                let col = client
                    .database("bench")
                    .collection::<Document>(&format!("ns{ns}"));
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
                            // Each writer owns its own namespace — no lane sharing.
                            let col = c
                                .database("bench")
                                .collection::<Document>(&format!("ns{t}"));
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

criterion_group!(benches, bench_writers_diff_ns);
criterion_main!(benches);
