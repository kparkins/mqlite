//! Insert throughput across payload size classes (~256B, ~4KiB, ~32KiB).
//!
//! Single writer, single namespace. Measures how serialized insert throughput
//! scales with document payload size. The actual byte count of each payload
//! class is printed so "approximately" is auditable.
//!
//! Run:
//!   cargo bench --bench payload_sizes -- --save-baseline current

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

use std::time::Duration;

use bson::doc;
use bson::Document;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;

mod common;

/// Nominal size classes (bytes).  The actual payload string will be exactly
/// this many ASCII 'x' characters, so the BSON document is slightly larger.
const SIZE_CLASSES: &[(&str, usize)] = &[("~256B", 230), ("~4KiB", 4_000), ("~32KiB", 32_000)];

const DOCS_PER_ITER: usize = 10;
const WRITER_COUNT: usize = 1;

fn metadata(size_class: &str, actual_bytes: usize) -> String {
    format!(
        "writers={WRITER_COUNT} payload_class={size_class} actual_bytes={actual_bytes} \
         durability={} {}",
        common::INTERVAL_100MS_LABEL,
        common::host_metadata()
    )
}

fn bench_payload_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_sizes");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    for (size_class, payload_bytes) in SIZE_CLASSES {
        let size_class = *size_class;
        let payload_bytes = *payload_bytes;

        group.throughput(Throughput::Bytes((payload_bytes * DOCS_PER_ITER) as u64));

        let param_str = format!("size={size_class}");
        let id = BenchmarkId::from_parameter(&param_str);

        eprintln!("[payload_sizes] {}", metadata(size_class, payload_bytes));

        group.bench_with_input(id, &payload_bytes, |b, &pb| {
            let dir = TempDir::new().expect("tempdir");
            let client = common::open_interval_client(&dir);
            let col = client
                .database("bench")
                .collection::<Document>("payload_col");
            // Pre-create namespace.
            col.insert_one(&doc! { "_id": -1i32, "init": true })
                .expect("init insert");

            let payload = "x".repeat(pb);

            b.iter(|| {
                for i in 0..DOCS_PER_ITER as i32 {
                    col.insert_one(&doc! {
                        "seq": i,
                        "payload": &payload,
                    })
                    .expect("insert must not fail");
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_payload_sizes);
criterion_main!(benches);
