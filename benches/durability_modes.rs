//! Same workload under FullSync and Interval(100ms) durability modes.
//!
//! Fixed writer shape: 1 writer, 1 namespace, ~256B payload.
//! This isolates the fsync cost from all other variables.
//!
//! Run:
//!   cargo bench --bench durability_modes -- --save-baseline current

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
use mqlite::DurabilityMode;
use tempfile::TempDir;

mod common;

const PAYLOAD_BYTES: usize = 230;
const DOCS_PER_ITER: usize = 10;
const WRITER_COUNT: usize = 1;
const PAYLOAD_CLASS: &str = "~256B";

fn metadata(durability: &str) -> String {
    format!(
        "durability={durability} writers={WRITER_COUNT} payload_class={PAYLOAD_CLASS} \
         actual_bytes={PAYLOAD_BYTES} {}",
        common::host_metadata()
    )
}

fn bench_durability_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("durability_modes");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(DOCS_PER_ITER as u64));

    let cases: &[(&str, DurabilityMode)] = &[
        ("FullSync", DurabilityMode::FullSync),
        (
            "Interval(100ms)",
            DurabilityMode::Interval(Duration::from_millis(100)),
        ),
    ];

    for (label, mode) in cases {
        let id = BenchmarkId::from_parameter(label);

        eprintln!("[durability_modes] {}", metadata(label));

        group.bench_with_input(id, label, |b, _label| {
            let dir = TempDir::new().expect("tempdir");
            let client = common::open_client(&dir.path().join("bench.mqlite"), mode.clone());
            let col = client
                .database("bench")
                .collection::<Document>("durability_col");
            // Pre-create namespace.
            col.insert_one(&doc! { "_id": -1i32, "init": true })
                .expect("init insert");

            let payload = "x".repeat(PAYLOAD_BYTES);

            b.iter(|| {
                for i in 0..DOCS_PER_ITER as i32 {
                    col.insert_one(&doc! { "seq": i, "payload": &payload })
                        .expect("insert must not fail");
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_durability_modes);
criterion_main!(benches);
