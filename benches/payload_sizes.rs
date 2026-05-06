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
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

/// Nominal size classes (bytes).  The actual payload string will be exactly
/// this many ASCII 'x' characters, so the BSON document is slightly larger.
const SIZE_CLASSES: &[(&str, usize)] = &[("~256B", 230), ("~4KiB", 4_000), ("~32KiB", 32_000)];

const DOCS_PER_ITER: usize = 10;
const WRITER_COUNT: usize = 1;

fn metadata(size_class: &str, actual_bytes: usize) -> String {
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|_| "unknown".to_owned());

    let cpu_count = num_cpus();
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    let durability = "Interval(100ms)";

    format!(
        "writers={WRITER_COUNT} payload_class={size_class} actual_bytes={actual_bytes} \
         durability={durability} rustc=\"{rustc}\" cpu_count={cpu_count} \
         arch={arch} os={os}"
    )
}

fn num_cpus() -> usize {
    std::process::Command::new("sh")
        .arg("-c")
        .arg("nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 1")
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(1)
}

fn open_client(dir: &TempDir) -> Client {
    let path = dir.path().join("bench.mqlite");
    let opts = OpenOptions::new().durability(DurabilityMode::Interval(Duration::from_millis(100)));
    Client::open_with_options(&path, opts).expect("open must succeed")
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
            let client = open_client(&dir);
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
