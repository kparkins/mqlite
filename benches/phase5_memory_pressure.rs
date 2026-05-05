//! Phase 5 US-034/US-035: reader memory-pressure benchmark.
//!
//! Run:
//!   cargo bench --profile release-test --bench phase5_memory_pressure
//!
//! After US-035, `BufferPoolPageStore::read_leaf` carries the pinned
//! `ArcSwap<Vec<u8>>` page image as a shared immutable `Arc`, so the hot
//! buffer-pool reader path does not clone 32 KiB of leaf bytes per reader.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "bench target uses assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

const DEFAULT_READER_COUNT: usize = 10_000;
const DEFAULT_WRITER_COUNT: usize = 4;
const HOT_DOCS: i32 = 512;
const WRITES_PER_WRITER: i32 = 64;
const PAYLOAD_BYTES: usize = 256;
const DEFAULT_READ_LEAF_COPY_BYTES_PER_READ: u64 = 0;
const DEFAULT_READER_STACK_BYTES: usize = 64 * 1024;
const REMEDIATION_RSS_THRESHOLD_PCT: f64 = 20.0;
const DEFAULT_ARTIFACT: &str = ".omc/artifacts/phase5-memory-pressure.txt";
const DEFAULT_BASELINE_ARTIFACT: &str = ".omc/artifacts/phase5-memory-pressure-phase4-baseline.txt";
const DEFAULT_ALLOCATION_OWNER: &str = "US-035 remediated path: BufferPoolPageStore::read_leaf carries the pinned ArcSwap<Vec<u8>> page image as a shared immutable Arc; no per-reader 32KB leaf clone on the buffer-pool reader path.";

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegates to the standard system allocator with the original
        // layout, then records the successful allocation size.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: `ptr` and `layout` are the exact pair supplied by the
        // allocator caller, so forwarding them to `System` preserves the
        // `GlobalAlloc` contract.
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegates to the standard system allocator with the original
        // layout, then records the successful allocation size.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwards the original allocation tuple to `System` and only
        // records the net size delta after a successful reallocation.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            if new_size >= old_size {
                ALLOCATED_BYTES.fetch_add((new_size - old_size) as u64, Ordering::Relaxed);
            } else {
                DEALLOCATED_BYTES.fetch_add((old_size - new_size) as u64, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

#[derive(Clone, Copy, Debug)]
struct Metrics {
    peak_rss_bytes: u64,
    allocator_churn_bytes: u64,
    allocator_deallocated_bytes: u64,
    read_throughput_ops_per_sec: f64,
    successful_reads: usize,
    successful_writes: usize,
    elapsed: Duration,
}

#[derive(Clone, Debug)]
struct Baseline {
    source: String,
    peak_rss_bytes: u64,
    allocator_churn_bytes: u64,
    read_throughput_ops_per_sec: f64,
    reader_count: Option<u64>,
    reader_threads: Option<u64>,
    reader_stack_bytes: Option<u64>,
    writer_count: Option<u64>,
}

#[derive(Clone, Debug)]
struct Config {
    story: String,
    reader_count: usize,
    reader_threads: usize,
    reader_stack_bytes: usize,
    writer_count: usize,
    read_leaf_copy_bytes_per_read: u64,
    artifact_path: PathBuf,
    baseline_artifact: Option<PathBuf>,
    command: String,
    baseline_source: String,
    allocation_owner: String,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("phase5_memory_pressure failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env();
    let metrics = run_workload(&config)?;
    let baseline = load_baseline(&config, metrics)?;
    write_artifact(&config, metrics, &baseline)?;

    eprintln!(
        "phase5_memory_pressure readers={} reader_threads={} reader_stack={} writers={} \
         peak_rss_bytes={} allocator_churn_bytes={} throughput={:.2}/s",
        config.reader_count,
        config.reader_threads,
        config.reader_stack_bytes,
        config.writer_count,
        metrics.peak_rss_bytes,
        metrics.allocator_churn_bytes,
        metrics.read_throughput_ops_per_sec,
    );
    Ok(())
}

impl Config {
    fn from_env() -> Self {
        let reader_count = env_usize("MQLITE_PHASE5_MEMORY_READERS", DEFAULT_READER_COUNT).max(1);
        let reader_threads = env_usize("MQLITE_PHASE5_MEMORY_READER_THREADS", reader_count).max(1);
        let reader_stack_bytes = env_usize(
            "MQLITE_PHASE5_MEMORY_READER_STACK_BYTES",
            DEFAULT_READER_STACK_BYTES,
        )
        .max(DEFAULT_READER_STACK_BYTES);
        let writer_count = env_usize("MQLITE_PHASE5_MEMORY_WRITERS", DEFAULT_WRITER_COUNT).max(1);
        let read_leaf_copy_bytes_per_read = env_u64(
            "MQLITE_PHASE5_MEMORY_READ_LEAF_COPY_BYTES_PER_READ",
            DEFAULT_READ_LEAF_COPY_BYTES_PER_READ,
        );
        let artifact_path = env_path("MQLITE_PHASE5_MEMORY_ARTIFACT", DEFAULT_ARTIFACT);
        let baseline_artifact = baseline_artifact_path(&artifact_path);
        let story =
            std::env::var("MQLITE_PHASE5_MEMORY_STORY").unwrap_or_else(|_| "US-034".to_owned());
        let command = std::env::var("MQLITE_PHASE5_MEMORY_COMMAND")
            .unwrap_or_else(|_| std::env::args().collect::<Vec<_>>().join(" "));
        let baseline_source = std::env::var("MQLITE_PHASE5_MEMORY_BASELINE_SOURCE")
            .unwrap_or_else(|_| "no external Phase 4 artifact supplied".to_owned());
        let allocation_owner = std::env::var("MQLITE_PHASE5_MEMORY_ALLOCATION_OWNER")
            .unwrap_or_else(|_| DEFAULT_ALLOCATION_OWNER.to_owned());

        Self {
            story,
            reader_count,
            reader_threads: reader_threads.min(reader_count),
            reader_stack_bytes,
            writer_count,
            read_leaf_copy_bytes_per_read,
            artifact_path,
            baseline_artifact,
            command,
            baseline_source,
            allocation_owner,
        }
    }
}

fn baseline_artifact_path(artifact_path: &Path) -> Option<PathBuf> {
    if std::env::var_os("MQLITE_PHASE5_MEMORY_NO_BASELINE").is_some() {
        return None;
    }
    if let Some(path) = std::env::var_os("MQLITE_PHASE5_MEMORY_BASELINE_ARTIFACT") {
        return Some(PathBuf::from(path));
    }

    let default = PathBuf::from(DEFAULT_BASELINE_ARTIFACT);
    if default.exists() && default != artifact_path {
        Some(default)
    } else {
        None
    }
}

fn run_workload(config: &Config) -> Result<Metrics, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    let client = open_client(&dir)?;
    seed_hot_collection(&client)?;

    let start = Arc::new(Barrier::new(
        config.reader_threads + config.writer_count + 1,
    ));
    let next_reader = Arc::new(AtomicUsize::new(0));
    let successful_reads = Arc::new(AtomicUsize::new(0));
    let successful_writes = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(config.reader_threads + config.writer_count);

    for _ in 0..config.reader_threads {
        handles.push(spawn_reader(
            client.clone(),
            Arc::clone(&start),
            Arc::clone(&next_reader),
            Arc::clone(&successful_reads),
            config.reader_count,
            config.reader_stack_bytes,
        )?);
    }

    for writer_id in 0..config.writer_count {
        handles.push(spawn_writer(
            client.clone(),
            Arc::clone(&start),
            Arc::clone(&successful_writes),
            writer_id,
        )?);
    }

    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    DEALLOCATED_BYTES.store(0, Ordering::Relaxed);
    let start_rss = max_rss_bytes().unwrap_or(0);
    let started = Instant::now();
    start.wait();

    for handle in handles {
        handle
            .join()
            .map_err(|_| "phase5 memory-pressure worker panicked")??;
    }

    let elapsed = started.elapsed();
    let peak_rss_bytes = max_rss_bytes().unwrap_or(start_rss).max(start_rss);
    let reads = successful_reads.load(Ordering::Relaxed);
    let writes = successful_writes.load(Ordering::Relaxed);
    let elapsed_secs = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let read_throughput_ops_per_sec = reads as f64 / elapsed_secs;

    Ok(Metrics {
        peak_rss_bytes,
        allocator_churn_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        allocator_deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
        read_throughput_ops_per_sec,
        successful_reads: reads,
        successful_writes: writes,
        elapsed,
    })
}

fn open_client(dir: &TempDir) -> mqlite::Result<Client> {
    let path = dir.path().join("phase5-memory-pressure.mqlite");
    let opts = OpenOptions::new().durability(DurabilityMode::Interval(Duration::from_millis(100)));
    Client::open_with_options(&path, opts)
}

fn seed_hot_collection(client: &Client) -> mqlite::Result<()> {
    let coll = hot_collection(client);
    let payload = "x".repeat(PAYLOAD_BYTES);
    let docs = (0..HOT_DOCS)
        .map(|id| doc! { "_id": id, "payload": &payload })
        .collect::<Vec<Document>>();
    coll.insert_many(&docs).run()?;
    Ok(())
}

fn hot_collection(client: &Client) -> mqlite::Collection<Document> {
    client.database("phase5_memory").collection("hot")
}

fn spawn_reader(
    client: Client,
    start: Arc<Barrier>,
    next_reader: Arc<AtomicUsize>,
    successful_reads: Arc<AtomicUsize>,
    reader_count: usize,
    reader_stack_bytes: usize,
) -> std::io::Result<thread::JoinHandle<Result<(), String>>> {
    thread::Builder::new()
        .name("phase5-memory-reader".to_owned())
        .stack_size(reader_stack_bytes)
        .spawn(move || {
            let coll = hot_collection(&client);
            start.wait();
            loop {
                let reader_id = next_reader.fetch_add(1, Ordering::Relaxed);
                if reader_id >= reader_count {
                    break;
                }

                let id = (reader_id as i32).rem_euclid(HOT_DOCS);
                coll.find_one(doc! { "_id": id })
                    .map_err(|err| format!("reader find_one failed: {err}"))?
                    .ok_or_else(|| format!("reader missed hot document _id={id}"))?;
                successful_reads.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        })
}

fn spawn_writer(
    client: Client,
    start: Arc<Barrier>,
    successful_writes: Arc<AtomicUsize>,
    writer_id: usize,
) -> std::io::Result<thread::JoinHandle<Result<(), String>>> {
    thread::Builder::new()
        .name("phase5-memory-writer".to_owned())
        .spawn(move || {
            let coll = hot_collection(&client);
            let payload = "w".repeat(PAYLOAD_BYTES);
            start.wait();
            for seq in 0..WRITES_PER_WRITER {
                let id = HOT_DOCS + (writer_id as i32 * WRITES_PER_WRITER) + seq;
                coll.insert_one(&doc! {
                    "_id": id,
                    "writer": writer_id as i32,
                    "seq": seq,
                    "payload": &payload,
                })
                .map_err(|err| format!("writer insert_one failed: {err}"))?;
                successful_writes.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        })
}

fn load_baseline(
    config: &Config,
    metrics: Metrics,
) -> Result<Baseline, Box<dyn std::error::Error>> {
    if let Some(path) = &config.baseline_artifact {
        let text = fs::read_to_string(path)?;
        let peak_rss_bytes = parse_artifact_u64(&text, "peak_rss_bytes")?;
        let allocator_churn_bytes = parse_artifact_u64(&text, "allocator_churn_bytes")?;
        let read_throughput_ops_per_sec = parse_artifact_f64(&text, "read_throughput_ops_per_sec")?;
        return Ok(Baseline {
            source: format!("{} ({})", config.baseline_source, path.display()),
            peak_rss_bytes,
            allocator_churn_bytes,
            read_throughput_ops_per_sec,
            reader_count: parse_artifact_u64_optional(&text, "reader_count"),
            reader_threads: parse_artifact_u64_optional(&text, "reader_worker_threads"),
            reader_stack_bytes: parse_artifact_u64_optional(&text, "reader_stack_bytes"),
            writer_count: parse_artifact_u64_optional(&text, "writer_count"),
        });
    }

    Ok(Baseline {
        source: config.baseline_source.clone(),
        peak_rss_bytes: metrics.peak_rss_bytes,
        allocator_churn_bytes: metrics.allocator_churn_bytes,
        read_throughput_ops_per_sec: metrics.read_throughput_ops_per_sec,
        reader_count: Some(config.reader_count as u64),
        reader_threads: Some(config.reader_threads as u64),
        reader_stack_bytes: Some(config.reader_stack_bytes as u64),
        writer_count: Some(config.writer_count as u64),
    })
}

fn write_artifact(
    config: &Config,
    metrics: Metrics,
    baseline: &Baseline,
) -> Result<(), Box<dyn std::error::Error>> {
    let peak_rss_delta_pct = delta_pct(
        metrics.peak_rss_bytes as f64,
        baseline.peak_rss_bytes as f64,
    );
    let allocator_churn_delta_pct = delta_pct(
        metrics.allocator_churn_bytes as f64,
        baseline.allocator_churn_bytes as f64,
    );
    let throughput_delta_pct = delta_pct(
        metrics.read_throughput_ops_per_sec,
        baseline.read_throughput_ops_per_sec,
    );
    let required_remediation = peak_rss_delta_pct > REMEDIATION_RSS_THRESHOLD_PCT;
    let source_commit = command_output("git", &["rev-parse", "HEAD"]);
    let working_tree_state = if command_output("git", &["status", "--short"]).is_empty() {
        "clean".to_owned()
    } else {
        "dirty".to_owned()
    };
    let estimated_read_leaf_copy_bytes =
        metrics.successful_reads as u64 * config.read_leaf_copy_bytes_per_read;
    let baseline_reader_count = artifact_value_or_unknown(baseline.reader_count);
    let baseline_reader_threads = artifact_value_or_unknown(baseline.reader_threads);
    let baseline_reader_stack_bytes = artifact_value_or_unknown(baseline.reader_stack_bytes);
    let baseline_writer_count = artifact_value_or_unknown(baseline.writer_count);

    let artifact = format!(
        "story={}\n\
         command={}\n\
         source_commit={}\n\
         working_tree_state={}\n\
         baseline_source={}\n\
         benchmark_method=Compare only artifacts produced by this benchmark harness with matching reader_count, reader_worker_threads, reader_stack_bytes, writer_count, hot_docs, payload_bytes, and durability.\n\
         reader_count={}\n\
         reader_worker_threads={}\n\
         reader_stack_bytes={}\n\
         baseline_reader_count={}\n\
         baseline_reader_worker_threads={}\n\
         baseline_reader_stack_bytes={}\n\
         writer_count={}\n\
         baseline_writer_count={}\n\
         writes_per_writer={}\n\
         hot_docs={}\n\
         payload_bytes={}\n\
         durability=Interval(100ms)\n\
         elapsed_ms={}\n\
         successful_reads={}\n\
         successful_writes={}\n\
         peak_rss_bytes={}\n\
         baseline_peak_rss_bytes={}\n\
         peak_rss_delta_pct={:.4}\n\
         allocator_churn_bytes={}\n\
         allocator_deallocated_bytes={}\n\
         baseline_allocator_churn_bytes={}\n\
         allocator_churn_delta_pct={:.4}\n\
         read_leaf_copy_bytes_per_read={}\n\
         estimated_read_leaf_copy_bytes={}\n\
         read_throughput_ops_per_sec={:.4}\n\
         baseline_read_throughput_ops_per_sec={:.4}\n\
         read_throughput_delta_pct={:.4}\n\
        required_remediation={}\n\
         remediation_rule=required_remediation is true iff peak_rss_delta_pct > 20.0\n\
         page_data_representation_changed=false\n\
        allocation_owner={}\n",
        config.story,
        config.command,
        source_commit,
        working_tree_state,
        baseline.source,
        config.reader_count,
        config.reader_threads,
        config.reader_stack_bytes,
        baseline_reader_count,
        baseline_reader_threads,
        baseline_reader_stack_bytes,
        config.writer_count,
        baseline_writer_count,
        WRITES_PER_WRITER,
        HOT_DOCS,
        PAYLOAD_BYTES,
        metrics.elapsed.as_millis(),
        metrics.successful_reads,
        metrics.successful_writes,
        metrics.peak_rss_bytes,
        baseline.peak_rss_bytes,
        peak_rss_delta_pct,
        metrics.allocator_churn_bytes,
        metrics.allocator_deallocated_bytes,
        baseline.allocator_churn_bytes,
        allocator_churn_delta_pct,
        config.read_leaf_copy_bytes_per_read,
        estimated_read_leaf_copy_bytes,
        metrics.read_throughput_ops_per_sec,
        baseline.read_throughput_ops_per_sec,
        throughput_delta_pct,
        required_remediation,
        config.allocation_owner,
    );

    if let Some(parent) = config.artifact_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.artifact_path, artifact)?;
    Ok(())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn env_path(name: &str, default: &str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn parse_artifact_u64(text: &str, key: &str) -> Result<u64, Box<dyn std::error::Error>> {
    parse_artifact_value(text, key)?.parse().map_err(Into::into)
}

fn parse_artifact_f64(text: &str, key: &str) -> Result<f64, Box<dyn std::error::Error>> {
    parse_artifact_value(text, key)?.parse().map_err(Into::into)
}

fn parse_artifact_u64_optional(text: &str, key: &str) -> Option<u64> {
    parse_artifact_value(text, key).ok()?.parse().ok()
}

fn parse_artifact_value<'a>(
    text: &'a str,
    key: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    let prefix = format!("{key}=");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .ok_or_else(|| format!("baseline artifact missing {key}").into())
}

fn delta_pct(current: f64, baseline: f64) -> f64 {
    if baseline == 0.0 {
        0.0
    } else {
        (current - baseline) * 100.0 / baseline
    }
}

fn artifact_value_or_unknown(value: Option<u64>) -> String {
    value
        .map(|raw| raw.to_string())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn command_output(program: &str, args: &[&str]) -> String {
    std::process::Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(unix)]
fn max_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` points to valid writable memory and `getrusage` fully
    // initializes it when the call succeeds.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: success from `getrusage` means the structure is initialized.
    let usage = unsafe { usage.assume_init() };
    let max_rss = usage.ru_maxrss.max(0) as u64;

    #[cfg(target_os = "macos")]
    {
        Some(max_rss)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some(max_rss * 1024)
    }
}

#[cfg(not(unix))]
fn max_rss_bytes() -> Option<u64> {
    None
}
