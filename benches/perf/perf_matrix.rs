//! Canonical operation-scoped performance matrix for mqlite.
//!
//! The measured window starts after database setup, namespace creation, and
//! benchmark document generation. Timed work is limited to public API
//! operations on prebuilt `_id` primary-key documents.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "benchmark targets use assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};

const DATABASE_NAME: &str = "perf_matrix";
const SINGLE_NAMESPACE: &str = "docs";
const DEFAULT_MULTI_WRITERS: usize = 4;
const DEFAULT_DOCS_PER_WRITER: usize = 20_000;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_READ_SEED_DOCS: usize = 20_000;
const DEFAULT_READ_OPS: usize = 100_000;
const PAYLOAD_BYTES: usize = 256;
const DURABILITY_INTERVAL_MS: u64 = 100;
const KEY_BAND_WIDTH: i64 = 1_000_000_000;
const AXIS_REQUIRED_MSG: &str = "--axis required; use --list-axes to inspect choices";

type DynError = Box<dyn std::error::Error>;
type BenchResult<T> = Result<T, DynError>;

#[derive(Clone, Copy)]
struct AxisSpec {
    name: &'static str,
    default_writers: usize,
    namespaces: NamespaceShape,
    operation: Operation,
}

#[derive(Clone, Copy)]
enum NamespaceShape {
    Single,
    Multi,
}

#[derive(Clone, Copy)]
enum Operation {
    InsertOne,
    InsertMany,
    ReadFindOne,
}

const AXES: &[AxisSpec] = &[
    AxisSpec {
        name: "single_writer_single_ns_single",
        default_writers: 1,
        namespaces: NamespaceShape::Single,
        operation: Operation::InsertOne,
    },
    AxisSpec {
        name: "single_writer_single_ns_batch",
        default_writers: 1,
        namespaces: NamespaceShape::Single,
        operation: Operation::InsertMany,
    },
    AxisSpec {
        name: "multi_writer_single_ns_single",
        default_writers: DEFAULT_MULTI_WRITERS,
        namespaces: NamespaceShape::Single,
        operation: Operation::InsertOne,
    },
    AxisSpec {
        name: "multi_writer_single_ns_batch",
        default_writers: DEFAULT_MULTI_WRITERS,
        namespaces: NamespaceShape::Single,
        operation: Operation::InsertMany,
    },
    AxisSpec {
        name: "multi_writer_multi_ns_single",
        default_writers: DEFAULT_MULTI_WRITERS,
        namespaces: NamespaceShape::Multi,
        operation: Operation::InsertOne,
    },
    AxisSpec {
        name: "multi_writer_multi_ns_batch",
        default_writers: DEFAULT_MULTI_WRITERS,
        namespaces: NamespaceShape::Multi,
        operation: Operation::InsertMany,
    },
    AxisSpec {
        name: "read_find_one",
        default_writers: 1,
        namespaces: NamespaceShape::Single,
        operation: Operation::ReadFindOne,
    },
];

struct Config {
    axis: AxisSpec,
    writers: usize,
    docs_per_writer: usize,
    batch_size: usize,
    read_seed_docs: usize,
    read_ops: usize,
}

struct WriterPlan {
    collection_name: String,
    docs: Vec<Document>,
}

struct Measurement {
    units: usize,
    elapsed: Duration,
}

struct TempWorkDir {
    path: PathBuf,
}

impl TempWorkDir {
    fn new() -> std::io::Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let process_id = process::id();
        let path = env::temp_dir().join(format!("mqlite-perf-{process_id}-{unique}"));
        fs::create_dir(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempWorkDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn main() -> BenchResult<()> {
    let config = match parse_args()? {
        ParseResult::ListAxes => {
            for axis in AXES {
                println!("{}", axis.name);
            }
            return Ok(());
        }
        ParseResult::Run(config) => config,
    };

    #[cfg(feature = "perf-counters")]
    reset_perf_counters();

    let dir = TempWorkDir::new()?;
    let client = open_interval_client(&dir)?;
    let measurement = match config.axis.operation {
        Operation::ReadFindOne => run_read_axis(&client, &config)?,
        _ => run_write_axis(&client, &config)?,
    };
    print_measurement(&config, &measurement);

    #[cfg(feature = "perf-counters")]
    print_perf_counters();

    std::io::stdout().flush().ok();
    Ok(())
}

enum ParseResult {
    ListAxes,
    Run(Config),
}

fn parse_args() -> BenchResult<ParseResult> {
    let mut axis_name: Option<String> = None;
    let mut writers_override: Option<usize> = None;
    let mut docs_per_writer = DEFAULT_DOCS_PER_WRITER;
    let mut batch_size = DEFAULT_BATCH_SIZE;
    let mut read_seed_docs = DEFAULT_READ_SEED_DOCS;
    let mut read_ops = DEFAULT_READ_OPS;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--list-axes" => return Ok(ParseResult::ListAxes),
            "--axis" => axis_name = args.next(),
            "--writers" => {
                writers_override = Some(parse_positive_usize(&arg, args.next())?);
            }
            "--docs-per-writer" => {
                docs_per_writer = parse_positive_usize(&arg, args.next())?;
            }
            "--batch-size" => {
                batch_size = parse_positive_usize(&arg, args.next())?;
            }
            "--read-seed-docs" => {
                read_seed_docs = parse_positive_usize(&arg, args.next())?;
            }
            "--read-ops" => {
                read_ops = parse_positive_usize(&arg, args.next())?;
            }
            other => return Err(format!("unknown arg: {other}").into()),
        }
    }

    let axis_name = axis_name.ok_or(AXIS_REQUIRED_MSG)?;
    let axis = *AXES
        .iter()
        .find(|candidate| candidate.name == axis_name)
        .ok_or_else(|| format!("unknown --axis: {axis_name}"))?;
    let writers = writers_override.unwrap_or(axis.default_writers);
    validate_writer_count(axis, writers)?;

    Ok(ParseResult::Run(Config {
        axis,
        writers,
        docs_per_writer,
        batch_size,
        read_seed_docs,
        read_ops,
    }))
}

fn parse_positive_usize(flag: &str, value: Option<String>) -> BenchResult<usize> {
    let parsed: usize = value
        .ok_or_else(|| format!("{flag} requires value"))?
        .parse()?;
    if parsed == 0 {
        return Err(format!("{flag} must be >= 1").into());
    }
    Ok(parsed)
}

fn validate_writer_count(axis: AxisSpec, writers: usize) -> BenchResult<()> {
    let single_writer_axis = axis.name.starts_with("single_writer_");
    if single_writer_axis && writers != 1 {
        return Err(format!("{} requires --writers 1", axis.name).into());
    }
    if !single_writer_axis
        && matches!(axis.operation, Operation::InsertOne | Operation::InsertMany)
        && writers < 2
    {
        return Err(format!("{} requires at least 2 writers", axis.name).into());
    }
    Ok(())
}

fn open_interval_client(dir: &TempWorkDir) -> BenchResult<Client> {
    let path = dir.path().join("perf-matrix.mqlite");
    let opts = OpenOptions::new()
        .durability(DurabilityMode::Interval(Duration::from_millis(
            DURABILITY_INTERVAL_MS,
        )))
        .busy_timeout(Duration::from_secs(30));
    Ok(Client::open_with_options(&path, opts)?)
}

fn run_write_axis(client: &Client, config: &Config) -> BenchResult<Measurement> {
    let plans = build_write_plans(client, config)?;
    let ready_barrier = Arc::new(Barrier::new(config.writers + 1));
    let start_barrier = Arc::new(Barrier::new(config.writers + 1));
    let mut handles = Vec::with_capacity(config.writers);

    for plan in plans {
        let worker = client.clone();
        let ready = Arc::clone(&ready_barrier);
        let start = Arc::clone(&start_barrier);
        let operation = config.axis.operation;
        let batch_size = config.batch_size;

        handles.push(thread::spawn(move || -> Result<usize, String> {
            let collection = worker
                .database(DATABASE_NAME)
                .collection::<Document>(&plan.collection_name);
            ready.wait();
            start.wait();
            match operation {
                Operation::InsertOne => {
                    for doc in &plan.docs {
                        collection.insert_one(doc).map_err(|err| err.to_string())?;
                    }
                }
                Operation::InsertMany => {
                    for batch in plan.docs.chunks(batch_size) {
                        collection
                            .insert_many(batch)
                            .run()
                            .map_err(|err| err.to_string())?;
                    }
                }
                Operation::ReadFindOne => unreachable!("read axis"),
            }
            Ok(plan.docs.len())
        }));
    }

    ready_barrier.wait();
    let started = Instant::now();
    start_barrier.wait();
    let mut units = 0usize;
    for handle in handles {
        match handle.join() {
            Ok(Ok(count)) => units += count,
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("perf worker thread panicked".into()),
        }
    }

    Ok(Measurement {
        units,
        elapsed: started.elapsed(),
    })
}

fn build_write_plans(client: &Client, config: &Config) -> BenchResult<Vec<WriterPlan>> {
    let db = client.database(DATABASE_NAME);
    let payload = "x".repeat(PAYLOAD_BYTES);
    let collection_names = collection_names(config);

    for collection_name in setup_collection_names(config) {
        db.create_collection(&collection_name)?;
    }

    Ok(collection_names
        .into_iter()
        .enumerate()
        .map(|(writer_id, collection_name)| WriterPlan {
            collection_name,
            docs: build_docs(writer_id, config.docs_per_writer, &payload),
        })
        .collect())
}

fn collection_names(config: &Config) -> Vec<String> {
    match config.axis.namespaces {
        NamespaceShape::Single => vec![SINGLE_NAMESPACE.to_owned(); config.writers],
        NamespaceShape::Multi => (0..config.writers)
            .map(|writer_id| format!("docs_{writer_id}"))
            .collect(),
    }
}

fn setup_collection_names(config: &Config) -> Vec<String> {
    match config.axis.namespaces {
        NamespaceShape::Single => vec![SINGLE_NAMESPACE.to_owned()],
        NamespaceShape::Multi => collection_names(config),
    }
}

fn build_docs(writer_id: usize, count: usize, payload: &str) -> Vec<Document> {
    let base = writer_id as i64 * KEY_BAND_WIDTH;
    (0..count)
        .map(|seq| {
            doc! {
                "_id": base + seq as i64,
                "writer": writer_id as i32,
                "seq": seq as i64,
                "payload": payload,
            }
        })
        .collect()
}

fn run_read_axis(client: &Client, config: &Config) -> BenchResult<Measurement> {
    let db = client.database(DATABASE_NAME);
    db.create_collection(SINGLE_NAMESPACE)?;
    let collection = db.collection::<Document>(SINGLE_NAMESPACE);
    let payload = "x".repeat(PAYLOAD_BYTES);
    let seed = build_docs(0, config.read_seed_docs, &payload);
    collection.insert_many(&seed).run()?;

    let filters = (0..config.read_ops)
        .map(|op| doc! { "_id": (op % config.read_seed_docs) as i64 })
        .collect::<Vec<_>>();
    let started = Instant::now();
    for filter in filters {
        let found = collection.find_one(filter)?;
        if found.is_none() {
            return Err("read_find_one returned no document".into());
        }
    }

    Ok(Measurement {
        units: config.read_ops,
        elapsed: started.elapsed(),
    })
}

fn print_measurement(config: &Config, measurement: &Measurement) {
    let elapsed_secs = measurement.elapsed.as_secs_f64().max(f64::EPSILON);
    let throughput = measurement.units as f64 / elapsed_secs;
    match config.axis.operation {
        Operation::ReadFindOne => {
            println!(
                "{{\"axis\":\"{}\",\"writers\":1,\"ops\":{},\"elapsed_secs\":{:.6},\
                 \"ops_per_second\":{:.2},\"seed_docs\":{},\
                 \"timed_scope\":\"operation_only\"}}",
                config.axis.name,
                measurement.units,
                elapsed_secs,
                throughput,
                config.read_seed_docs
            );
        }
        Operation::InsertOne | Operation::InsertMany => {
            println!(
                "{{\"axis\":\"{}\",\"writers\":{},\"namespaces\":{},\"docs\":{},\
                 \"docs_per_writer\":{},\"batch_size\":{},\"payload_bytes\":{},\
                 \"elapsed_secs\":{:.6},\"docs_per_second\":{:.2},\
                 \"timed_scope\":\"operation_only\"}}",
                config.axis.name,
                config.writers,
                namespace_count(config),
                measurement.units,
                config.docs_per_writer,
                config.batch_size,
                PAYLOAD_BYTES,
                elapsed_secs,
                throughput
            );
        }
    }
}

fn namespace_count(config: &Config) -> usize {
    match config.axis.namespaces {
        NamespaceShape::Single => 1,
        NamespaceShape::Multi => config.writers,
    }
}

#[cfg(feature = "perf-counters")]
fn reset_perf_counters() {
    mqlite::perf_counters::reset_shared_latch_wait_hist();
    mqlite::perf_counters::reset_flip_counters();
}

#[cfg(feature = "perf-counters")]
fn print_perf_counters() {
    use mqlite::perf_counters as pc;
    println!(
        "{{\"perf_counters\":{{\"flip_retry_rate\":{:.6},\"flip_retry_exhausted\":{},\
         \"shared_latch_wait_p50_ns\":{},\"shared_latch_wait_p99_ns\":{},\
         \"install_phase_b_mean_hold_ns\":{},\"live_delta_check_mean_hold_ns\":{}}}}}",
        pc::flip_retry_rate(),
        pc::flip_retry_exhausted_count(),
        pc::shared_latch_wait_p50_ns(),
        pc::shared_latch_wait_p99_ns(),
        pc::install_phase_b_mean_hold_ns(),
        pc::live_delta_check_mean_hold_ns(),
    );
}
