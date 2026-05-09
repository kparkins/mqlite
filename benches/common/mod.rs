#![allow(
    dead_code,
    reason = "each Criterion bench compiles as its own crate and uses a subset of the shared helpers"
)]

use std::path::Path;
use std::time::Duration;

use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

/// Human-readable label for the shared interval durability setting.
pub(crate) const INTERVAL_100MS_LABEL: &str = "Interval(100ms)";

/// Return host metadata printed by benchmark harnesses.
pub(crate) fn host_metadata() -> String {
    let rustc = non_empty_command_output("rustc", &["--version"]);

    let cpu_count = std::process::Command::new("sh")
        .arg("-c")
        .arg("nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 1")
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(1);
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    format!("rustc=\"{rustc}\" cpu_count={cpu_count} arch={arch} os={os}")
}

/// Return trimmed command stdout or `"unknown"` if the command fails.
pub(crate) fn command_output(program: &str, args: &[&str]) -> String {
    std::process::Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Return non-empty trimmed command stdout or `"unknown"`.
pub(crate) fn non_empty_command_output(program: &str, args: &[&str]) -> String {
    let output = command_output(program, args);
    if output.is_empty() {
        "unknown".to_owned()
    } else {
        output
    }
}

/// Return the interval durability mode shared by throughput benchmarks.
pub(crate) fn interval_100ms() -> DurabilityMode {
    DurabilityMode::Interval(Duration::from_millis(100))
}

/// Open a benchmark database at `path` using the supplied durability mode.
pub(crate) fn open_client(path: &Path, mode: DurabilityMode) -> Client {
    let opts = OpenOptions::new().durability(mode);
    Client::open_with_options(path, opts).expect("open must succeed")
}

/// Open `bench.mqlite` in `dir` with 100ms interval durability.
pub(crate) fn open_interval_client(dir: &TempDir) -> Client {
    open_client(&dir.path().join("bench.mqlite"), interval_100ms())
}
