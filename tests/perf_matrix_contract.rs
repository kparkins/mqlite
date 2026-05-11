//! Contract tests for the consolidated benchmark surface.

use std::path::Path;
use std::process::Command;

const REQUIRED_AXES: &[&str] = &[
    "single_writer_single_ns_single",
    "single_writer_single_ns_batch",
    "multi_writer_single_ns_single",
    "multi_writer_single_ns_batch",
    "multi_writer_multi_ns_single",
    "multi_writer_multi_ns_batch",
];

#[test]
fn benchmark_surface_is_consolidated_under_benches() {
    assert!(Path::new("benches/perf/perf_matrix.rs").is_file());
    assert!(Path::new("benches/perf/run_baselines.py").is_file());
    assert!(Path::new("benches/perf/sample_hot.py").is_file());

    assert!(!Path::new("examples/perf_axis.rs").exists());
    assert!(!Path::new("examples/perf_goal.rs").exists());
    assert!(!Path::new("tools/perf").exists());
    assert!(!Path::new("benches/writers_same_ns.rs").exists());
    assert!(!Path::new("benches/writers_diff_ns.rs").exists());
    assert!(!Path::new("benches/same_collection_multiwriter.rs").exists());
}

#[test]
fn perf_matrix_lists_required_write_axes() {
    let output = Command::new(env!("CARGO_BIN_EXE_perf_matrix"))
        .arg("--list-axes")
        .output()
        .expect("run perf_matrix --list-axes");
    assert!(
        output.status.success(),
        "perf_matrix --list-axes failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for axis in REQUIRED_AXES {
        assert!(stdout.lines().any(|line| line == *axis), "missing {axis}");
    }
}

#[test]
fn perf_matrix_smoke_runs_single_and_multi_namespace_axes() {
    for (axis, writers) in [
        ("single_writer_single_ns_single", "1"),
        ("multi_writer_multi_ns_batch", "2"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_perf_matrix"))
            .args([
                "--axis",
                axis,
                "--writers",
                writers,
                "--docs-per-writer",
                "2",
                "--batch-size",
                "2",
            ])
            .output()
            .unwrap_or_else(|error| panic!("run perf_matrix {axis}: {error}"));
        assert!(
            output.status.success(),
            "perf_matrix {axis} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("\"timed_scope\":\"operation_only\""));
        assert!(stdout.contains(&format!("\"axis\":\"{axis}\"")));
    }
}
