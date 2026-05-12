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

const REQUIRED_DURABILITIES: &[&str] = &["full-sync", "interval-50ms", "none"];

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
fn perf_matrix_lists_required_write_axes() -> Result<(), String> {
    let output = Command::new(env!("CARGO_BIN_EXE_perf_matrix"))
        .arg("--list-axes")
        .output()
        .map_err(|error| format!("run perf_matrix --list-axes: {error}"))?;
    assert!(
        output.status.success(),
        "perf_matrix --list-axes failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for axis in REQUIRED_AXES {
        assert!(stdout.lines().any(|line| line == *axis), "missing {axis}");
    }
    Ok(())
}

#[test]
fn perf_matrix_lists_required_durability_modes() -> Result<(), String> {
    let output = Command::new(env!("CARGO_BIN_EXE_perf_matrix"))
        .arg("--list-durability")
        .output()
        .map_err(|error| format!("run perf_matrix --list-durability: {error}"))?;
    assert!(
        output.status.success(),
        "perf_matrix --list-durability failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    for durability in REQUIRED_DURABILITIES {
        assert!(
            stdout.lines().any(|line| line == *durability),
            "missing {durability}"
        );
    }
    Ok(())
}

#[test]
fn perf_matrix_smoke_runs_single_and_multi_namespace_axes() -> Result<(), String> {
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
                "--exit-after-measurement",
            ])
            .output()
            .map_err(|error| format!("run perf_matrix {axis}: {error}"))?;
        assert!(
            output.status.success(),
            "perf_matrix {axis} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("\"timed_scope\":\"operation_only\""));
        assert!(stdout.contains(&format!("\"axis\":\"{axis}\"")));
    }
    Ok(())
}

#[test]
fn perf_matrix_smoke_runs_supported_durability_modes() -> Result<(), String> {
    for durability in REQUIRED_DURABILITIES {
        let output = Command::new(env!("CARGO_BIN_EXE_perf_matrix"))
            .args([
                "--axis",
                "single_writer_single_ns_single",
                "--durability",
                durability,
                "--writers",
                "1",
                "--docs-per-writer",
                "1",
                "--batch-size",
                "1",
                "--exit-after-measurement",
            ])
            .output()
            .map_err(|error| format!("run perf_matrix {durability}: {error}"))?;
        assert!(
            output.status.success(),
            "perf_matrix {durability} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("\"timed_scope\":\"operation_only\""));
        assert!(stdout.contains(&format!("\"durability\":\"{durability}\"")));
    }
    Ok(())
}
