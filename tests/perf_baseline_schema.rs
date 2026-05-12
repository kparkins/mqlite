//! Smoke test for `docs/perf-baselines/*.json` sidecars.
//!
//! Round-trips every JSON sidecar through `serde_json::Value` and asserts
//! the schema documented in `docs/perf-baselines/SCHEMA.md`. Future PRs
//! that consume the baselines (e.g. PR1 compounding-delta math) rely on
//! the schema being stable; this test fails loudly if a sidecar drifts.

use std::fs;
use std::path::Path;

use serde_json::Value;

const BASELINE_DIR: &str = "docs/perf-baselines";

const REQUIRED_TOP_LEVEL: &[&str] = &[
    "date",
    "branch",
    "hardware",
    "build_cmd",
    "axis_runs",
    "rows",
];

const REQUIRED_ROW_FIELDS: &[&str] = &[
    "axis",
    "durability",
    "writers",
    "median_dps",
    "min_dps",
    "max_dps",
    "envelope",
];

const ALLOWED_AXES: &[&str] = &[
    "single_writer_single_ns_single",
    "single_writer_single_ns_batch",
    "multi_writer_single_ns_single",
    "multi_writer_single_ns_batch",
    "multi_writer_multi_ns_single",
    "multi_writer_multi_ns_batch",
    "same_ns_single",
    "same_ns_batch",
    "same_ns_partitioned",
    "multi_ns_single",
    "multi_ns_batch",
    "read_find_one",
];

const ALLOWED_DURABILITIES: &[&str] = &["full-sync", "interval-50ms", "none"];

#[test]
fn baseline_sidecars_match_schema() -> Result<(), String> {
    let dir = Path::new(BASELINE_DIR);
    assert!(
        dir.is_dir(),
        "{BASELINE_DIR}/ does not exist (PR0 must create it)"
    );

    let mut sidecars: Vec<_> = fs::read_dir(dir)
        .map_err(|error| format!("{BASELINE_DIR}: {error}"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|s| s == "json").unwrap_or(false))
        .collect();
    sidecars.sort();

    // No sidecars yet is a soft pass: PR0 lands the schema + smoke test in
    // one commit and the baseline data in a follow-up commit, so this test
    // must succeed in the interim. Once any sidecar exists it must conform.
    if sidecars.is_empty() {
        eprintln!(
            "note: no JSON sidecars in {BASELINE_DIR}/ yet - \
             schema check is a no-op"
        );
        return Ok(());
    }

    for path in &sidecars {
        let path_display = path.display().to_string();
        let body = fs::read_to_string(path).map_err(|error| format!("{path_display}: {error}"))?;
        let v: Value =
            serde_json::from_str(&body).map_err(|error| format!("{path_display}: {error}"))?;

        let obj = v
            .as_object()
            .ok_or_else(|| format!("{path_display}: top-level must be object"))?;

        for key in REQUIRED_TOP_LEVEL {
            assert!(
                obj.contains_key(*key),
                "{}: missing top-level field {key:?}",
                path.display()
            );
        }

        assert!(
            obj["date"].is_string(),
            "{}: date must be string",
            path.display()
        );
        assert!(
            obj["branch"].is_string(),
            "{}: branch must be string",
            path.display()
        );
        assert!(
            obj["axis_runs"].is_u64(),
            "{}: axis_runs must be unsigned integer",
            path.display()
        );
        assert_run_shape_fields(path, obj);

        let rows = obj["rows"]
            .as_array()
            .ok_or_else(|| format!("{path_display}: rows must be array"))?;
        assert!(!rows.is_empty(), "{}: rows array is empty", path.display());

        for (i, row) in rows.iter().enumerate() {
            let row = row
                .as_object()
                .ok_or_else(|| format!("{path_display}: rows[{i}] must be object"))?;
            for key in REQUIRED_ROW_FIELDS {
                assert!(
                    row.contains_key(*key),
                    "{}: rows[{i}] missing field {key:?}",
                    path.display()
                );
            }

            let axis = row["axis"]
                .as_str()
                .ok_or_else(|| format!("{path_display}: rows[{i}].axis must be string"))?;
            assert!(
                ALLOWED_AXES.contains(&axis),
                "{}: rows[{i}].axis = {axis:?} not in allowed set",
                path.display()
            );

            let durability = row["durability"]
                .as_str()
                .ok_or_else(|| format!("{path_display}: rows[{i}].durability must be string"))?;
            assert!(
                ALLOWED_DURABILITIES.contains(&durability),
                "{}: rows[{i}].durability = {durability:?} not in allowed set",
                path.display()
            );

            assert!(
                row["writers"].is_u64(),
                "{}: rows[{i}].writers must be unsigned integer",
                path.display()
            );
            for num_field in ["median_dps", "min_dps", "max_dps", "envelope"] {
                assert!(
                    row[num_field].is_number(),
                    "{}: rows[{i}].{num_field} must be number",
                    path.display()
                );
            }

            let median = row_number(row, "median_dps", &path_display, i)?;
            let min = row_number(row, "min_dps", &path_display, i)?;
            let max = row_number(row, "max_dps", &path_display, i)?;
            let envelope = row_number(row, "envelope", &path_display, i)?;
            assert!(
                min <= median && median <= max,
                "{}: rows[{i}] min/median/max ordering broken: {min}/{median}/{max}",
                path.display()
            );
            assert!(
                envelope >= 0.0,
                "{}: rows[{i}].envelope must be non-negative",
                path.display()
            );
        }
    }

    Ok(())
}

fn assert_run_shape_fields(path: &Path, obj: &serde_json::Map<String, Value>) {
    let has_duration = obj
        .get("duration_seconds")
        .is_some_and(|value| value.is_u64());
    let has_fixed_docs = obj
        .get("docs_per_writer")
        .is_some_and(|value| value.is_u64())
        && obj.get("batch_size").is_some_and(|value| value.is_u64());
    assert!(
        has_duration || has_fixed_docs,
        "{}: sidecar must declare either duration_seconds or \
         docs_per_writer + batch_size",
        path.display()
    );
}

fn row_number(
    row: &serde_json::Map<String, Value>,
    field: &str,
    path_display: &str,
    row_index: usize,
) -> Result<f64, String> {
    row.get(field)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("{path_display}: rows[{row_index}].{field} must be number"))
}
