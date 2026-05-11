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
    "duration_seconds",
    "rows",
];

const REQUIRED_ROW_FIELDS: &[&str] = &[
    "axis",
    "writers",
    "median_dps",
    "min_dps",
    "max_dps",
    "envelope",
];

const ALLOWED_AXES: &[&str] = &[
    "same_ns_single",
    "same_ns_batch",
    "same_ns_partitioned",
    "multi_ns_single",
    "multi_ns_batch",
    "read_find_one",
];

#[test]
fn baseline_sidecars_match_schema() {
    let dir = Path::new(BASELINE_DIR);
    assert!(
        dir.is_dir(),
        "{BASELINE_DIR}/ does not exist (PR0 must create it)"
    );

    let mut sidecars: Vec<_> = fs::read_dir(dir)
        .expect("read perf-baselines dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|s| s == "json").unwrap_or(false))
        .collect();
    sidecars.sort();

    // No sidecars yet is a soft pass: PR0 lands the schema + smoke test in
    // one commit and the baseline data in a follow-up commit, so this test
    // must succeed in the interim. Once any sidecar exists it must conform.
    if sidecars.is_empty() {
        eprintln!("note: no JSON sidecars in {BASELINE_DIR}/ yet — schema check is a no-op");
        return;
    }

    for path in &sidecars {
        let body =
            fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let v: Value = serde_json::from_str(&body)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));

        let obj = v
            .as_object()
            .unwrap_or_else(|| panic!("{}: top-level must be object", path.display()));

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
        assert!(
            obj["duration_seconds"].is_u64(),
            "{}: duration_seconds must be unsigned integer",
            path.display()
        );

        let rows = obj["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("{}: rows must be array", path.display()));
        assert!(!rows.is_empty(), "{}: rows array is empty", path.display());

        for (i, row) in rows.iter().enumerate() {
            let row = row
                .as_object()
                .unwrap_or_else(|| panic!("{}: rows[{i}] must be object", path.display()));
            for key in REQUIRED_ROW_FIELDS {
                assert!(
                    row.contains_key(*key),
                    "{}: rows[{i}] missing field {key:?}",
                    path.display()
                );
            }

            let axis = row["axis"].as_str().unwrap_or_else(|| {
                panic!("{}: rows[{i}].axis must be string", path.display())
            });
            assert!(
                ALLOWED_AXES.contains(&axis),
                "{}: rows[{i}].axis = {axis:?} not in allowed set",
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

            let median = row["median_dps"].as_f64().unwrap();
            let min = row["min_dps"].as_f64().unwrap();
            let max = row["max_dps"].as_f64().unwrap();
            let envelope = row["envelope"].as_f64().unwrap();
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
}
