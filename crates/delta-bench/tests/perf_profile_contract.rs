use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use delta_bench::manifests::load_manifest;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read_env(path: &Path) -> HashMap<String, String> {
    fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn manifest() -> delta_bench::manifests::BenchmarkManifest {
    let path = repo_root().join("bench/manifests/core_rust.yaml");
    load_manifest(&path).expect("load core rust manifest")
}

fn assert_env_eq(path: &Path, expected: &[(&str, &str)]) {
    let env = read_env(path);
    for (key, value) in expected {
        assert_eq!(env.get(*key), Some(&value.to_string()), "{key}");
    }
}

#[test]
fn pr_write_perf_profile_uses_compare_contract() {
    assert_env_eq(
        &repo_root().join("bench/methodologies/pr-write-perf.env"),
        &[
            ("METHODOLOGY_PROFILE", "pr-write-perf"),
            ("METHODOLOGY_VERSION", "1"),
            ("PROFILE_KIND", "compare"),
            ("TARGET", "write_perf"),
            ("COMPARE_MODE", "decision"),
            ("DATASET_POLICY", "intrinsic_case_workload"),
            ("WARMUP", "1"),
            ("ITERS", "7"),
            ("PREWARM_ITERS", "1"),
            ("COMPARE_RUNS", "5"),
            ("MEASURE_ORDER", "alternate"),
            ("TIMING_PHASE", "execute"),
            ("AGGREGATION", "median"),
            ("SPREAD_METRIC", "iqr_ms"),
        ],
    );
}

#[test]
fn pr_tpcds_profile_uses_compare_contract() {
    assert_env_eq(
        &repo_root().join("bench/methodologies/pr-tpcds.env"),
        &[
            ("METHODOLOGY_PROFILE", "pr-tpcds"),
            ("METHODOLOGY_VERSION", "1"),
            ("PROFILE_KIND", "compare"),
            ("TARGET", "tpcds"),
            ("DATASET_ID", "tpcds_duckdb"),
            ("COMPARE_MODE", "decision"),
            ("WARMUP", "1"),
            ("ITERS", "5"),
            ("PREWARM_ITERS", "1"),
            ("COMPARE_RUNS", "5"),
            ("MEASURE_ORDER", "alternate"),
            ("TIMING_PHASE", "execute"),
            ("AGGREGATION", "median"),
            ("SPREAD_METRIC", "iqr_ms"),
        ],
    );
}

#[test]
fn pr_delete_update_perf_profile_uses_compare_contract() {
    assert_env_eq(
        &repo_root().join("bench/methodologies/pr-delete-update-perf.env"),
        &[
            ("METHODOLOGY_PROFILE", "pr-delete-update-perf"),
            ("METHODOLOGY_VERSION", "1"),
            ("PROFILE_KIND", "compare"),
            ("TARGET", "delete_update_perf"),
            ("DATASET_ID", "medium_selective"),
            ("COMPARE_MODE", "decision"),
            ("WARMUP", "1"),
            ("ITERS", "5"),
            ("PREWARM_ITERS", "1"),
            ("COMPARE_RUNS", "5"),
            ("MEASURE_ORDER", "alternate"),
            ("TIMING_PHASE", "execute"),
            ("AGGREGATION", "median"),
            ("DATASET_POLICY", "shared_run_scope"),
            ("SPREAD_METRIC", "iqr_ms"),
        ],
    );
}

#[test]
fn pr_merge_perf_profile_uses_compare_contract() {
    assert_env_eq(
        &repo_root().join("bench/methodologies/pr-merge-perf.env"),
        &[
            ("METHODOLOGY_PROFILE", "pr-merge-perf"),
            ("METHODOLOGY_VERSION", "1"),
            ("PROFILE_KIND", "compare"),
            ("TARGET", "merge_perf"),
            ("DATASET_ID", "medium_selective"),
            ("COMPARE_MODE", "decision"),
            ("WARMUP", "1"),
            ("ITERS", "5"),
            ("PREWARM_ITERS", "1"),
            ("COMPARE_RUNS", "5"),
            ("MEASURE_ORDER", "alternate"),
            ("TIMING_PHASE", "execute"),
            ("AGGREGATION", "median"),
            ("DATASET_POLICY", "shared_run_scope"),
            ("SPREAD_METRIC", "iqr_ms"),
        ],
    );
}

#[test]
fn pr_optimize_perf_profile_uses_compare_contract() {
    assert_env_eq(
        &repo_root().join("bench/methodologies/pr-optimize-perf.env"),
        &[
            ("METHODOLOGY_PROFILE", "pr-optimize-perf"),
            ("METHODOLOGY_VERSION", "1"),
            ("PROFILE_KIND", "compare"),
            ("TARGET", "optimize_perf"),
            ("DATASET_ID", "medium_selective"),
            ("COMPARE_MODE", "decision"),
            ("WARMUP", "1"),
            ("ITERS", "5"),
            ("PREWARM_ITERS", "1"),
            ("COMPARE_RUNS", "5"),
            ("MEASURE_ORDER", "alternate"),
            ("TIMING_PHASE", "execute"),
            ("AGGREGATION", "median"),
            ("DATASET_POLICY", "shared_run_scope"),
            ("SPREAD_METRIC", "iqr_ms"),
        ],
    );
}

#[test]
fn write_perf_cases_carry_explicit_decision_metadata() {
    let manifest = manifest();
    let write_perf_cases = manifest
        .cases
        .iter()
        .filter(|case| case.target == "write_perf")
        .collect::<Vec<_>>();

    assert!(
        !write_perf_cases.is_empty(),
        "expected write_perf cases in core_rust.yaml"
    );

    for case in write_perf_cases {
        assert_eq!(case.supports_decision, Some(true), "{}", case.id);
        assert_eq!(case.required_runs, Some(5), "{}", case.id);
        assert_eq!(case.decision_threshold_pct, Some(5.0), "{}", case.id);
        assert_eq!(
            case.decision_metric.as_deref(),
            Some("median"),
            "{}",
            case.id
        );
    }
}

#[test]
fn new_perf_owned_dml_and_maintenance_cases_carry_explicit_decision_metadata() {
    let manifest = manifest();
    let perf_cases = manifest
        .cases
        .iter()
        .filter(|case| {
            matches!(
                case.target.as_str(),
                "delete_update_perf" | "merge_perf" | "optimize_perf"
            )
        })
        .collect::<Vec<_>>();

    assert!(
        !perf_cases.is_empty(),
        "expected new perf-owned DML/maintenance cases in core_rust.yaml"
    );

    for case in perf_cases {
        assert_eq!(case.supports_decision, Some(true), "{}", case.id);
        assert_eq!(case.required_runs, Some(5), "{}", case.id);
        assert_eq!(case.decision_threshold_pct, Some(5.0), "{}", case.id);
        assert_eq!(
            case.decision_metric.as_deref(),
            Some("median"),
            "{}",
            case.id
        );
    }
}
#[test]
fn enabled_tpcds_cases_remain_exact() {
    let manifest = manifest();
    let enabled_tpcds_cases = manifest
        .cases
        .iter()
        .filter(|case| case.target == "tpcds" && case.enabled)
        .map(|case| case.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        enabled_tpcds_cases,
        vec!["tpcds_q03", "tpcds_q07", "tpcds_q64"]
    );
}
