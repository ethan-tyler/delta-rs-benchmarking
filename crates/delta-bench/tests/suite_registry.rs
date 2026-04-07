use delta_bench::suites::{list_cases_for_target, list_targets};

#[test]
fn list_targets_includes_optimize_vacuum() {
    assert!(
        list_targets().contains(&"optimize_vacuum"),
        "optimize_vacuum target missing from list_targets"
    );
}

#[test]
fn list_targets_includes_delete_update() {
    assert!(
        list_targets().contains(&"delete_update"),
        "delete_update target missing from list_targets"
    );
}

#[test]
fn list_targets_includes_tpcds() {
    assert!(
        list_targets().contains(&"tpcds"),
        "tpcds target missing from list_targets"
    );
}

#[test]
fn list_targets_includes_interop_py() {
    assert!(
        list_targets().contains(&"interop_py"),
        "interop_py target missing from list_targets"
    );
}

#[test]
fn list_targets_includes_write_perf() {
    assert!(
        list_targets().contains(&"write_perf"),
        "write_perf target missing from list_targets"
    );
}

#[test]
fn list_targets_include_new_perf_owned_dml_and_maintenance_suites() {
    for target in ["delete_update_perf", "merge_perf", "optimize_perf"] {
        assert!(
            list_targets().contains(&target),
            "{target} target missing from list_targets"
        );
    }
}

#[test]
fn list_targets_includes_concurrency() {
    assert!(
        list_targets().contains(&"concurrency"),
        "concurrency target missing from list_targets"
    );
}

#[test]
fn list_targets_excludes_scan_planning() {
    assert!(
        !list_targets().contains(&"scan_planning"),
        "scan_planning should not be a public suite target"
    );
}

#[test]
fn optimize_vacuum_case_list_is_exact() {
    let cases = list_cases_for_target("optimize_vacuum").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "optimize_compact_small_files".to_string(),
            "optimize_noop_already_compact".to_string(),
            "optimize_heavy_compaction".to_string(),
            "vacuum_dry_run_lite".to_string(),
            "vacuum_execute_lite".to_string(),
        ]
    );
}

#[test]
fn write_perf_case_list_is_exact() {
    let cases = list_cases_for_target("write_perf").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "write_perf_partitioned_1m_parts_010".to_string(),
            "write_perf_partitioned_1m_parts_100".to_string(),
            "write_perf_partitioned_5m_parts_010".to_string(),
            "write_perf_unpartitioned_1m".to_string(),
        ]
    );
}

#[test]
fn delete_update_perf_case_list_is_exact() {
    let cases = list_cases_for_target("delete_update_perf").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "delete_perf_localized_1pct".to_string(),
            "delete_perf_scattered_5pct_small_files".to_string(),
            "update_perf_literal_5pct_scattered".to_string(),
            "update_perf_all_rows_expr".to_string(),
        ]
    );
}

#[test]
fn merge_perf_case_list_is_exact() {
    let cases = list_cases_for_target("merge_perf").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "merge_perf_upsert_10pct".to_string(),
            "merge_perf_upsert_50pct".to_string(),
            "merge_perf_localized_1pct".to_string(),
            "merge_perf_delete_5pct".to_string(),
        ]
    );
}

#[test]
fn optimize_perf_case_list_is_exact() {
    let cases = list_cases_for_target("optimize_perf").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "optimize_perf_compact_small_files".to_string(),
            "optimize_perf_noop_already_compact".to_string(),
            "vacuum_perf_execute_lite".to_string(),
        ]
    );
}

#[test]
fn scan_case_list_is_exact() {
    let cases = list_cases_for_target("scan").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "scan_full_narrow".to_string(),
            "scan_projection_region".to_string(),
            "scan_filter_flag".to_string(),
            "scan_pruning_hit".to_string(),
            "scan_pruning_miss".to_string(),
        ]
    );
}

#[test]
fn scan_planning_target_is_rejected() {
    let err = list_cases_for_target("scan_planning")
        .expect_err("scan_planning should not be a public suite target");
    assert!(
        err.to_string().contains("unknown suite target"),
        "unexpected error: {err}"
    );
}

#[test]
fn delete_update_case_list_is_exact() {
    let cases = list_cases_for_target("delete_update").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "delete_1pct_localized".to_string(),
            "delete_5pct_scattered".to_string(),
            "delete_50pct_broad".to_string(),
            "update_literal_1pct_localized".to_string(),
            "update_literal_5pct_scattered".to_string(),
            "update_expr_50pct_broad".to_string(),
            "update_all_rows_expr".to_string(),
        ]
    );
}

#[test]
fn tpcds_case_list_is_exact() {
    let cases = list_cases_for_target("tpcds").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "tpcds_q03".to_string(),
            "tpcds_q07".to_string(),
            "tpcds_q64".to_string(),
            "tpcds_q72".to_string(),
        ]
    );
}

#[test]
fn interop_py_case_list_is_exact() {
    let cases = list_cases_for_target("interop_py").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "pandas_roundtrip_smoke".to_string(),
            "polars_roundtrip_smoke".to_string(),
            "pyarrow_dataset_scan_perf".to_string(),
        ]
    );
}

#[test]
fn concurrency_case_list_is_exact() {
    let cases = list_cases_for_target("concurrency").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "concurrent_table_create".to_string(),
            "concurrent_append_multi".to_string(),
            "update_vs_compaction".to_string(),
            "delete_vs_compaction".to_string(),
            "optimize_vs_optimize_overlap".to_string(),
        ]
    );
}

#[test]
fn all_case_list_includes_interop_py_cases() {
    let cases = list_cases_for_target("all").expect("known target should work");
    assert!(
        cases.iter().any(|case| case == "pandas_roundtrip_smoke"),
        "all target should include interop_py cases"
    );
}

#[test]
fn all_case_list_excludes_write_perf_cases() {
    let cases = list_cases_for_target("all").expect("known target should work");
    assert!(
        cases.iter().all(|case| !case.starts_with("write_perf_")),
        "all target should exclude opt-in write_perf cases"
    );
}

#[test]
fn all_case_list_excludes_new_perf_owned_dml_and_maintenance_cases() {
    let cases = list_cases_for_target("all").expect("known target should work");
    for prefix in [
        "delete_perf_",
        "merge_perf_",
        "optimize_perf_",
        "vacuum_perf_",
    ] {
        assert!(
            cases.iter().all(|case| !case.starts_with(prefix)),
            "all target should exclude opt-in perf cases with prefix {prefix}"
        );
    }
}

#[test]
fn unknown_target_returns_error() {
    let err = list_cases_for_target("totally_unknown_target")
        .expect_err("unknown target should return an explicit error");
    assert!(
        err.to_string().contains("unknown suite target"),
        "unexpected error: {err}"
    );
}

#[test]
fn legacy_scan_target_alias_is_rejected() {
    let err = list_cases_for_target("read_scan")
        .expect_err("legacy target alias should return an explicit error");
    assert!(
        err.to_string().contains("unknown suite target"),
        "unexpected error: {err}"
    );
}

#[test]
fn legacy_delete_update_target_alias_is_rejected() {
    let err = list_cases_for_target("delete_update_dml")
        .expect_err("legacy target alias should return an explicit error");
    assert!(
        err.to_string().contains("unknown suite target"),
        "unexpected error: {err}"
    );
}

#[test]
fn legacy_merge_target_alias_is_rejected() {
    let err = list_cases_for_target("merge_dml")
        .expect_err("legacy target alias should return an explicit error");
    assert!(
        err.to_string().contains("unknown suite target"),
        "unexpected error: {err}"
    );
}
