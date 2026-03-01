use delta_bench::suites::{list_cases_for_target, list_targets};

#[test]
fn list_targets_includes_optimize_vacuum() {
    assert!(
        list_targets().contains(&"optimize_vacuum"),
        "optimize_vacuum target missing from list_targets"
    );
}

#[test]
fn list_targets_includes_delete_update_dml() {
    assert!(
        list_targets().contains(&"delete_update_dml"),
        "delete_update_dml target missing from list_targets"
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
fn read_scan_case_list_is_exact() {
    let cases = list_cases_for_target("read_scan").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "read_full_scan_narrow".to_string(),
            "read_projection_region".to_string(),
            "read_filter_flag_true".to_string(),
            "read_partition_pruning_hit".to_string(),
            "read_partition_pruning_miss".to_string(),
        ]
    );
}

#[test]
fn delete_update_dml_case_list_is_exact() {
    let cases = list_cases_for_target("delete_update_dml").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "delete_rowsMatchedFraction_0.01_partition_localized".to_string(),
            "delete_rowsMatchedFraction_0.05_scattered".to_string(),
            "delete_rowsMatchedFraction_0.50_broad".to_string(),
            "update_literal_rowsMatchedFraction_0.01_partition_localized".to_string(),
            "update_literal_rowsMatchedFraction_0.05_scattered".to_string(),
            "update_expression_rowsMatchedFraction_0.50_broad".to_string(),
            "update_all_rows_expression".to_string(),
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
fn all_case_list_includes_interop_py_cases() {
    let cases = list_cases_for_target("all").expect("known target should work");
    assert!(
        cases.iter().any(|case| case == "pandas_roundtrip_smoke"),
        "all target should include interop_py cases"
    );
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
