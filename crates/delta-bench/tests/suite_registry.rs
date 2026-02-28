use delta_bench::suites::{list_cases_for_target, list_targets};

#[test]
fn list_targets_includes_optimize_vacuum() {
    assert!(
        list_targets().contains(&"optimize_vacuum"),
        "optimize_vacuum target missing from list_targets"
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
fn optimize_vacuum_case_list_is_exact() {
    let cases = list_cases_for_target("optimize_vacuum").expect("known target should work");
    assert_eq!(
        cases,
        vec![
            "optimize_compact_small_files".to_string(),
            "vacuum_dry_run_lite".to_string(),
            "vacuum_execute_lite".to_string(),
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
fn unknown_target_returns_error() {
    let err = list_cases_for_target("totally_unknown_target")
        .expect_err("unknown target should return an explicit error");
    assert!(
        err.to_string().contains("unknown suite target"),
        "unexpected error: {err}"
    );
}
