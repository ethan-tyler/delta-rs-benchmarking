use delta_bench::assertions::CaseAssertion;
use delta_bench::cli::RunnerMode;
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{
    apply_dataset_assertion_policy, plan_run_cases, run_planned_cases, run_target, PlannedCase,
};

#[test]
fn case_filter_requires_at_least_one_matching_case() {
    let err = plan_run_cases("all", RunnerMode::Rust, Some("definitely_not_a_case"))
        .expect_err("no case match must return explicit error");
    assert!(
        err.to_string().contains("case filter"),
        "unexpected error: {err}"
    );
}

#[test]
fn all_runner_plan_includes_python_manifest_cases() {
    let plan = plan_run_cases("all", RunnerMode::All, None).expect("plan should build");
    let ids = plan.iter().map(|case| case.id.as_str()).collect::<Vec<_>>();

    assert!(
        ids.contains(&"pandas_roundtrip_smoke"),
        "interop python case should be included in runner=all plan"
    );
    assert!(
        ids.contains(&"pyarrow_dataset_scan_perf"),
        "interop python case should be included in runner=all plan"
    );
}

#[test]
fn all_runner_plan_is_manifest_ordered() {
    let plan = plan_run_cases("all", RunnerMode::All, None).expect("plan should build");
    let ids = plan.iter().map(|case| case.id.as_str()).collect::<Vec<_>>();
    let rust_idx = ids
        .iter()
        .position(|id| *id == "scan_full_narrow")
        .expect("rust case missing");
    let py_idx = ids
        .iter()
        .position(|id| *id == "pandas_roundtrip_smoke")
        .expect("python case missing");
    assert!(
        rust_idx < py_idx,
        "expected rust manifest sequence to be emitted before python manifest sequence"
    );
}

#[tokio::test]
async fn run_planned_cases_applies_assertions_and_can_fail_case() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let planned = vec![PlannedCase {
        id: "write_append_small".to_string(),
        target: "write".to_string(),
        assertions: vec![CaseAssertion::ExactResultHash(
            "sha256:not-real".to_string(),
        )],
    }];
    let cases = run_planned_cases(temp.path(), &planned, "sf1", 0, 1, &storage)
        .await
        .expect("planned run should execute");
    let only = &cases[0];

    assert!(!only.success, "assertion should convert case into failure");
    assert!(
        only.failure
            .as_ref()
            .map(|f| f.message.contains("result hash mismatch"))
            .unwrap_or(false),
        "unexpected failure payload: {:?}",
        only.failure
    );
}

#[tokio::test]
async fn run_planned_cases_applies_expected_failure_reclassification() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let planned = vec![PlannedCase {
        id: "write_append_small".to_string(),
        target: "write".to_string(),
        assertions: vec![CaseAssertion::ExpectedErrorContains(
            "fixture load failed".to_string(),
        )],
    }];
    let cases = run_planned_cases(temp.path(), &planned, "sf1", 0, 1, &storage)
        .await
        .expect("planned run should execute");
    let only = &cases[0];

    assert!(only.success, "expected-error assertion should mark success");
    assert_eq!(only.classification, "expected_failure");
}

#[tokio::test]
async fn manifest_hash_assertions_pass_for_write_case() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let planned = plan_run_cases("write", RunnerMode::Rust, Some("write_append_small"))
        .expect("plan should include write case with assertions");
    assert_eq!(planned.len(), 1);
    assert!(
        planned[0].assertions.len() >= 2,
        "expected exact_result_hash and schema_hash assertions from manifest"
    );

    let cases = run_planned_cases(temp.path(), &planned, "sf1", 0, 1, &storage)
        .await
        .expect("planned run should execute");
    let only = &cases[0];
    assert!(
        only.success,
        "manifest-based hash assertions should pass for write case: {:?}",
        only.failure
    );
}

#[tokio::test]
async fn run_target_all_requires_manifest_planning_api() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let err = run_target(temp.path(), "all", "sf1", 0, 1, &storage)
        .await
        .expect_err("run_target(all) should be rejected");
    assert!(
        err.to_string().contains("plan_run_cases"),
        "unexpected error: {err}"
    );
}

#[test]
fn tpcds_duckdb_dataset_skips_tpcds_hash_assertions() {
    let mut planned = vec![PlannedCase {
        id: "tpcds_q03".to_string(),
        target: "tpcds".to_string(),
        assertions: vec![
            CaseAssertion::ExactResultHash("sha256:expected".to_string()),
            CaseAssertion::SchemaHash("sha256:schema".to_string()),
            CaseAssertion::ExpectedErrorContains("fixture load failed".to_string()),
        ],
    }];

    apply_dataset_assertion_policy(&mut planned, Some("tpcds_duckdb"));

    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].assertions.len(), 2);
    assert!(
        planned[0]
            .assertions
            .iter()
            .all(|assertion| !matches!(assertion, CaseAssertion::ExactResultHash(_))),
        "exact result hash assertions should be dropped for tpcds_duckdb"
    );
    assert!(
        planned[0]
            .assertions
            .iter()
            .any(|assertion| matches!(assertion, CaseAssertion::SchemaHash(_))),
        "schema hash assertions must remain enabled for tpcds_duckdb"
    );
    assert!(planned[0]
        .assertions
        .iter()
        .any(|assertion| matches!(assertion, CaseAssertion::ExpectedErrorContains(_))));
}

#[test]
fn tpcds_duckdb_dataset_policy_does_not_modify_non_tpcds_cases() {
    let mut planned = vec![PlannedCase {
        id: "write_append_small".to_string(),
        target: "write".to_string(),
        assertions: vec![
            CaseAssertion::ExactResultHash("sha256:expected".to_string()),
            CaseAssertion::SchemaHash("sha256:schema".to_string()),
        ],
    }];

    apply_dataset_assertion_policy(&mut planned, Some("tpcds_duckdb"));

    assert_eq!(planned.len(), 1);
    assert_eq!(planned[0].assertions.len(), 2);
    assert!(matches!(
        planned[0].assertions[0],
        CaseAssertion::ExactResultHash(_)
    ));
    assert!(matches!(
        planned[0].assertions[1],
        CaseAssertion::SchemaHash(_)
    ));
}
