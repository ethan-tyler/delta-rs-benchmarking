use delta_bench::cli::{BenchmarkLane, RunnerMode, TimingPhase};
use delta_bench::data::fixtures::{generate_fixtures_with_profile, FixtureProfile};
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{list_cases_for_target, list_targets, plan_run_cases, run_target};

#[tokio::test]
async fn metadata_perf_smoke_runs_the_dedicated_perf_owned_case_set() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_fixtures_with_profile(
        temp.path(),
        "sf1",
        42,
        true,
        FixtureProfile::ManyVersions,
        &storage,
    )
    .await
    .expect("fixtures should be generated");

    assert!(
        list_targets().contains(&"metadata_perf"),
        "metadata_perf target should be registered"
    );
    assert_eq!(
        list_cases_for_target("metadata_perf").expect("metadata_perf case list"),
        vec![
            "metadata_perf_load_head_long_history".to_string(),
            "metadata_perf_time_travel_v0_long_history".to_string(),
            "metadata_perf_load_checkpointed_head".to_string(),
            "metadata_perf_load_uncheckpointed_head".to_string(),
        ]
    );
    let planned = plan_run_cases(
        "metadata_perf",
        RunnerMode::Rust,
        Some("metadata_perf_load_checkpointed_head"),
    )
    .expect("metadata_perf planning should work");
    assert_eq!(planned.len(), 1, "expected exactly one filtered case");
    assert_eq!(
        planned
            .iter()
            .map(|case| case.id.as_str())
            .collect::<Vec<_>>(),
        vec!["metadata_perf_load_checkpointed_head"]
    );
    assert!(
        planned[0].supports_decision,
        "metadata_perf should carry decision metadata"
    );
    assert_eq!(planned[0].required_runs, Some(5));
    assert_eq!(planned[0].decision_threshold_pct, Some(5.0));
    assert_eq!(planned[0].decision_metric.as_deref(), Some("median"));

    let cases = run_target(
        temp.path(),
        "metadata_perf",
        "sf1",
        BenchmarkLane::Macro,
        TimingPhase::Execute,
        0,
        1,
        &storage,
    )
    .await
    .expect("metadata_perf suite should run");

    assert_eq!(
        cases
            .iter()
            .map(|case| case.case.as_str())
            .collect::<Vec<_>>(),
        vec![
            "metadata_perf_load_head_long_history",
            "metadata_perf_time_travel_v0_long_history",
            "metadata_perf_load_checkpointed_head",
            "metadata_perf_load_uncheckpointed_head",
        ]
    );
    assert!(
        cases.iter().all(|case| case.success),
        "metadata_perf failures: {:?}",
        cases
            .iter()
            .map(|case| (&case.case, &case.failure))
            .collect::<Vec<_>>()
    );
}
