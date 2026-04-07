use delta_bench::cli::{BenchmarkLane, TimingPhase};
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::run_target;

#[tokio::test]
async fn optimize_perf_smoke_runs_the_perf_owned_case_set() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("fixtures should be generated");

    let cases = run_target(
        temp.path(),
        "optimize_perf",
        "sf1",
        BenchmarkLane::Macro,
        TimingPhase::Execute,
        0,
        1,
        &storage,
    )
    .await
    .expect("optimize_perf suite should run");

    assert_eq!(
        cases
            .iter()
            .map(|case| case.case.as_str())
            .collect::<Vec<_>>(),
        vec![
            "optimize_perf_compact_small_files",
            "optimize_perf_noop_already_compact",
            "vacuum_perf_execute_lite",
        ]
    );
    assert!(
        cases.iter().all(|case| case.success),
        "optimize_perf failures: {:?}",
        cases
            .iter()
            .map(|case| (&case.case, &case.failure))
            .collect::<Vec<_>>()
    );
}
