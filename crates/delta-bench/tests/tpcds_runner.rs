#[path = "support/tpcds_fixture.rs"]
mod support;

use delta_bench::cli::TimingPhase;
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::tpcds;

#[tokio::test]
async fn enabled_queries_execute_and_emit_successful_cases() {
    let temp = tempfile::tempdir().expect("tempdir");
    support::write_store_sales_fixture(temp.path(), "sf1").await;
    let storage = StorageConfig::local();

    let cases = tpcds::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("run tpcds");

    let enabled = cases
        .iter()
        .filter(|case| case.case != "tpcds_q72")
        .collect::<Vec<_>>();
    assert!(!enabled.is_empty(), "expected enabled TPC-DS query cases");
    assert!(
        enabled.iter().all(|case| case.success),
        "enabled queries should succeed: {enabled:#?}"
    );
}

#[tokio::test]
async fn generated_fixtures_provide_required_tpcds_tables() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = tpcds::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("run tpcds");
    let enabled = cases
        .iter()
        .filter(|case| case.case != "tpcds_q72")
        .collect::<Vec<_>>();
    assert!(
        enabled.iter().all(|case| case.success),
        "generated fixtures should satisfy enabled TPC-DS cases: {enabled:#?}"
    );
}

#[tokio::test]
async fn q72_is_reported_as_deterministically_skipped() {
    let temp = tempfile::tempdir().expect("tempdir");
    support::write_store_sales_fixture(temp.path(), "sf1").await;
    let storage = StorageConfig::local();

    let cases = tpcds::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("run tpcds");
    let q72 = cases
        .iter()
        .find(|case| case.case == "tpcds_q72")
        .expect("q72 result should exist");
    assert!(!q72.success, "q72 should be emitted as skipped/failure");
    let failure = q72
        .failure
        .as_ref()
        .expect("q72 failure payload should be present");
    assert!(
        failure.message.to_ascii_lowercase().contains("skipped"),
        "q72 failure message should encode skip behavior; got: {}",
        failure.message
    );
}

#[tokio::test]
async fn successful_samples_include_normalized_and_scan_metrics() {
    let temp = tempfile::tempdir().expect("tempdir");
    support::write_store_sales_fixture(temp.path(), "sf1").await;
    let storage = StorageConfig::local();

    let cases = tpcds::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("run tpcds");

    let samples = cases
        .iter()
        .filter(|case| case.success)
        .flat_map(|case| case.samples.iter())
        .collect::<Vec<_>>();
    assert!(
        !samples.is_empty(),
        "expected at least one successful sample"
    );

    for sample in samples {
        let metrics = sample
            .metrics
            .as_ref()
            .expect("successful samples should include metrics");
        assert!(
            metrics.rows_processed.is_some(),
            "rows_processed should be normalized in metrics"
        );
        assert!(
            metrics.files_scanned.is_some()
                || metrics.files_pruned.is_some()
                || metrics.bytes_scanned.is_some()
                || metrics.scan_time_ms.is_some(),
            "expected at least one scan metric in sample: {metrics:?}"
        );
    }
}

#[tokio::test]
async fn tpcds_case_names_remain_stable_under_plan_timing() {
    let temp = tempfile::tempdir().expect("tempdir");
    support::write_store_sales_fixture(temp.path(), "sf1").await;
    let storage = StorageConfig::local();

    let cases = tpcds::run(temp.path(), "sf1", TimingPhase::Plan, 0, 1, &storage)
        .await
        .expect("run tpcds");

    assert_eq!(
        cases
            .iter()
            .map(|case| case.case.as_str())
            .collect::<Vec<_>>(),
        vec!["tpcds_q03", "tpcds_q07", "tpcds_q64", "tpcds_q72"]
    );
    let q03 = cases
        .iter()
        .find(|case| case.case == "tpcds_q03")
        .expect("q03 case should exist");
    assert!(
        q03.success,
        "plan timing should not change enabled case success: {:?}",
        q03.failure
    );
}
