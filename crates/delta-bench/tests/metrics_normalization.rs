use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{metadata, write};

#[tokio::test]
async fn write_samples_include_normalized_metrics() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = write::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("run write suite");
    let first_sample = &cases[0].samples[0];
    let metrics = first_sample
        .metrics
        .as_ref()
        .expect("metrics should be present");
    assert!(metrics.rows_processed.is_some());
    assert!(metrics.operations.is_some());
    assert!(metrics.table_version.is_some());
}

#[tokio::test]
async fn metadata_samples_report_table_version_metric() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = metadata::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("run metadata suite");
    assert!(!cases.is_empty());
    assert!(
        cases[0].success,
        "metadata case failed: {:?}",
        cases[0].failure
    );
    let sample = &cases[0].samples[0];
    let metrics = sample.metrics.as_ref().expect("metrics should be present");
    assert!(metrics.table_version.is_some());
    assert_eq!(sample.rows, metrics.rows_processed);
}
