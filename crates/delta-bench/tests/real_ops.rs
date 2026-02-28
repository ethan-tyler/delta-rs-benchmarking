use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{optimize_vacuum, read_scan};

#[tokio::test]
async fn generated_fixtures_support_real_read_scan_suite() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = read_scan::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("read suite run");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| c.success));
}

#[tokio::test]
async fn read_scan_samples_include_physical_scan_metrics() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = read_scan::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("read suite run");
    assert!(!cases.is_empty());
    for case in &cases {
        assert!(case.success, "case failed: {:?}", case.failure);
        let sample = case.samples.first().expect("at least one sample");
        let metrics = sample.metrics.as_ref().expect("metrics should exist");
        assert!(metrics.files_scanned.is_some(), "missing files_scanned");
        assert!(metrics.files_pruned.is_some(), "missing files_pruned");
        assert!(metrics.bytes_scanned.is_some(), "missing bytes_scanned");
        assert!(metrics.scan_time_ms.is_some(), "missing scan_time_ms");
    }
}

#[tokio::test]
async fn generated_fixtures_support_optimize_vacuum_suite() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = optimize_vacuum::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("optimize_vacuum suite run");
    assert_eq!(cases.len(), 3);
    assert!(
        cases.iter().all(|c| c.success),
        "optimize_vacuum failures: {:?}",
        cases
            .iter()
            .map(|c| (&c.case, &c.failure))
            .collect::<Vec<_>>()
    );

    let optimize_case = cases
        .iter()
        .find(|c| c.case == "optimize_compact_small_files")
        .expect("optimize case should exist");
    let optimize_sample = optimize_case
        .samples
        .first()
        .expect("optimize sample should exist");
    let optimize_metrics = optimize_sample
        .metrics
        .as_ref()
        .expect("optimize metrics should exist");
    let files_scanned = optimize_metrics
        .files_scanned
        .expect("files_scanned should be present");
    let files_pruned = optimize_metrics
        .files_pruned
        .expect("files_pruned should be present");
    assert!(
        files_scanned >= files_pruned,
        "files_scanned should be >= files_pruned"
    );
}
