use delta_bench::storage::StorageConfig;
use delta_bench::suites::{merge_dml, optimize_vacuum, write};

#[tokio::test]
async fn write_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = write::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}

#[tokio::test]
async fn merge_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = merge_dml::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}

#[tokio::test]
async fn optimize_vacuum_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = optimize_vacuum::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}
