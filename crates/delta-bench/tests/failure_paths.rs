use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{delete_update_dml, merge_dml, optimize_vacuum, write};

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
async fn merge_suite_missing_partitioned_fixture_returns_fixture_failures_for_all_cases() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");
    std::fs::remove_dir_all(
        temp.path()
            .join("sf1")
            .join("merge_partitioned_target_delta"),
    )
    .expect("remove partitioned merge fixture");

    let cases = merge_dml::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
    for case in cases {
        let failure = case
            .failure
            .expect("failure should be present for missing fixture");
        assert!(
            failure.message.contains("fixture load failed"),
            "expected normalized fixture failure for {}, got: {}",
            case.case,
            failure.message
        );
    }
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

#[tokio::test]
async fn delete_update_dml_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = delete_update_dml::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}
