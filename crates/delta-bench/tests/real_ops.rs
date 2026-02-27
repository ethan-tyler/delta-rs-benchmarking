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
}
