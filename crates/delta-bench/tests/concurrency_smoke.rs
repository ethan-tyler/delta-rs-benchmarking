use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::concurrency;

const CREATE_WORKERS: u64 = 4;
const APPEND_WORKERS: u64 = 4;
const CONTENDED_ATTEMPTS: u64 = 6;

#[tokio::test(flavor = "multi_thread")]
async fn generated_standard_fixtures_support_concurrency_suite() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = concurrency::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("concurrency suite run");
    assert_eq!(cases.len(), 5, "expected exact concurrency case list");
    assert!(
        cases.iter().all(|case| case.success),
        "concurrency failures: {:?}",
        cases
            .iter()
            .map(|case| (&case.case, &case.failure))
            .collect::<Vec<_>>()
    );

    for case in &cases {
        assert_eq!(
            case.samples.len(),
            1,
            "expected one measured sample per case"
        );
        let metrics = case.samples[0]
            .metrics
            .as_ref()
            .expect("sample metrics should be present");
        assert!(
            metrics.schema_hash.is_some(),
            "concurrency metrics should carry schema_hash for '{}'",
            case.case
        );
        let contention = metrics
            .contention
            .as_ref()
            .expect("contention metrics should be present");
        let expected_attempts = match case.case.as_str() {
            "concurrent_table_create" => CREATE_WORKERS,
            "concurrent_append_multi" => APPEND_WORKERS,
            "update_vs_compaction" | "delete_vs_compaction" | "optimize_vs_optimize_overlap" => {
                CONTENDED_ATTEMPTS
            }
            other => panic!("unexpected case id: {other}"),
        };
        assert_eq!(
            contention.ops_attempted, expected_attempts,
            "unexpected fixed work for case '{}'",
            case.case
        );
        match case.case.as_str() {
            "concurrent_table_create" | "concurrent_append_multi" => assert!(
                metrics.table_version.is_some(),
                "shared-table concurrency case '{}' should report table_version",
                case.case
            ),
            "update_vs_compaction" | "delete_vs_compaction" | "optimize_vs_optimize_overlap" => {
                assert!(
                    metrics.table_version.is_none(),
                    "cloned-table concurrency case '{}' should omit table_version",
                    case.case
                );
                // Contended cases race 2 workers across 3 independent copies, so
                // at least one conflict should be observed in practice.
                assert!(
                    contention.ops_failed > 0,
                    "contended case '{}' should observe at least one conflict; \
                     got ops_failed=0 (attempted={}, succeeded={})",
                    case.case,
                    contention.ops_attempted,
                    contention.ops_succeeded,
                );
            }
            other => panic!("unexpected case id: {other}"),
        }
    }
}
