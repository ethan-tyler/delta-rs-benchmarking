use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::scan_replay_support;
use delta_bench::storage::StorageConfig;
use deltalake_core::datafusion::prelude::SessionContext;

#[tokio::test]
async fn scan_replay_helpers_clone_plain_snapshot_and_plan_query() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let fixtures_dir = temp.path().join("fixtures");
    let storage = StorageConfig::local();

    generate_fixtures(&fixtures_dir, "sf1", 42, true, &storage)
        .await
        .expect("fixtures should be generated");

    let spec = scan_replay_support::benchmark_case_spec(&fixtures_dir, "sf1", &storage)
        .expect("replay case spec");
    let sql = scan_replay_support::benchmark_case_sql(&spec);
    let loaded = scan_replay_support::benchmark_load_case(&storage, spec)
        .await
        .expect("replay case load");

    let snapshot = scan_replay_support::benchmark_clone_plain_snapshot(&loaded)
        .expect("plain snapshot should clone from loaded state");
    let provider = scan_replay_support::benchmark_provider_from_snapshot(&loaded, snapshot)
        .await
        .expect("provider from plain snapshot");
    let ctx = SessionContext::new();
    ctx.register_table("bench", provider)
        .expect("register provider from plain snapshot");
    let df = ctx.sql(sql).await.expect("planning SQL");
    let plan = df
        .create_physical_plan()
        .await
        .expect("physical plan from plain snapshot provider");
    assert!(
        !plan.schema().fields().is_empty(),
        "expected a physical plan schema"
    );

    let prepared = scan_replay_support::benchmark_plan_case(loaded, sql)
        .await
        .expect("plan benchmark helper");
    let executed = scan_replay_support::benchmark_execute_case(prepared)
        .await
        .expect("execute benchmark helper");
    let metrics = scan_replay_support::benchmark_validate_case(executed)
        .await
        .expect("validate benchmark helper");
    assert!(
        metrics.result_hash.is_some(),
        "replay helper should preserve result hash semantics"
    );
    assert!(
        metrics.schema_hash.is_some(),
        "replay helper should preserve schema hash semantics"
    );
}
