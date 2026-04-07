use std::fs;
use std::sync::Arc;

use delta_bench::data::fixtures::{generate_fixtures_with_profile, FixtureProfile};
use delta_bench::fingerprint::hash_record_batches_unordered;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::metadata_perf::{
    benchmark_case_spec, benchmark_clone_plain_snapshot, benchmark_control_provider_from_loaded,
    benchmark_has_last_checkpoint_hint, benchmark_load_case, benchmark_provider_from_snapshot,
    benchmark_snapshot_at_version, MetadataReplayVariant,
};
use deltalake_core::datafusion::datasource::TableProvider;
use deltalake_core::datafusion::physical_plan::collect;
use deltalake_core::datafusion::prelude::SessionContext;

async fn provider_query_result_hash(sql: &str, provider: Arc<dyn TableProvider>) -> String {
    let ctx = SessionContext::new();
    ctx.register_table("bench", provider)
        .expect("register provider");
    let df = ctx.sql(sql).await.expect("create dataframe");
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.expect("create plan");
    let batches = collect(plan, task_ctx).await.expect("collect query");
    hash_record_batches_unordered(&batches).expect("hash query results")
}

#[tokio::test]
async fn metadata_replay_bench_contract_keeps_checkpointed_and_uncheckpointed_paths_distinct() {
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

    let checkpointed = benchmark_case_spec(
        temp.path(),
        "sf1",
        MetadataReplayVariant::CheckpointedHead,
        &storage,
    )
    .expect("checkpointed replay spec");
    let uncheckpointed = benchmark_case_spec(
        temp.path(),
        "sf1",
        MetadataReplayVariant::UncheckpointedHead,
        &storage,
    )
    .expect("uncheckpointed replay spec");
    let long_history = benchmark_case_spec(
        temp.path(),
        "sf1",
        MetadataReplayVariant::LongHistory,
        &storage,
    )
    .expect("long-history replay spec");

    assert!(
        benchmark_has_last_checkpoint_hint(&checkpointed),
        "checkpointed replay path should have a _last_checkpoint hint"
    );
    assert!(
        !benchmark_has_last_checkpoint_hint(&uncheckpointed),
        "uncheckpointed replay path should not have a _last_checkpoint hint"
    );
    assert!(
        fs::read_dir(long_history.table_log_dir())
            .expect("metadata long-history log dir")
            .count()
            > 20,
        "long-history replay fixture should contain a measurable transaction history"
    );
    assert!(
        !benchmark_has_last_checkpoint_hint(&long_history),
        "long-history replay fixture should stay checkpoint-free"
    );
    assert!(
        fs::read_dir(long_history.table_log_dir())
            .expect("metadata long-history log dir")
            .filter_map(Result::ok)
            .all(|entry| {
                !entry
                    .file_name()
                    .to_string_lossy()
                    .contains("checkpoint.parquet")
            }),
        "long-history replay fixture should not contain checkpoint parquet artifacts"
    );

    let loaded = benchmark_load_case(&storage, checkpointed)
        .await
        .expect("load checkpointed replay case");
    let snapshot =
        benchmark_clone_plain_snapshot(&loaded).expect("clone plain snapshot from loaded table");
    let provider = benchmark_provider_from_snapshot(&loaded, snapshot)
        .await
        .expect("table provider from snapshot");
    let ctx = SessionContext::new();
    ctx.register_table("bench", provider)
        .expect("register snapshot-backed provider");
    let df = ctx
        .sql("SELECT COUNT(*) FROM bench")
        .await
        .expect("plan SQL over replay provider");
    let plan = df
        .create_physical_plan()
        .await
        .expect("physical plan from metadata replay provider");
    assert!(
        !plan.schema().fields().is_empty(),
        "replay provider should build a plan schema"
    );

    let loaded_long_history = benchmark_load_case(&storage, long_history)
        .await
        .expect("load long-history replay case");
    let latest_provider = benchmark_control_provider_from_loaded(&loaded_long_history)
        .await
        .expect("latest provider from loaded long-history table");
    let version_zero = benchmark_snapshot_at_version(&loaded_long_history, 0)
        .await
        .expect("replay snapshot at version 0");
    let provider = benchmark_provider_from_snapshot(&loaded_long_history, version_zero)
        .await
        .expect("provider from version-zero snapshot");
    let ctx = SessionContext::new();
    ctx.register_table("bench", provider.clone())
        .expect("register version-zero provider");
    let df = ctx
        .sql("SELECT COUNT(*) FROM bench")
        .await
        .expect("plan SQL over version-zero provider");
    let plan = df
        .create_physical_plan()
        .await
        .expect("physical plan from version-zero provider");
    assert!(
        !plan.schema().fields().is_empty(),
        "version-zero replay provider should build a plan schema"
    );
    let latest_hash =
        provider_query_result_hash("SELECT COUNT(*) FROM bench", latest_provider).await;
    let version_zero_hash =
        provider_query_result_hash("SELECT COUNT(*) FROM bench", provider).await;
    assert_ne!(
        latest_hash, version_zero_hash,
        "version-zero replay provider should diverge from the loaded latest provider path"
    );
}

#[test]
fn metadata_replay_bench_profile_is_criterion_only() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let profile = root.join("bench/methodologies/metadata-replay-criterion.env");
    let bench = root.join("crates/delta-bench/benches/metadata_replay_bench.rs");

    let profile_text = fs::read_to_string(&profile).expect("read metadata replay criterion env");
    assert!(
        profile_text.contains("PROFILE_KIND=criterion"),
        "metadata replay profile must stay criterion-only"
    );
    assert!(
        profile_text.contains("CRITERION_BENCH=metadata_replay_bench"),
        "metadata replay profile must target the dedicated criterion bench"
    );
    let bench_text = fs::read_to_string(&bench).expect("read metadata replay bench source");
    for expected in [
        "CheckpointedHead",
        "UncheckpointedHead",
        "LongHistory",
        "criterion_group!",
        "criterion_main!",
    ] {
        assert!(
            bench_text.contains(expected),
            "metadata replay bench missing {expected}"
        );
    }
}
