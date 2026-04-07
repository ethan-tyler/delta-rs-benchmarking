use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::scan_replay_support::{
    benchmark_case_spec, benchmark_case_sql, benchmark_clone_plain_snapshot,
    benchmark_control_provider_from_loaded, benchmark_load_case, benchmark_provider_from_snapshot,
    ScanReplayCaseSpec,
};
use delta_bench::storage::StorageConfig;
use deltalake_core::datafusion::datasource::TableProvider;
use deltalake_core::datafusion::prelude::SessionContext;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

struct BenchState {
    runtime: Runtime,
    fixtures: TempDir,
    storage: StorageConfig,
    spec: ScanReplayCaseSpec,
}

fn build_state() -> BenchState {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let fixtures = tempfile::tempdir().expect("fixtures tempdir");
    let storage = StorageConfig::local();
    runtime
        .block_on(generate_fixtures(
            fixtures.path(),
            "sf1",
            42,
            true,
            &storage,
        ))
        .expect("generate fixtures");
    let spec = benchmark_case_spec(fixtures.path(), "sf1", &storage).expect("replay case spec");

    BenchState {
        runtime,
        fixtures,
        storage,
        spec,
    }
}

async fn plan_sql_with_provider(
    sql: &'static str,
    provider: Arc<dyn TableProvider>,
) -> delta_bench::error::BenchResult<()> {
    let ctx = SessionContext::new();
    ctx.register_table("bench", provider)?;
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    black_box(plan);
    Ok(())
}

fn bench_scan_replay_planning(c: &mut Criterion) {
    let state = build_state();
    let sql = benchmark_case_sql(&state.spec);
    let mut group = c.benchmark_group("scan_replay_filter_flag");

    group.bench_function(
        BenchmarkId::new("provider_path", "control_table_provider"),
        |b| {
            b.iter_batched(
                || {
                    state
                        .runtime
                        .block_on(benchmark_load_case(&state.storage, state.spec.clone()))
                        .expect("load planning case")
                },
                |loaded| {
                    let provider = state
                        .runtime
                        .block_on(benchmark_control_provider_from_loaded(&loaded))
                        .expect("control provider");
                    state
                        .runtime
                        .block_on(plan_sql_with_provider(sql, provider))
                        .expect("control planning");
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function(BenchmarkId::new("provider_path", "snapshot_replay"), |b| {
        b.iter_batched(
            || {
                state
                    .runtime
                    .block_on(benchmark_load_case(&state.storage, state.spec.clone()))
                    .expect("load planning case")
            },
            |loaded| {
                let snapshot = benchmark_clone_plain_snapshot(&loaded).expect("plain snapshot");
                let provider = state
                    .runtime
                    .block_on(benchmark_provider_from_snapshot(&loaded, snapshot))
                    .expect("snapshot provider");
                state
                    .runtime
                    .block_on(plan_sql_with_provider(sql, provider))
                    .expect("snapshot planning");
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
    black_box(state.fixtures.path());
}

criterion_group!(benches, bench_scan_replay_planning);
criterion_main!(benches);
