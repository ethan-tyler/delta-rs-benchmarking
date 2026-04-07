use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use delta_bench::data::fixtures::{generate_fixtures_with_profile, FixtureProfile};
use delta_bench::storage::StorageConfig;
use delta_bench::suites::metadata_perf::{
    benchmark_case_spec, benchmark_clone_plain_snapshot, benchmark_load_case,
    benchmark_provider_from_snapshot, benchmark_snapshot_at_version, MetadataReplayCaseSpec,
    MetadataReplayVariant,
};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

struct BenchState {
    runtime: Runtime,
    fixtures: TempDir,
    storage: StorageConfig,
    checkpointed_head: MetadataReplayCaseSpec,
    uncheckpointed_head: MetadataReplayCaseSpec,
    long_history: MetadataReplayCaseSpec,
}

fn build_state() -> BenchState {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let fixtures = tempfile::tempdir().expect("fixtures tempdir");
    let storage = StorageConfig::local();
    runtime
        .block_on(generate_fixtures_with_profile(
            fixtures.path(),
            "sf1",
            42,
            true,
            FixtureProfile::ManyVersions,
            &storage,
        ))
        .expect("generate metadata replay fixtures");

    let checkpointed_head = benchmark_case_spec(
        fixtures.path(),
        "sf1",
        MetadataReplayVariant::CheckpointedHead,
        &storage,
    )
    .expect("checkpointed replay spec");
    let uncheckpointed_head = benchmark_case_spec(
        fixtures.path(),
        "sf1",
        MetadataReplayVariant::UncheckpointedHead,
        &storage,
    )
    .expect("uncheckpointed replay spec");
    let long_history = benchmark_case_spec(
        fixtures.path(),
        "sf1",
        MetadataReplayVariant::LongHistory,
        &storage,
    )
    .expect("long-history replay spec");

    BenchState {
        runtime,
        fixtures,
        storage,
        checkpointed_head,
        uncheckpointed_head,
        long_history,
    }
}

fn bench_metadata_replay_internals(c: &mut Criterion) {
    let state = build_state();
    let mut group = c.benchmark_group("metadata_replay");

    group.bench_function(
        BenchmarkId::new("snapshot_clone", "checkpointed_head"),
        |b| {
            b.iter_batched(
                || {
                    state
                        .runtime
                        .block_on(benchmark_load_case(
                            &state.storage,
                            state.checkpointed_head.clone(),
                        ))
                        .expect("load checkpointed replay case")
                },
                |loaded| {
                    let snapshot = benchmark_clone_plain_snapshot(&loaded)
                        .expect("clone checkpointed snapshot");
                    black_box(snapshot);
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function(
        BenchmarkId::new("provider_from_snapshot", "checkpointed_head"),
        |b| {
            b.iter_batched(
                || {
                    state
                        .runtime
                        .block_on(benchmark_load_case(
                            &state.storage,
                            state.checkpointed_head.clone(),
                        ))
                        .expect("load checkpointed replay case")
                },
                |loaded| {
                    let snapshot = benchmark_clone_plain_snapshot(&loaded)
                        .expect("clone checkpointed snapshot");
                    let provider = state
                        .runtime
                        .block_on(benchmark_provider_from_snapshot(&loaded, snapshot))
                        .expect("provider from checkpointed snapshot");
                    black_box(provider);
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function(
        BenchmarkId::new("provider_from_snapshot", "uncheckpointed_head"),
        |b| {
            b.iter_batched(
                || {
                    state
                        .runtime
                        .block_on(benchmark_load_case(
                            &state.storage,
                            state.uncheckpointed_head.clone(),
                        ))
                        .expect("load uncheckpointed replay case")
                },
                |loaded| {
                    let snapshot = benchmark_clone_plain_snapshot(&loaded)
                        .expect("clone uncheckpointed snapshot");
                    let provider = state
                        .runtime
                        .block_on(benchmark_provider_from_snapshot(&loaded, snapshot))
                        .expect("provider from uncheckpointed snapshot");
                    black_box(provider);
                },
                BatchSize::PerIteration,
            );
        },
    );

    group.bench_function(BenchmarkId::new("log_replay", "long_history_v0"), |b| {
        b.iter_batched(
            || {
                state
                    .runtime
                    .block_on(benchmark_load_case(
                        &state.storage,
                        state.long_history.clone(),
                    ))
                    .expect("load long-history replay case")
            },
            |loaded| {
                let snapshot = state
                    .runtime
                    .block_on(benchmark_snapshot_at_version(&loaded, 0))
                    .expect("snapshot at version zero");
                black_box(snapshot);
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
    black_box(state.fixtures.path());
}

criterion_group!(benches, bench_metadata_replay_internals);
criterion_main!(benches);
