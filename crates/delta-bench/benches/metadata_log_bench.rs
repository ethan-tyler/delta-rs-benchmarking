use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use delta_bench::data::fixtures::{generate_fixtures_with_profile, FixtureProfile};
use delta_bench::metadata_bench_support::{
    benchmark_case_context, benchmark_case_spec, benchmark_commit_log_bytes,
    benchmark_fileless_snapshot_input, benchmark_materialize_files_from_input,
    benchmark_snapshot_try_new, MetadataLogActionProfile, MetadataLogCaseContext,
};
use delta_bench::storage::StorageConfig;
use deltalake_core::logstore::get_actions;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

struct BenchState {
    runtime: Runtime,
    fixtures: TempDir,
    ctx: MetadataLogCaseContext,
    simple_actions: bytes::Bytes,
    with_stats: bytes::Bytes,
    full_complexity: bytes::Bytes,
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
        .expect("generate metadata log fixtures");

    let spec =
        benchmark_case_spec(fixtures.path(), "sf1", &storage).expect("metadata log case spec");
    let ctx = runtime
        .block_on(benchmark_case_context(&storage, spec))
        .expect("metadata log bench context");

    BenchState {
        runtime,
        fixtures,
        ctx,
        simple_actions: benchmark_commit_log_bytes(MetadataLogActionProfile::SimpleActions, 1_000),
        with_stats: benchmark_commit_log_bytes(MetadataLogActionProfile::WithStats, 1_000),
        full_complexity: benchmark_commit_log_bytes(
            MetadataLogActionProfile::FullComplexity,
            1_000,
        ),
    }
}

fn bench_metadata_log_internals(c: &mut Criterion) {
    let state = build_state();

    let mut actions_group = c.benchmark_group("metadata_log/get_actions");
    actions_group.throughput(Throughput::Elements(1_000));
    actions_group.bench_function("simple_actions_1000", |b| {
        b.iter(|| {
            let actions = get_actions(0, black_box(&state.simple_actions)).expect("parse actions");
            black_box(actions.len());
        });
    });
    actions_group.bench_function("with_stats_1000", |b| {
        b.iter(|| {
            let actions = get_actions(0, black_box(&state.with_stats)).expect("parse actions");
            black_box(actions.len());
        });
    });
    actions_group.bench_function("full_complexity_1000", |b| {
        b.iter(|| {
            let actions = get_actions(0, black_box(&state.full_complexity)).expect("parse actions");
            black_box(actions.len());
        });
    });
    actions_group.finish();

    let mut snapshot_group = c.benchmark_group("metadata_log/snapshot");
    snapshot_group.bench_function("try_new_head_many_versions", |b| {
        b.iter(|| {
            let snapshot = state
                .runtime
                .block_on(benchmark_snapshot_try_new(&state.ctx, None))
                .expect("head snapshot");
            black_box(snapshot.version());
        });
    });
    snapshot_group.bench_function("try_new_version_0_many_versions", |b| {
        b.iter(|| {
            let snapshot = state
                .runtime
                .block_on(benchmark_snapshot_try_new(&state.ctx, Some(0)))
                .expect("version-zero snapshot");
            black_box(snapshot.version());
        });
    });
    snapshot_group.finish();

    let mut eager_snapshot_group = c.benchmark_group("metadata_log/eager_snapshot");
    eager_snapshot_group.bench_function("materialize_files_head_many_versions", |b| {
        b.iter_batched(
            || {
                state
                    .runtime
                    .block_on(benchmark_fileless_snapshot_input(&state.ctx, None))
                    .expect("fileless head snapshot input")
            },
            |input| {
                let materialized = state
                    .runtime
                    .block_on(benchmark_materialize_files_from_input(&state.ctx, input))
                    .expect("materialized head files");
                black_box((materialized.version, materialized.files.len()));
            },
            BatchSize::PerIteration,
        );
    });
    eager_snapshot_group.finish();

    black_box(state.fixtures.path());
}

criterion_group!(benches, bench_metadata_log_internals);
criterion_main!(benches);
