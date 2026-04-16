use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::merge_bench_support::{
    benchmark_early_filter_context, benchmark_generalize_context, timed_generalize_filter,
    timed_try_construct_early_filter, MergeEarlyFilterContext, MergeFilterEarlyVariant,
    MergeFilterGeneralizeVariant, MergeGeneralizeContext,
};
use delta_bench::storage::StorageConfig;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

struct BenchState {
    runtime: Runtime,
    fixtures: TempDir,
    partition_eq_source_target: MergeGeneralizeContext,
    non_partition_eq_minmax: MergeGeneralizeContext,
    localized_partition_expansion: MergeEarlyFilterContext,
    mixed_partition_and_stats: MergeEarlyFilterContext,
    streaming_source: MergeEarlyFilterContext,
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

    let localized_partition_expansion = runtime
        .block_on(benchmark_early_filter_context(
            fixtures.path(),
            "sf1",
            MergeFilterEarlyVariant::LocalizedPartitionExpansion,
            &storage,
        ))
        .expect("localized partition expansion context");
    let mixed_partition_and_stats = runtime
        .block_on(benchmark_early_filter_context(
            fixtures.path(),
            "sf1",
            MergeFilterEarlyVariant::MixedPartitionAndStats,
            &storage,
        ))
        .expect("mixed partition and stats context");
    let streaming_source = runtime
        .block_on(benchmark_early_filter_context(
            fixtures.path(),
            "sf1",
            MergeFilterEarlyVariant::StreamingSource,
            &storage,
        ))
        .expect("streaming source context");

    BenchState {
        runtime,
        fixtures,
        partition_eq_source_target: benchmark_generalize_context(
            MergeFilterGeneralizeVariant::PartitionEqSourceTarget,
        ),
        non_partition_eq_minmax: benchmark_generalize_context(
            MergeFilterGeneralizeVariant::NonPartitionEqMinmax,
        ),
        localized_partition_expansion,
        mixed_partition_and_stats,
        streaming_source,
    }
}

fn bench_merge_filter_planning(c: &mut Criterion) {
    let state = build_state();

    let mut generalize_group = c.benchmark_group("merge_filter/generalize");
    generalize_group.bench_function("partition_eq_source_target", |b| {
        b.iter(|| {
            let outcome = timed_generalize_filter(&state.partition_eq_source_target)
                .expect("partition generalize filter");
            black_box((outcome.rendered_len, outcome.placeholder_count));
        });
    });
    generalize_group.bench_function("non_partition_eq_minmax", |b| {
        b.iter(|| {
            let outcome = timed_generalize_filter(&state.non_partition_eq_minmax)
                .expect("non-partition generalize filter");
            black_box((outcome.rendered_len, outcome.placeholder_count));
        });
    });
    generalize_group.finish();

    let mut early_filter_group = c.benchmark_group("merge_filter/early_filter");
    early_filter_group.measurement_time(Duration::from_secs(8));
    early_filter_group.bench_function("localized_partition_expansion", |b| {
        b.iter(|| {
            let outcome = state
                .runtime
                .block_on(timed_try_construct_early_filter(
                    &state.localized_partition_expansion,
                ))
                .expect("localized early filter");
            black_box(outcome.rendered_len);
        });
    });
    early_filter_group.bench_function("mixed_partition_and_stats", |b| {
        b.iter(|| {
            let outcome = state
                .runtime
                .block_on(timed_try_construct_early_filter(
                    &state.mixed_partition_and_stats,
                ))
                .expect("mixed early filter");
            black_box(outcome.rendered_len);
        });
    });
    early_filter_group.bench_function("streaming_source", |b| {
        b.iter(|| {
            let outcome = state
                .runtime
                .block_on(timed_try_construct_early_filter(&state.streaming_source))
                .expect("streaming-source early filter");
            black_box(outcome.rendered_len);
        });
    });
    early_filter_group.finish();

    black_box(state.fixtures.path());
}

criterion_group!(benches, bench_merge_filter_planning);
criterion_main!(benches);
