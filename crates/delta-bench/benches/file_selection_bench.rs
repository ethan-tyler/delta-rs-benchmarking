use criterion::{black_box, criterion_group, criterion_main, Criterion};
use delta_bench::data::fixtures::generate_file_selection_fixtures;
use delta_bench::file_selection_bench_support::{
    benchmark_case_context, benchmark_case_spec, timed_find_files, timed_find_files_scan,
    timed_scan_files_where_matches, FileSelectionCaseContext, FileSelectionVariant,
};
use delta_bench::storage::StorageConfig;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

struct BenchState {
    runtime: Runtime,
    fixtures: TempDir,
    partition_only: FileSelectionCaseContext,
    delete_data_predicate: FileSelectionCaseContext,
    update_data_predicate: FileSelectionCaseContext,
}

fn build_context(
    runtime: &Runtime,
    fixtures: &TempDir,
    storage: &StorageConfig,
    variant: FileSelectionVariant,
) -> FileSelectionCaseContext {
    let spec = benchmark_case_spec(fixtures.path(), "sf1", variant, storage)
        .expect("file-selection case spec");
    runtime
        .block_on(benchmark_case_context(storage, spec))
        .expect("file-selection case context")
}

fn build_state() -> BenchState {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let fixtures = tempfile::tempdir().expect("fixtures tempdir");
    let storage = StorageConfig::local();
    runtime
        .block_on(generate_file_selection_fixtures(
            fixtures.path(),
            "sf1",
            42,
            true,
            &storage,
        ))
        .expect("generate fixtures");

    let partition_only = build_context(
        &runtime,
        &fixtures,
        &storage,
        FileSelectionVariant::PartitionOnly,
    );
    let delete_data_predicate = build_context(
        &runtime,
        &fixtures,
        &storage,
        FileSelectionVariant::DeleteDataPredicate,
    );
    let update_data_predicate = build_context(
        &runtime,
        &fixtures,
        &storage,
        FileSelectionVariant::UpdateDataPredicate,
    );

    BenchState {
        runtime,
        fixtures,
        partition_only,
        delete_data_predicate,
        update_data_predicate,
    }
}

fn bench_file_selection_internals(c: &mut Criterion) {
    let state = build_state();

    let mut partition_group = c.benchmark_group("file_selection/partition_only");
    partition_group.bench_function("find_files", |b| {
        b.iter(|| {
            let result = state
                .runtime
                .block_on(timed_find_files(&state.partition_only))
                .expect("partition-only find_files");
            black_box((result.candidate_count, result.partition_scan));
        });
    });
    partition_group.finish();

    let mut data_group = c.benchmark_group("file_selection/data_predicate");
    data_group.bench_function("find_files_scan", |b| {
        b.iter(|| {
            let result = state
                .runtime
                .block_on(timed_find_files_scan(&state.delete_data_predicate))
                .expect("data-predicate find_files_scan");
            black_box(result.candidate_count);
        });
    });
    data_group.bench_function("scan_files_where_matches_delete", |b| {
        b.iter(|| {
            let result = state
                .runtime
                .block_on(timed_scan_files_where_matches(&state.delete_data_predicate))
                .expect("delete matched-files scan");
            black_box(result.candidate_count);
        });
    });
    data_group.bench_function("scan_files_where_matches_update", |b| {
        b.iter(|| {
            let result = state
                .runtime
                .block_on(timed_scan_files_where_matches(&state.update_data_predicate))
                .expect("update matched-files scan");
            black_box(result.candidate_count);
        });
    });
    data_group.finish();

    black_box(state.fixtures.path());
}

criterion_group!(benches, bench_file_selection_internals);
criterion_main!(benches);
