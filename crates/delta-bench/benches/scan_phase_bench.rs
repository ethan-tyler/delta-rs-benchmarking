use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::scan::{
    benchmark_case_spec, benchmark_case_sql, benchmark_execute_case, benchmark_load_case,
    benchmark_plan_case, benchmark_validate_case, ScanCaseSpec,
};
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};

const PR_SENSITIVE_CASES: [&str; 3] = [
    "scan_filter_flag",
    "scan_projection_region",
    "scan_pruning_hit",
];

struct BenchState {
    runtime: Runtime,
    fixtures: TempDir,
    storage: StorageConfig,
    spec: ScanCaseSpec,
}

fn build_state(case_name: &str) -> BenchState {
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
    let spec = benchmark_case_spec(fixtures.path(), "sf1", case_name, &storage).expect("spec");

    BenchState {
        runtime,
        fixtures,
        storage,
        spec,
    }
}

fn bench_scan_case_phases(c: &mut Criterion, case_name: &str) {
    let state = build_state(case_name);
    let sql = benchmark_case_sql(&state.spec);
    let mut group = c.benchmark_group(case_name);

    group.bench_function(BenchmarkId::new("phase", "load"), |b| {
        b.iter_batched(
            || state.spec.clone(),
            |spec| {
                let loaded = state
                    .runtime
                    .block_on(benchmark_load_case(&state.storage, spec))
                    .expect("load phase");
                black_box(loaded);
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function(BenchmarkId::new("phase", "plan"), |b| {
        b.iter_batched(
            || {
                state
                    .runtime
                    .block_on(benchmark_load_case(&state.storage, state.spec.clone()))
                    .expect("load setup")
            },
            |loaded| {
                let prepared = state
                    .runtime
                    .block_on(benchmark_plan_case(loaded, sql))
                    .expect("plan phase");
                black_box(prepared);
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function(BenchmarkId::new("phase", "execute"), |b| {
        b.iter_batched(
            || {
                let loaded = state
                    .runtime
                    .block_on(benchmark_load_case(&state.storage, state.spec.clone()))
                    .expect("load setup");
                state
                    .runtime
                    .block_on(benchmark_plan_case(loaded, sql))
                    .expect("plan setup")
            },
            |prepared| {
                let executed = state
                    .runtime
                    .block_on(benchmark_execute_case(prepared))
                    .expect("execute phase");
                black_box(executed);
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function(BenchmarkId::new("phase", "validate"), |b| {
        b.iter_batched(
            || {
                let loaded = state
                    .runtime
                    .block_on(benchmark_load_case(&state.storage, state.spec.clone()))
                    .expect("load setup");
                let prepared = state
                    .runtime
                    .block_on(benchmark_plan_case(loaded, sql))
                    .expect("plan setup");
                state
                    .runtime
                    .block_on(benchmark_execute_case(prepared))
                    .expect("execute setup")
            },
            |executed| {
                let metrics = state
                    .runtime
                    .block_on(benchmark_validate_case(executed))
                    .expect("validate phase");
                black_box(metrics);
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
    black_box(state.fixtures.path());
}

fn bench_scan_pr_sensitive_phases(c: &mut Criterion) {
    for case_name in PR_SENSITIVE_CASES {
        bench_scan_case_phases(c, case_name);
    }
}

criterion_group!(benches, bench_scan_pr_sensitive_phases);
criterion_main!(benches);
