#[path = "support/env_vars.rs"]
mod env_vars_support;

use std::future::Future;
use std::sync::OnceLock;
use std::time::Instant;

use delta_bench::cli::TimingPhase;
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::scan;
use tokio::sync::Mutex;

use env_vars_support::with_env_vars;

const PHASE_DELAY_MS: f64 = 250.0;
const PHASE_SAMPLE_RUNS: usize = 5;
const TARGET_DELAY_TOLERANCE_MS: f64 = 40.0;
const CONTROL_DRIFT_TOLERANCE_MS: f64 = 75.0;

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    if values.len() % 2 == 0 {
        (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.0
    } else {
        values[values.len() / 2]
    }
}

fn env_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn run_with_optional_env<F, Fut, T>(entries: &[(&str, &str)], op: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    if entries.is_empty() {
        op().await
    } else {
        with_env_vars(entries, op).await
    }
}

async fn measure_operation_elapsed_ms<F, Fut, T>(
    entries: &[(&str, &str)],
    op: F,
) -> delta_bench::error::BenchResult<f64>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = delta_bench::error::BenchResult<T>>,
{
    let start = Instant::now();
    run_with_optional_env(entries, op).await?;
    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

async fn measure_load_phase_once(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let spec = scan::benchmark_case_spec(fixtures_dir, "sf1", "scan_filter_flag", storage)
        .expect("resolve scan case");
    measure_operation_elapsed_ms(entries, || async move {
        scan::benchmark_load_case(storage, spec).await
    })
    .await
    .expect("load phase should succeed")
}

async fn measure_load_phase(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let _env_guard = env_mutex().lock().await;
    let mut elapsed = Vec::with_capacity(PHASE_SAMPLE_RUNS);
    for _ in 0..PHASE_SAMPLE_RUNS {
        elapsed.push(measure_load_phase_once(fixtures_dir, storage, entries).await);
    }
    median(&mut elapsed)
}

async fn measure_plan_phase_once(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let spec = scan::benchmark_case_spec(fixtures_dir, "sf1", "scan_filter_flag", storage)
        .expect("resolve scan case");
    let sql = scan::benchmark_case_sql(&spec);
    let loaded = scan::benchmark_load_case(storage, spec)
        .await
        .expect("load scan case");
    measure_operation_elapsed_ms(entries, || async move {
        scan::benchmark_plan_case(loaded, sql).await
    })
    .await
    .expect("plan phase should succeed")
}

async fn measure_plan_phase(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let _env_guard = env_mutex().lock().await;
    let mut elapsed = Vec::with_capacity(PHASE_SAMPLE_RUNS);
    for _ in 0..PHASE_SAMPLE_RUNS {
        elapsed.push(measure_plan_phase_once(fixtures_dir, storage, entries).await);
    }
    median(&mut elapsed)
}

async fn measure_execute_phase_once(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let spec = scan::benchmark_case_spec(fixtures_dir, "sf1", "scan_filter_flag", storage)
        .expect("resolve scan case");
    let sql = scan::benchmark_case_sql(&spec);
    let loaded = scan::benchmark_load_case(storage, spec)
        .await
        .expect("load scan case");
    let prepared = scan::benchmark_plan_case(loaded, sql)
        .await
        .expect("plan scan case");
    measure_operation_elapsed_ms(entries, || async move {
        scan::benchmark_execute_case(prepared).await
    })
    .await
    .expect("execute phase should succeed")
}

async fn measure_execute_phase(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let _env_guard = env_mutex().lock().await;
    let mut elapsed = Vec::with_capacity(PHASE_SAMPLE_RUNS);
    for _ in 0..PHASE_SAMPLE_RUNS {
        elapsed.push(measure_execute_phase_once(fixtures_dir, storage, entries).await);
    }
    median(&mut elapsed)
}

async fn measure_validate_phase_once(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let spec = scan::benchmark_case_spec(fixtures_dir, "sf1", "scan_filter_flag", storage)
        .expect("resolve scan case");
    let sql = scan::benchmark_case_sql(&spec);
    let loaded = scan::benchmark_load_case(storage, spec)
        .await
        .expect("load scan case");
    let prepared = scan::benchmark_plan_case(loaded, sql)
        .await
        .expect("plan scan case");
    let executed = scan::benchmark_execute_case(prepared)
        .await
        .expect("execute scan case");
    measure_operation_elapsed_ms(entries, || async move {
        scan::benchmark_validate_case(executed).await
    })
    .await
    .expect("validate phase should succeed")
}

async fn measure_validate_phase(
    fixtures_dir: &std::path::Path,
    storage: &StorageConfig,
    entries: &[(&str, &str)],
) -> f64 {
    let _env_guard = env_mutex().lock().await;
    let mut elapsed = Vec::with_capacity(PHASE_SAMPLE_RUNS);
    for _ in 0..PHASE_SAMPLE_RUNS {
        elapsed.push(measure_validate_phase_once(fixtures_dir, storage, entries).await);
    }
    median(&mut elapsed)
}

fn assert_target_phase_shift(label: &str, baseline: f64, delayed: f64, expected_delay_ms: f64) {
    assert!(
        delayed - baseline >= expected_delay_ms - TARGET_DELAY_TOLERANCE_MS,
        "{label} delay should move timing by about {expected_delay_ms} ms; baseline={baseline:.3}, delayed={delayed:.3}"
    );
}

fn assert_control_phase_stable(label: &str, baseline: f64, delayed: f64) {
    assert!(
        (delayed - baseline).abs() <= CONTROL_DRIFT_TOLERANCE_MS,
        "{label} delay leaked into unrelated timing; baseline={baseline:.3}, delayed={delayed:.3}"
    );
}

#[tokio::test]
async fn plan_delay_requires_explicit_validation_opt_in() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let _env_guard = env_mutex().lock().await;
    let case = with_env_vars(
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", ""),
            ("DELTA_BENCH_SCAN_DELAY_PLAN_MS", "150"),
        ],
        || async {
            scan::run_single_case(
                temp.path(),
                "sf1",
                "scan_filter_flag",
                TimingPhase::Plan,
                &storage,
            )
            .await
        },
    )
    .await
    .expect("scan case should complete with a failure payload");

    assert!(
        !case.success,
        "delay injection should fail closed without opt-in"
    );
    let failure = case.failure.expect("expected failure payload");
    assert!(
        failure
            .message
            .contains("validation-only scan phase delay injection requires DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[tokio::test]
async fn load_delay_canary_only_moves_load_timing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let baseline_load = measure_load_phase(temp.path(), &storage, &[]).await;
    let baseline_execute = measure_execute_phase(temp.path(), &storage, &[]).await;

    let delayed_load = measure_load_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_LOAD_MS", "250"),
        ],
    )
    .await;
    let delayed_execute = measure_execute_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_LOAD_MS", "250"),
        ],
    )
    .await;

    assert_target_phase_shift("load", baseline_load, delayed_load, PHASE_DELAY_MS);
    assert_control_phase_stable("load", baseline_execute, delayed_execute);
}

#[tokio::test]
async fn plan_delay_canary_only_moves_plan_timing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let baseline_plan = measure_plan_phase(temp.path(), &storage, &[]).await;
    let baseline_execute = measure_execute_phase(temp.path(), &storage, &[]).await;

    let delayed_plan = measure_plan_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_PLAN_MS", "250"),
        ],
    )
    .await;
    let delayed_execute = measure_execute_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_PLAN_MS", "250"),
        ],
    )
    .await;

    assert_target_phase_shift("plan", baseline_plan, delayed_plan, PHASE_DELAY_MS);
    assert_control_phase_stable("plan", baseline_execute, delayed_execute);
}

#[tokio::test]
async fn execute_delay_canary_only_moves_execute_timing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let baseline_execute = measure_execute_phase(temp.path(), &storage, &[]).await;
    let baseline_plan = measure_plan_phase(temp.path(), &storage, &[]).await;

    let delayed_execute = measure_execute_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_EXECUTE_MS", "250"),
        ],
    )
    .await;
    let delayed_plan = measure_plan_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_EXECUTE_MS", "250"),
        ],
    )
    .await;

    assert_target_phase_shift("execute", baseline_execute, delayed_execute, PHASE_DELAY_MS);
    assert_control_phase_stable("execute", baseline_plan, delayed_plan);
}

#[tokio::test]
async fn validate_delay_canary_only_moves_validate_timing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let baseline_validate = measure_validate_phase(temp.path(), &storage, &[]).await;
    let baseline_execute = measure_execute_phase(temp.path(), &storage, &[]).await;

    let delayed_validate = measure_validate_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_VALIDATE_MS", "250"),
        ],
    )
    .await;
    let delayed_execute = measure_execute_phase(
        temp.path(),
        &storage,
        &[
            ("DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY", "1"),
            ("DELTA_BENCH_SCAN_DELAY_VALIDATE_MS", "250"),
        ],
    )
    .await;

    assert_target_phase_shift(
        "validate",
        baseline_validate,
        delayed_validate,
        PHASE_DELAY_MS,
    );
    assert_control_phase_stable("validate", baseline_execute, delayed_execute);
}
