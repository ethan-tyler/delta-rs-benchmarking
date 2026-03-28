use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::datafusion::execution::context::TaskContext;
use deltalake_core::datafusion::physical_plan::collect;
use deltalake_core::datafusion::physical_plan::ExecutionPlan;
use deltalake_core::datafusion::prelude::SessionContext;
use url::Url;

use crate::cli::TimingPhase;
use crate::data::fixtures::{narrow_sales_table_url, read_partitioned_table_url};
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::{hash_arrow_schema, hash_record_batches_unordered};
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics};
use crate::runner::{
    run_case_async_with_timing_phase, CaseExecutionResult, PhaseTiming, TimedSample,
};
use crate::storage::StorageConfig;
use crate::suites::scan_metrics::extract_scan_metrics;

const LOAD_DELAY_ENV: &str = "DELTA_BENCH_SCAN_DELAY_LOAD_MS";
const PLAN_DELAY_ENV: &str = "DELTA_BENCH_SCAN_DELAY_PLAN_MS";
const EXECUTE_DELAY_ENV: &str = "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS";
const VALIDATE_DELAY_ENV: &str = "DELTA_BENCH_SCAN_DELAY_VALIDATE_MS";
const ALLOW_DELAY_ENV: &str = "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY";

pub fn case_names() -> Vec<String> {
    vec![
        "scan_full_narrow".to_string(),
        "scan_projection_region".to_string(),
        "scan_filter_flag".to_string(),
        "scan_pruning_hit".to_string(),
        "scan_pruning_miss".to_string(),
    ]
}

#[doc(hidden)]
#[derive(Clone)]
pub struct ScanCaseSpec {
    table_url: Url,
    sql: &'static str,
}

#[doc(hidden)]
pub struct LoadedSqlQuery {
    ctx: SessionContext,
    total_active_files: Option<u64>,
}

#[doc(hidden)]
pub struct PreparedSqlQuery {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    total_active_files: Option<u64>,
}

#[doc(hidden)]
pub struct ExecutedSqlQuery {
    plan: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    total_active_files: Option<u64>,
    execution_elapsed_ms: f64,
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    timing_phase: TimingPhase,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    let table_url = narrow_sales_table_url(fixtures_dir, scale, storage)?;
    let partitioned_table_url = read_partitioned_table_url(fixtures_dir, scale, storage)?;

    let mut results = Vec::new();

    let full_scan = run_query_case(
        "scan_full_narrow",
        timing_phase,
        warmup,
        iterations,
        storage,
        table_url.clone(),
        "SELECT COUNT(*) FROM bench",
    )
    .await;
    results.push(into_case_result(full_scan));

    let projection = run_query_case(
        "scan_projection_region",
        timing_phase,
        warmup,
        iterations,
        storage,
        table_url.clone(),
        "SELECT region, SUM(value_i64) FROM bench GROUP BY region",
    )
    .await;
    results.push(into_case_result(projection));

    let filtered = run_query_case(
        "scan_filter_flag",
        timing_phase,
        warmup,
        iterations,
        storage,
        table_url.clone(),
        "SELECT COUNT(*) FROM bench WHERE flag = true AND value_i64 > 0",
    )
    .await;
    results.push(into_case_result(filtered));

    let partition_hit = run_query_case(
        "scan_pruning_hit",
        timing_phase,
        warmup,
        iterations,
        storage,
        partitioned_table_url.clone(),
        "SELECT COUNT(*) FROM bench WHERE region = 'us'",
    )
    .await;
    results.push(into_case_result(partition_hit));

    let partition_miss = run_query_case(
        "scan_pruning_miss",
        timing_phase,
        warmup,
        iterations,
        storage,
        partitioned_table_url,
        "SELECT COUNT(*) FROM bench",
    )
    .await;
    results.push(into_case_result(partition_miss));

    Ok(results)
}

pub async fn run_single_case(
    fixtures_dir: &Path,
    scale: &str,
    case_name: &str,
    timing_phase: TimingPhase,
    storage: &StorageConfig,
) -> BenchResult<CaseResult> {
    let (table_url, sql) = resolve_case_spec(fixtures_dir, scale, case_name, storage)?;

    Ok(into_case_result(
        run_query_case(case_name, timing_phase, 0, 1, storage, table_url, sql).await,
    ))
}

#[doc(hidden)]
pub fn benchmark_case_spec(
    fixtures_dir: &Path,
    scale: &str,
    case_name: &str,
    storage: &StorageConfig,
) -> BenchResult<ScanCaseSpec> {
    let (table_url, sql) = resolve_case_spec(fixtures_dir, scale, case_name, storage)?;
    Ok(ScanCaseSpec { table_url, sql })
}

#[doc(hidden)]
pub async fn benchmark_load_case(
    storage: &StorageConfig,
    spec: ScanCaseSpec,
) -> BenchResult<LoadedSqlQuery> {
    load_sql_query_context(storage, spec.table_url).await
}

#[doc(hidden)]
pub async fn benchmark_plan_case(
    loaded: LoadedSqlQuery,
    sql: &'static str,
) -> BenchResult<PreparedSqlQuery> {
    plan_loaded_sql_query(loaded, sql).await
}

#[doc(hidden)]
pub async fn benchmark_execute_case(prepared: PreparedSqlQuery) -> BenchResult<ExecutedSqlQuery> {
    execute_prepared_query(prepared).await
}

#[doc(hidden)]
pub async fn benchmark_validate_case(executed: ExecutedSqlQuery) -> BenchResult<SampleMetrics> {
    let (metrics, _) = validate_executed_query(executed).await?;
    Ok(metrics)
}

#[doc(hidden)]
pub fn benchmark_case_sql(spec: &ScanCaseSpec) -> &'static str {
    spec.sql
}

fn resolve_case_spec(
    fixtures_dir: &Path,
    scale: &str,
    case_name: &str,
    storage: &StorageConfig,
) -> BenchResult<(Url, &'static str)> {
    match case_name {
        "scan_full_narrow" => Ok((
            narrow_sales_table_url(fixtures_dir, scale, storage)?,
            "SELECT COUNT(*) FROM bench",
        )),
        "scan_projection_region" => Ok((
            narrow_sales_table_url(fixtures_dir, scale, storage)?,
            "SELECT region, SUM(value_i64) FROM bench GROUP BY region",
        )),
        "scan_filter_flag" => Ok((
            narrow_sales_table_url(fixtures_dir, scale, storage)?,
            "SELECT COUNT(*) FROM bench WHERE flag = true AND value_i64 > 0",
        )),
        "scan_pruning_hit" => Ok((
            read_partitioned_table_url(fixtures_dir, scale, storage)?,
            "SELECT COUNT(*) FROM bench WHERE region = 'us'",
        )),
        "scan_pruning_miss" => Ok((
            read_partitioned_table_url(fixtures_dir, scale, storage)?,
            "SELECT COUNT(*) FROM bench",
        )),
        other => Err(crate::error::BenchError::InvalidArgument(format!(
            "unknown scan case '{other}'"
        ))),
    }
}

async fn run_query_case(
    case_name: &str,
    timing_phase: TimingPhase,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
    table_url: Url,
    sql: &'static str,
) -> CaseExecutionResult {
    run_case_async_with_timing_phase(case_name, warmup, iterations, timing_phase, || {
        let storage = storage.clone();
        let table_url = table_url.clone();
        async move {
            let load_start = std::time::Instant::now();
            let loaded = load_sql_query_context(&storage, table_url)
                .await
                .map_err(|e| e.to_string())?;
            let load_elapsed_ms = load_start.elapsed().as_secs_f64() * 1000.0;

            let planning_start = std::time::Instant::now();
            let prepared = plan_loaded_sql_query(loaded, sql)
                .await
                .map_err(|e| e.to_string())?;
            let planning_elapsed_ms = planning_start.elapsed().as_secs_f64() * 1000.0;

            let executed = execute_prepared_query(prepared)
                .await
                .map_err(|e| e.to_string())?;
            let execution_elapsed_ms = executed.execution_elapsed_ms;

            let (metrics, validate_elapsed_ms) = validate_executed_query(executed)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<TimedSample<SampleMetrics>, String>(TimedSample::new(
                metrics,
                PhaseTiming::default()
                    .with_load_ms(load_elapsed_ms)
                    .with_plan_ms(planning_elapsed_ms)
                    .with_execute_ms(execution_elapsed_ms)
                    .with_validate_ms(validate_elapsed_ms),
            ))
        }
    })
    .await
}

async fn load_sql_query_context(
    storage: &StorageConfig,
    table_url: Url,
) -> BenchResult<LoadedSqlQuery> {
    apply_phase_delay(LOAD_DELAY_ENV).await?;
    let table = storage.open_table(table_url).await?;
    let total_active_files = table
        .snapshot()
        .ok()
        .map(|snapshot| snapshot.log_data().num_files() as u64);
    let ctx = SessionContext::new();
    ctx.register_table("bench", table.table_provider().await?)?;

    Ok(LoadedSqlQuery {
        ctx,
        total_active_files,
    })
}

async fn plan_loaded_sql_query(loaded: LoadedSqlQuery, sql: &str) -> BenchResult<PreparedSqlQuery> {
    apply_phase_delay(PLAN_DELAY_ENV).await?;
    let df = loaded.ctx.sql(sql).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;

    Ok(PreparedSqlQuery {
        plan,
        task_ctx,
        total_active_files: loaded.total_active_files,
    })
}

async fn execute_prepared_query(prepared: PreparedSqlQuery) -> BenchResult<ExecutedSqlQuery> {
    let query_start = std::time::Instant::now();
    apply_phase_delay(EXECUTE_DELAY_ENV).await?;
    let batches = collect(prepared.plan.clone(), prepared.task_ctx).await?;
    let query_elapsed_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    Ok(ExecutedSqlQuery {
        plan: prepared.plan,
        batches,
        total_active_files: prepared.total_active_files,
        execution_elapsed_ms: query_elapsed_ms,
    })
}

async fn validate_executed_query(executed: ExecutedSqlQuery) -> BenchResult<(SampleMetrics, f64)> {
    let validate_start = std::time::Instant::now();
    apply_phase_delay(VALIDATE_DELAY_ENV).await?;
    let rows_processed = executed
        .batches
        .iter()
        .map(|b| b.num_rows() as u64)
        .sum::<u64>();
    let scan_metrics = extract_scan_metrics(&executed.plan);
    let files_scanned = scan_metrics.files_scanned.or_else(|| {
        executed.total_active_files.and_then(|total| {
            scan_metrics
                .files_pruned
                .and_then(|pruned| total.checked_sub(pruned))
        })
    });
    let files_pruned = scan_metrics.files_pruned.or_else(|| {
        executed
            .total_active_files
            .and_then(|total| files_scanned.and_then(|scanned| total.checked_sub(scanned)))
    });
    let result_hash = hash_record_batches_unordered(&executed.batches)?;
    let schema_hash = hash_arrow_schema(executed.plan.schema().as_ref())?;
    let validate_elapsed_ms = validate_start.elapsed().as_secs_f64() * 1000.0;

    Ok((
        SampleMetrics::base(Some(rows_processed), None, None, None)
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned,
                files_pruned,
                bytes_scanned: scan_metrics.bytes_scanned,
                scan_time_ms: scan_metrics.scan_time_ms,
                rewrite_time_ms: None,
            })
            .with_runtime_io(RuntimeIOMetrics {
                peak_rss_mb: None,
                cpu_time_ms: None,
                bytes_read: None,
                bytes_written: None,
                files_touched: None,
                files_skipped: None,
                spill_bytes: None,
                result_hash: Some(result_hash),
                schema_hash: Some(schema_hash),
                semantic_state_digest: None,
                validation_summary: None,
            }),
        validate_elapsed_ms,
    ))
}

async fn apply_phase_delay(env_name: &str) -> BenchResult<()> {
    let Some(delay) = parse_phase_delay(env_name)? else {
        return Ok(());
    };
    // Validation-only canaries can inject a fixed delay into one scan phase to prove timing isolation.
    tokio::time::sleep(delay).await;
    Ok(())
}

fn parse_phase_delay(env_name: &str) -> BenchResult<Option<Duration>> {
    let Some(raw) = std::env::var(env_name).ok() else {
        return Ok(None);
    };
    if std::env::var(ALLOW_DELAY_ENV).as_deref() != Ok("1") {
        return Err(BenchError::InvalidArgument(format!(
            "validation-only scan phase delay injection requires {ALLOW_DELAY_ENV}=1"
        )));
    }
    let millis = raw.parse::<u64>().map_err(|_| {
        BenchError::InvalidArgument(format!(
            "{env_name} must be an unsigned integer number of milliseconds"
        ))
    })?;
    Ok(Some(Duration::from_millis(millis)))
}

fn into_case_result(result: CaseExecutionResult) -> CaseResult {
    match result {
        CaseExecutionResult::Success(case) | CaseExecutionResult::Failure(case) => case,
    }
}
