use std::path::Path;
use std::sync::Arc;

use deltalake_core::datafusion::execution::context::TaskContext;
use deltalake_core::datafusion::physical_plan::collect;
use deltalake_core::datafusion::physical_plan::ExecutionPlan;
use deltalake_core::datafusion::prelude::SessionContext;
use url::Url;

use crate::cli::TimingPhase;
use crate::data::fixtures::{narrow_sales_table_url, read_partitioned_table_url};
use crate::error::BenchResult;
use crate::fingerprint::{hash_arrow_schema, hash_record_batches_unordered};
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics};
use crate::runner::{
    run_case_async_with_timing_phase, CaseExecutionResult, PhaseTiming, TimedSample,
};
use crate::storage::StorageConfig;
use crate::suites::scan_metrics::extract_scan_metrics;

pub fn case_names() -> Vec<String> {
    vec![
        "scan_full_narrow".to_string(),
        "scan_projection_region".to_string(),
        "scan_filter_flag".to_string(),
        "scan_pruning_hit".to_string(),
        "scan_pruning_miss".to_string(),
    ]
}

struct PreparedSqlQuery {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    total_active_files: Option<u64>,
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
            let planning_start = std::time::Instant::now();
            let prepared = prepare_sql_query(&storage, table_url, sql)
                .await
                .map_err(|e| e.to_string())?;
            let planning_elapsed_ms = planning_start.elapsed().as_secs_f64() * 1000.0;
            let (metrics, execution_elapsed_ms) = execute_prepared_query(prepared)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<TimedSample<SampleMetrics>, String>(TimedSample::new(
                metrics,
                PhaseTiming::default()
                    .with_plan_ms(planning_elapsed_ms)
                    .with_execute_ms(execution_elapsed_ms),
            ))
        }
    })
    .await
}

async fn prepare_sql_query(
    storage: &StorageConfig,
    table_url: Url,
    sql: &str,
) -> BenchResult<PreparedSqlQuery> {
    let table = storage.open_table(table_url).await?;
    let total_active_files = table
        .snapshot()
        .ok()
        .map(|snapshot| snapshot.log_data().num_files() as u64);
    let ctx = SessionContext::new();
    ctx.register_table("bench", table.table_provider().await?)?;
    let df = ctx.sql(sql).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;

    Ok(PreparedSqlQuery {
        plan,
        task_ctx,
        total_active_files,
    })
}

async fn execute_prepared_query(prepared: PreparedSqlQuery) -> BenchResult<(SampleMetrics, f64)> {
    let query_start = std::time::Instant::now();
    let batches = collect(prepared.plan.clone(), prepared.task_ctx).await?;
    let query_elapsed_ms = query_start.elapsed().as_secs_f64() * 1000.0;
    let rows_processed = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    let scan_metrics = extract_scan_metrics(&prepared.plan);
    let files_pruned = scan_metrics.files_pruned.or_else(|| {
        prepared.total_active_files.and_then(|total| {
            scan_metrics
                .files_scanned
                .and_then(|scanned| total.checked_sub(scanned))
        })
    });
    let result_hash = hash_record_batches_unordered(&batches)?;
    let schema_hash = hash_arrow_schema(prepared.plan.schema().as_ref())?;

    Ok((
        SampleMetrics::base(Some(rows_processed), None, None, None)
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: scan_metrics.files_scanned,
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
            }),
        query_elapsed_ms,
    ))
}

fn into_case_result(result: CaseExecutionResult) -> CaseResult {
    match result {
        CaseExecutionResult::Success(case) | CaseExecutionResult::Failure(case) => case,
    }
}
