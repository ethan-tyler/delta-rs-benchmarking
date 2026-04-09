use std::sync::Arc;

use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::datafusion::datasource::TableProvider;
use deltalake_core::datafusion::execution::context::TaskContext;
use deltalake_core::datafusion::physical_plan::collect;
use deltalake_core::datafusion::physical_plan::ExecutionPlan;
use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::kernel::Snapshot;
use deltalake_core::DeltaTable;
use url::Url;

use crate::data::fixtures::narrow_sales_table_url;
use crate::error::BenchResult;
use crate::fingerprint::{hash_arrow_schema, hash_record_batches_unordered};
use crate::replay_snapshot::clone_plain_snapshot_from_loaded_table;
use crate::results::{RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics};
use crate::storage::StorageConfig;
use crate::suites::scan_metrics::extract_scan_metrics;
use crate::version_compat::snapshot_version_arg;

const REPLAY_PROBE_SQL: &str = "SELECT COUNT(*) FROM bench WHERE flag = true AND value_i64 > 0";

#[doc(hidden)]
#[derive(Clone)]
pub struct ScanReplayCaseSpec {
    table_url: Url,
    sql: &'static str,
}

#[doc(hidden)]
pub struct LoadedReplayQuery {
    table: DeltaTable,
    total_active_files: Option<u64>,
}

#[doc(hidden)]
pub struct PreparedReplayQuery {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    total_active_files: Option<u64>,
}

#[doc(hidden)]
pub struct ExecutedReplayQuery {
    plan: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    total_active_files: Option<u64>,
}

#[doc(hidden)]
pub fn benchmark_case_spec(
    fixtures_dir: &std::path::Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<ScanReplayCaseSpec> {
    Ok(ScanReplayCaseSpec {
        table_url: narrow_sales_table_url(fixtures_dir, scale, storage)?,
        sql: REPLAY_PROBE_SQL,
    })
}

#[doc(hidden)]
pub fn benchmark_case_sql(spec: &ScanReplayCaseSpec) -> &'static str {
    spec.sql
}

#[doc(hidden)]
pub async fn benchmark_load_case(
    storage: &StorageConfig,
    spec: ScanReplayCaseSpec,
) -> BenchResult<LoadedReplayQuery> {
    load_replay_query(storage, spec.table_url).await
}

#[doc(hidden)]
pub fn benchmark_clone_plain_snapshot(loaded: &LoadedReplayQuery) -> BenchResult<Snapshot> {
    clone_plain_snapshot_from_loaded_table(&loaded.table)
}

#[doc(hidden)]
pub async fn benchmark_provider_from_snapshot(
    loaded: &LoadedReplayQuery,
    snapshot: Snapshot,
) -> BenchResult<Arc<dyn TableProvider>> {
    Ok(loaded
        .table
        .table_provider()
        .with_snapshot(snapshot)
        .await?)
}

#[doc(hidden)]
pub async fn benchmark_control_provider_from_loaded(
    loaded: &LoadedReplayQuery,
) -> BenchResult<Arc<dyn TableProvider>> {
    Ok(loaded.table.table_provider().await?)
}

#[doc(hidden)]
pub async fn benchmark_snapshot_at_version(
    loaded: &LoadedReplayQuery,
    version: u64,
) -> BenchResult<Snapshot> {
    Ok(Snapshot::try_new(
        loaded.table.log_store().as_ref(),
        Default::default(),
        Some(snapshot_version_arg(version)?),
    )
    .await?)
}

#[doc(hidden)]
pub async fn benchmark_plan_case(
    loaded: LoadedReplayQuery,
    sql: &'static str,
) -> BenchResult<PreparedReplayQuery> {
    plan_loaded_query(loaded, sql).await
}

#[doc(hidden)]
pub async fn benchmark_execute_case(
    prepared: PreparedReplayQuery,
) -> BenchResult<ExecutedReplayQuery> {
    execute_prepared_query(prepared).await
}

#[doc(hidden)]
pub async fn benchmark_validate_case(executed: ExecutedReplayQuery) -> BenchResult<SampleMetrics> {
    let (metrics, _) = validate_executed_query(executed).await?;
    Ok(metrics)
}

async fn load_replay_query(
    storage: &StorageConfig,
    table_url: Url,
) -> BenchResult<LoadedReplayQuery> {
    let table = storage.open_table(table_url).await?;
    let total_active_files = table
        .snapshot()
        .ok()
        .map(|snapshot| snapshot.log_data().num_files() as u64);

    Ok(LoadedReplayQuery {
        table,
        total_active_files,
    })
}

async fn plan_loaded_query(
    loaded: LoadedReplayQuery,
    sql: &'static str,
) -> BenchResult<PreparedReplayQuery> {
    let snapshot = clone_plain_snapshot_from_loaded_table(&loaded.table)?;
    let provider = loaded
        .table
        .table_provider()
        .with_snapshot(snapshot)
        .await?;

    let ctx = SessionContext::new();
    ctx.register_table("bench", provider)?;
    let df = ctx.sql(sql).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;

    Ok(PreparedReplayQuery {
        plan,
        task_ctx,
        total_active_files: loaded.total_active_files,
    })
}

async fn execute_prepared_query(prepared: PreparedReplayQuery) -> BenchResult<ExecutedReplayQuery> {
    let batches = collect(prepared.plan.clone(), prepared.task_ctx).await?;

    Ok(ExecutedReplayQuery {
        plan: prepared.plan,
        batches,
        total_active_files: prepared.total_active_files,
    })
}

async fn validate_executed_query(
    executed: ExecutedReplayQuery,
) -> BenchResult<(SampleMetrics, f64)> {
    let validate_start = std::time::Instant::now();
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
