use std::path::Path;
use std::sync::Arc;

use deltalake_core::datafusion::physical_plan::collect;
use deltalake_core::datafusion::prelude::SessionContext;

use crate::data::fixtures::narrow_sales_table_url;
use crate::error::BenchResult;
use crate::results::{CaseResult, SampleMetrics};
use crate::runner::{run_case_async, CaseExecutionResult};
use crate::storage::StorageConfig;
use crate::suites::scan_metrics::extract_scan_metrics;
use url::Url;

pub fn case_names() -> Vec<String> {
    vec![
        "read_full_scan_narrow".to_string(),
        "read_projection_region".to_string(),
        "read_filter_flag_true".to_string(),
    ]
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    let table_url = narrow_sales_table_url(fixtures_dir, scale, storage)?;

    let mut results = Vec::new();

    let full_scan = run_case_async("read_full_scan_narrow", warmup, iterations, || {
        let table_url = table_url.clone();
        let storage = storage.clone();
        async move {
            run_sql_query(&storage, table_url, "SELECT COUNT(*) FROM bench")
                .await
                .map_err(|e| e.to_string())
        }
    })
    .await;
    results.push(into_case_result(full_scan));

    let projection = run_case_async("read_projection_region", warmup, iterations, || {
        let table_url = table_url.clone();
        let storage = storage.clone();
        async move {
            run_sql_query(
                &storage,
                table_url,
                "SELECT region, SUM(value_i64) FROM bench GROUP BY region",
            )
            .await
            .map_err(|e| e.to_string())
        }
    })
    .await;
    results.push(into_case_result(projection));

    let filtered = run_case_async("read_filter_flag_true", warmup, iterations, || {
        let table_url = table_url.clone();
        let storage = storage.clone();
        async move {
            run_sql_query(
                &storage,
                table_url,
                "SELECT COUNT(*) FROM bench WHERE flag = true AND value_i64 > 0",
            )
            .await
            .map_err(|e| e.to_string())
        }
    })
    .await;
    results.push(into_case_result(filtered));

    Ok(results)
}

async fn run_sql_query(
    storage: &StorageConfig,
    table_url: Url,
    sql: &str,
) -> BenchResult<SampleMetrics> {
    let table = storage.open_table(table_url).await?;
    let ctx = SessionContext::new();
    ctx.register_table("bench", table.table_provider().await?)?;
    let df = ctx.sql(sql).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;
    let batches = collect(plan.clone(), task_ctx).await?;
    let rows_processed = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
    let scan_metrics = extract_scan_metrics(&plan);

    Ok(SampleMetrics {
        rows_processed: Some(rows_processed),
        bytes_processed: None,
        operations: None,
        table_version: None,
        files_scanned: scan_metrics.files_scanned,
        files_pruned: scan_metrics.files_pruned,
        bytes_scanned: scan_metrics.bytes_scanned,
        scan_time_ms: scan_metrics.scan_time_ms,
        rewrite_time_ms: None,
    })
}

fn into_case_result(result: CaseExecutionResult) -> CaseResult {
    match result {
        CaseExecutionResult::Success(c) | CaseExecutionResult::Failure(c) => c,
    }
}
