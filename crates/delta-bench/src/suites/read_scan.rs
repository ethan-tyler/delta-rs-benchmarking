use std::path::Path;
use std::sync::Arc;

use deltalake_core::datafusion::datasource::physical_plan::FileScanConfig;
use deltalake_core::datafusion::datasource::source::{DataSource, DataSourceExec};
use deltalake_core::datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use deltalake_core::datafusion::physical_plan::{collect, ExecutionPlan};
use deltalake_core::datafusion::prelude::SessionContext;

use crate::data::fixtures::narrow_sales_table_url;
use crate::error::BenchResult;
use crate::results::{CaseResult, SampleMetrics};
use crate::runner::{run_case_async, CaseExecutionResult};
use crate::storage::StorageConfig;
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

#[derive(Default)]
struct ReadScanMetrics {
    files_scanned: Option<u64>,
    files_pruned: Option<u64>,
    bytes_scanned: Option<u64>,
    scan_time_ms: Option<u64>,
}

fn extract_scan_metrics(plan: &Arc<dyn ExecutionPlan>) -> ReadScanMetrics {
    let mut files_scanned_total = 0_u64;
    let mut files_scanned_seen = false;
    let mut files_pruned_total = 0_u64;
    let mut files_pruned_seen = false;
    let mut bytes_scanned_total = 0_u64;
    let mut bytes_scanned_seen = false;
    let mut scan_elapsed_nanos_total = 0_u64;
    let mut scan_elapsed_seen = false;

    collect_scan_metrics(
        plan,
        &mut files_scanned_total,
        &mut files_scanned_seen,
        &mut files_pruned_total,
        &mut files_pruned_seen,
        &mut bytes_scanned_total,
        &mut bytes_scanned_seen,
        &mut scan_elapsed_nanos_total,
        &mut scan_elapsed_seen,
    );

    let files_scanned = files_scanned_seen.then_some(files_scanned_total);
    // Keep this populated for compatibility with scan paths that expose scanned count
    // but not explicit pruned count.
    let files_pruned = if files_pruned_seen {
        Some(files_pruned_total)
    } else {
        files_scanned.map(|_| 0)
    };

    let bytes_scanned = if bytes_scanned_seen {
        Some(bytes_scanned_total)
    } else {
        // Some scan paths do not currently expose bytes_scanned counters directly.
        files_scanned.map(|_| 0)
    };

    let scan_time_ms = if scan_elapsed_seen {
        Some(scan_elapsed_nanos_total / 1_000_000)
    } else {
        files_scanned.map(|_| 0)
    };

    ReadScanMetrics {
        files_scanned,
        files_pruned,
        bytes_scanned,
        scan_time_ms,
    }
}

#[allow(clippy::too_many_arguments)]
fn collect_scan_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    files_scanned_total: &mut u64,
    files_scanned_seen: &mut bool,
    files_pruned_total: &mut u64,
    files_pruned_seen: &mut bool,
    bytes_scanned_total: &mut u64,
    bytes_scanned_seen: &mut bool,
    scan_elapsed_nanos_total: &mut u64,
    scan_elapsed_seen: &mut bool,
) {
    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some(scan_config) = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
        {
            let file_scan_metrics = scan_config.metrics().clone_inner();
            if let Some(v) = sum_count_metrics(&file_scan_metrics, &["bytes_scanned"]) {
                *bytes_scanned_total = bytes_scanned_total.saturating_add(v);
                *bytes_scanned_seen = true;
            }
        }
    }

    if let Some(metrics) = plan.metrics() {
        if let Some(v) = sum_count_metrics(&metrics, &["files_scanned", "count_files_scanned"]) {
            *files_scanned_total = files_scanned_total.saturating_add(v);
            *files_scanned_seen = true;
        }

        if let Some(v) = sum_count_metrics(&metrics, &["files_pruned", "count_files_pruned"]) {
            *files_pruned_total = files_pruned_total.saturating_add(v);
            *files_pruned_seen = true;
        }

        if let Some(v) = sum_pruned_metrics(&metrics, &["files_ranges_pruned_statistics"]) {
            *files_pruned_total = files_pruned_total.saturating_add(v);
            *files_pruned_seen = true;
        }

        let is_scan_node = has_metric_name(&metrics, &["files_scanned", "count_files_scanned"])
            || has_metric_name(&metrics, &["bytes_scanned"])
            || has_metric_name(&metrics, &["files_ranges_pruned_statistics"]);

        if let Some(v) = sum_count_metrics(&metrics, &["bytes_scanned"]) {
            *bytes_scanned_total = bytes_scanned_total.saturating_add(v);
            *bytes_scanned_seen = true;
        }

        if is_scan_node {
            if let Some(elapsed_nanos) = metrics.elapsed_compute() {
                *scan_elapsed_nanos_total =
                    scan_elapsed_nanos_total.saturating_add(elapsed_nanos as u64);
                *scan_elapsed_seen = true;
            }
        }
    }

    for child in plan.children() {
        collect_scan_metrics(
            child,
            files_scanned_total,
            files_scanned_seen,
            files_pruned_total,
            files_pruned_seen,
            bytes_scanned_total,
            bytes_scanned_seen,
            scan_elapsed_nanos_total,
            scan_elapsed_seen,
        );
    }
}

fn has_metric_name(metrics: &MetricsSet, names: &[&str]) -> bool {
    metrics.iter().any(|metric| {
        let name = metric.value().name();
        names.contains(&name)
    })
}

fn sum_count_metrics(metrics: &MetricsSet, names: &[&str]) -> Option<u64> {
    let mut total = 0_u64;
    let mut seen = false;
    for metric in metrics.iter() {
        if let MetricValue::Count { name, count } = metric.value() {
            if names.iter().any(|candidate| *candidate == name.as_ref()) {
                total = total.saturating_add(count.value() as u64);
                seen = true;
            }
        }
    }
    seen.then_some(total)
}

fn sum_pruned_metrics(metrics: &MetricsSet, names: &[&str]) -> Option<u64> {
    let mut total = 0_u64;
    let mut seen = false;
    for metric in metrics.iter() {
        if let MetricValue::PruningMetrics {
            name,
            pruning_metrics,
        } = metric.value()
        {
            if names.iter().any(|candidate| *candidate == name.as_ref()) {
                total = total.saturating_add(pruning_metrics.pruned() as u64);
                seen = true;
            }
        }
    }
    seen.then_some(total)
}
