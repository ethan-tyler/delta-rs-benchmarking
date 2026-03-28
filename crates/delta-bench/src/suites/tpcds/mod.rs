pub mod catalog;
pub mod registration;
pub mod sql_loader;

use std::path::Path;
use std::sync::Arc;

use crate::cli::TimingPhase;
use crate::error::BenchResult;
use crate::fingerprint::{hash_arrow_schema, hash_record_batches_unordered};
use crate::results::{
    CaseFailure, CaseResult, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics,
    FAILURE_KIND_EXECUTION_ERROR, FAILURE_KIND_UNSUPPORTED,
};
use crate::runner::{
    run_case_async_with_timing_phase, CaseExecutionResult, PhaseTiming, TimedSample,
};
use crate::storage::StorageConfig;
use crate::suites::scan_metrics::extract_scan_metrics;
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::datafusion::execution::context::TaskContext;
use deltalake_core::datafusion::physical_plan::collect;
use deltalake_core::datafusion::physical_plan::ExecutionPlan;
use deltalake_core::datafusion::prelude::SessionContext;

struct LoadedTpcdsQuery {
    ctx: SessionContext,
}

struct PreparedTpcdsQuery {
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
}

struct ExecutedTpcdsQuery {
    plan: Arc<dyn ExecutionPlan>,
    batches: Vec<RecordBatch>,
    execution_elapsed_ms: f64,
}

pub fn case_names() -> Vec<String> {
    catalog::phase1_query_catalog()
        .into_iter()
        .map(|spec| format!("tpcds_{}", spec.id))
        .collect()
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    timing_phase: TimingPhase,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    let specs = catalog::phase1_query_catalog();
    run_with_specs_and_sql_dir(
        fixtures_dir,
        scale,
        timing_phase,
        warmup,
        iterations,
        storage,
        &specs,
        &sql_loader::default_sql_dir(),
    )
    .await
}

pub(crate) async fn run_with_specs_and_sql_dir(
    fixtures_dir: &Path,
    scale: &str,
    timing_phase: TimingPhase,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
    specs: &[catalog::TpcdsQuerySpec],
    sql_dir: &Path,
) -> BenchResult<Vec<CaseResult>> {
    let specs = specs.to_vec();

    let mut out = Vec::new();
    for spec in specs {
        let case_name = format!("tpcds_{}", spec.id);
        if !spec.enabled {
            out.push(skipped_case_result(case_name, spec.skip_reason));
            continue;
        }

        let sql = match load_case_sql(&spec, sql_dir) {
            Ok(sql) => sql,
            Err(err) => {
                out.push(CaseResult {
                    case: case_name,
                    success: false,
                    validation_passed: false,
                    perf_valid: false,
                    classification: "supported".to_string(),
                    samples: Vec::new(),
                    elapsed_stats: None,
                    run_summary: None,
                    run_summaries: None,
                    suite_manifest_hash: None,
                    case_definition_hash: None,
                    compatibility_key: None,
                    supports_decision: None,
                    required_runs: None,
                    decision_threshold_pct: None,
                    decision_metric: None,
                    failure_kind: Some(FAILURE_KIND_EXECUTION_ERROR.to_string()),
                    failure: Some(CaseFailure {
                        message: format!(
                            "failed to load SQL for enabled query {}: {}",
                            spec.id, err
                        ),
                    }),
                });
                continue;
            }
        };

        let fixture_root = fixtures_dir.to_path_buf();
        let scale = scale.to_string();
        let storage = storage.clone();
        let result =
            run_case_async_with_timing_phase(&case_name, warmup, iterations, timing_phase, || {
                let sql = sql.clone();
                let fixture_root = fixture_root.clone();
                let scale = scale.clone();
                let storage = storage.clone();
                async move {
                    let load_start = std::time::Instant::now();
                    let loaded = load_query_context(&fixture_root, &scale, &storage, &sql)
                        .await
                        .map_err(|err| err.to_string())?;
                    let load_elapsed_ms = load_start.elapsed().as_secs_f64() * 1000.0;

                    let planning_start = std::time::Instant::now();
                    let prepared = plan_loaded_query(loaded, &sql)
                        .await
                        .map_err(|err| err.to_string())?;
                    let planning_elapsed_ms = planning_start.elapsed().as_secs_f64() * 1000.0;

                    let executed = execute_prepared_query(prepared)
                        .await
                        .map_err(|err| err.to_string())?;
                    let execution_elapsed_ms = executed.execution_elapsed_ms;
                    let (metrics, validate_elapsed_ms) = validate_executed_query(executed)
                        .await
                        .map_err(|err| err.to_string())?;
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
            .await;
        out.push(into_case_result(result));
    }

    Ok(out)
}

fn load_case_sql(spec: &catalog::TpcdsQuerySpec, sql_dir: &Path) -> BenchResult<String> {
    let loaded = sql_loader::load_enabled_queries_from_dir(std::slice::from_ref(spec), sql_dir)?;
    let Some(query) = loaded.into_iter().next() else {
        return Err(crate::error::BenchError::InvalidArgument(format!(
            "missing SQL for enabled query {} (expected file {})",
            spec.id, spec.sql_file
        )));
    };
    Ok(query.sql)
}

async fn load_query_context(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
    sql: &str,
) -> BenchResult<LoadedTpcdsQuery> {
    let ctx = SessionContext::new();
    registration::register_tables_for_sql(&ctx, fixtures_dir, scale, storage, sql).await?;

    Ok(LoadedTpcdsQuery { ctx })
}

async fn plan_loaded_query(loaded: LoadedTpcdsQuery, sql: &str) -> BenchResult<PreparedTpcdsQuery> {
    let df = loaded.ctx.sql(sql).await?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await?;

    Ok(PreparedTpcdsQuery { plan, task_ctx })
}

async fn execute_prepared_query(prepared: PreparedTpcdsQuery) -> BenchResult<ExecutedTpcdsQuery> {
    let timed_start = std::time::Instant::now();
    let batches = collect(prepared.plan.clone(), prepared.task_ctx).await?;
    let elapsed_ms = timed_start.elapsed().as_secs_f64() * 1000.0;

    Ok(ExecutedTpcdsQuery {
        plan: prepared.plan,
        batches,
        execution_elapsed_ms: elapsed_ms,
    })
}

async fn validate_executed_query(
    executed: ExecutedTpcdsQuery,
) -> BenchResult<(SampleMetrics, f64)> {
    let validate_start = std::time::Instant::now();
    let rows_processed = executed
        .batches
        .iter()
        .map(|batch| batch.num_rows() as u64)
        .sum();
    let scan = extract_scan_metrics(&executed.plan);
    let result_hash = hash_record_batches_unordered(&executed.batches)?;
    let schema_hash = hash_arrow_schema(executed.plan.schema().as_ref())?;
    let validate_elapsed_ms = validate_start.elapsed().as_secs_f64() * 1000.0;

    Ok((
        SampleMetrics::base(Some(rows_processed), None, None, None)
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: scan.files_scanned,
                files_pruned: scan.files_pruned,
                bytes_scanned: scan.bytes_scanned,
                scan_time_ms: scan.scan_time_ms,
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

fn skipped_case_result(case: String, skip_reason: Option<&str>) -> CaseResult {
    CaseResult {
        case,
        success: false,
        validation_passed: false,
        perf_valid: false,
        classification: "supported".to_string(),
        samples: Vec::new(),
        elapsed_stats: None,
        run_summary: None,
        run_summaries: None,
        suite_manifest_hash: None,
        case_definition_hash: None,
        compatibility_key: None,
        supports_decision: None,
        required_runs: None,
        decision_threshold_pct: None,
        decision_metric: None,
        failure_kind: Some(FAILURE_KIND_UNSUPPORTED.to_string()),
        failure: Some(CaseFailure {
            message: format!(
                "skipped: {}",
                skip_reason.unwrap_or("query disabled in current TPC-DS phase")
            ),
        }),
    }
}

fn into_case_result(result: CaseExecutionResult) -> CaseResult {
    match result {
        CaseExecutionResult::Success(case) | CaseExecutionResult::Failure(case) => case,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        catalog::TpcdsQuerySpec, execute_prepared_query, load_query_context, plan_loaded_query,
        run_with_specs_and_sql_dir, validate_executed_query,
    };
    use crate::cli::TimingPhase;
    use crate::data::fixtures::generate_fixtures;
    use crate::storage::StorageConfig;
    use crate::suites::scan_metrics::sum_pruned_metrics;
    use deltalake_core::datafusion::physical_plan::metrics::{
        ExecutionPlanMetricsSet, MetricBuilder,
    };

    #[test]
    fn sum_pruned_metrics_includes_pruning_metrics_values() {
        let metrics = ExecutionPlanMetricsSet::new();
        let pruning =
            MetricBuilder::new(&metrics).pruning_metrics("files_ranges_pruned_statistics", 0);
        pruning.add_pruned(9);
        assert_eq!(
            sum_pruned_metrics(&metrics.clone_inner(), &["files_ranges_pruned_statistics"]),
            Some(9)
        );
    }

    #[tokio::test]
    async fn prepared_query_setup_and_execute_path_produces_metrics() {
        let temp = tempfile::tempdir().expect("fixtures tempdir");
        let storage = StorageConfig::local();
        generate_fixtures(temp.path(), "sf1", 42, true, &storage)
            .await
            .expect("generate fixtures");

        let loaded = load_query_context(
            temp.path(),
            "sf1",
            &storage,
            "SELECT COUNT(*) FROM store_sales",
        )
        .await
        .expect("load query context");
        let prepared = plan_loaded_query(loaded, "SELECT COUNT(*) FROM store_sales")
            .await
            .expect("plan query");
        let executed = execute_prepared_query(prepared)
            .await
            .expect("execute query");
        let elapsed_ms = executed.execution_elapsed_ms;
        let (metrics, _) = validate_executed_query(executed)
            .await
            .expect("validate query");

        assert!(elapsed_ms > 0.0);
        assert!(metrics.rows_processed.is_some());
        assert!(metrics.rows_processed.unwrap_or(0) > 0);
    }

    #[tokio::test]
    async fn missing_sql_is_reported_as_case_failure_not_suite_error() {
        let specs = vec![TpcdsQuerySpec {
            id: "q99",
            sql_file: "q99.sql",
            enabled: true,
            skip_reason: None,
        }];
        let temp_fixtures = tempfile::tempdir().expect("fixtures tempdir");
        let temp_sql = tempfile::tempdir().expect("sql tempdir");
        let storage = StorageConfig::local();

        let result = run_with_specs_and_sql_dir(
            temp_fixtures.path(),
            "sf1",
            TimingPhase::Execute,
            0,
            1,
            &storage,
            &specs,
            temp_sql.path(),
        )
        .await
        .expect("suite should return case-level failures instead of hard failing");

        assert_eq!(result.len(), 1);
        let case = &result[0];
        assert_eq!(case.case, "tpcds_q99");
        assert!(!case.success);
        let msg = case
            .failure
            .as_ref()
            .expect("failure payload for missing SQL")
            .message
            .to_ascii_lowercase();
        assert!(
            msg.contains("failed to load sql"),
            "expected missing SQL failure, got: {msg}"
        );
    }
}
