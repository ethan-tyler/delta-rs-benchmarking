use std::path::Path;
use std::sync::Arc;

use serde_json::json;
use url::Url;

use deltalake_core::DeltaTable;

use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::fixtures::{
    delete_update_small_files_table_path, load_rows, read_partitioned_table_path,
    write_delta_table_partitioned_small_files,
};
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::hash_json;
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics};
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;
use crate::validation::{lane_requires_semantic_validation, validate_table_state};

#[derive(Clone, Copy)]
pub(crate) enum DmlOperation {
    Delete,
    UpdateLiteral,
    UpdateExpression,
    UpdateAllExpression,
}

#[derive(Clone, Copy)]
pub(crate) struct DeleteUpdateCase {
    pub(crate) name: &'static str,
    pub(crate) operation: DmlOperation,
    pub(crate) rows_matched_fraction: Option<f64>,
    pub(crate) partition_localized: bool,
    pub(crate) small_files_seed: bool,
}

struct IterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
}

const DELETE_UPDATE_CASES: [DeleteUpdateCase; 7] = [
    DeleteUpdateCase {
        name: "delete_1pct_localized",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.01),
        partition_localized: true,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "delete_5pct_scattered",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.05),
        partition_localized: false,
        small_files_seed: true,
    },
    DeleteUpdateCase {
        name: "delete_50pct_broad",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.50),
        partition_localized: false,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "update_literal_1pct_localized",
        operation: DmlOperation::UpdateLiteral,
        rows_matched_fraction: Some(0.01),
        partition_localized: true,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "update_literal_5pct_scattered",
        operation: DmlOperation::UpdateLiteral,
        rows_matched_fraction: Some(0.05),
        partition_localized: false,
        small_files_seed: true,
    },
    DeleteUpdateCase {
        name: "update_expr_50pct_broad",
        operation: DmlOperation::UpdateExpression,
        rows_matched_fraction: Some(0.50),
        partition_localized: false,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "update_all_rows_expr",
        operation: DmlOperation::UpdateAllExpression,
        rows_matched_fraction: None,
        partition_localized: false,
        small_files_seed: false,
    },
];

pub fn case_names() -> Vec<String> {
    DELETE_UPDATE_CASES
        .iter()
        .map(|case| case.name.to_string())
        .collect()
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    lane: BenchmarkLane,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    if storage.is_local() {
        let standard_source = read_partitioned_table_path(fixtures_dir, scale);
        let small_files_source = delete_update_small_files_table_path(fixtures_dir, scale);
        if !standard_source.exists() || !small_files_source.exists() {
            return Ok(fixture_error_cases(
                case_names(),
                "missing delete/update fixture tables; run bench data first",
            ));
        }

        let mut out = Vec::new();
        for case in DELETE_UPDATE_CASES {
            let source = if case.small_files_seed {
                small_files_source.clone()
            } else {
                standard_source.clone()
            };

            let c = run_case_async_with_async_setup(
                case.name,
                warmup,
                iterations,
                || {
                    let source = source.clone();
                    let storage = storage.clone();
                    async move {
                        prepare_iteration(&source, &storage)
                            .await
                            .map_err(|e| e.to_string())
                    }
                },
                |setup| async move {
                    let _keep_temp = setup._temp;
                    run_delete_update_case(setup.table, case, lane)
                        .await
                        .map_err(|e| e.to_string())
                },
            )
            .await;
            out.push(into_case_result(c));
        }

        return Ok(out);
    }

    let rows = match load_rows(fixtures_dir, scale) {
        Ok(rows) => Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(case_names(), &e.to_string())),
    };

    let mut out = Vec::new();
    for case in DELETE_UPDATE_CASES {
        let c = run_case_async_with_async_setup(
            case.name,
            warmup,
            iterations,
            || {
                let storage = storage.clone();
                let seed_rows = Arc::clone(&rows);
                async move {
                    let (base_table_name, chunk_size) = if case.small_files_seed {
                        ("delete_update_small_files_delta", 64)
                    } else {
                        ("read_partitioned_delta", 128)
                    };
                    let table_url = storage
                        .isolated_table_url(scale, base_table_name, case.name)
                        .map_err(|e| e.to_string())?;
                    write_delta_table_partitioned_small_files(
                        table_url.clone(),
                        seed_rows.as_slice(),
                        chunk_size,
                        &["region"],
                        &storage,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                    let table = storage
                        .open_table(table_url)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<DeltaTable, String>(table)
                }
            },
            |table| async move {
                run_delete_update_case(table, case, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(c));
    }

    Ok(out)
}

pub(crate) async fn run_delete_update_case(
    table: DeltaTable,
    case: DeleteUpdateCase,
    lane: BenchmarkLane,
) -> BenchResult<SampleMetrics> {
    match case.operation {
        DmlOperation::Delete => {
            let predicate = case_predicate(case).ok_or_else(|| {
                BenchError::InvalidArgument(format!("missing predicate for {}", case.name))
            })?;
            let (table, metrics) = table.delete().with_predicate(predicate.as_str()).await?;
            let table_version = table.version().map(|v| v as u64);
            let result_hash = hash_json(&json!({
                "operation": "delete",
                "rows_affected": metrics.num_deleted_rows as u64,
                "files_added": metrics.num_added_files as u64,
                "files_removed": metrics.num_removed_files as u64,
                "table_version": table_version,
            }))?;
            let mut schema_hash = hash_json(&json!([
                "operation:string",
                "rows_affected:u64",
                "files_added:u64",
                "files_removed:u64",
                "table_version:u64",
            ]))?;
            let mut semantic_state_digest = None;
            let mut validation_summary = None;
            if lane_requires_semantic_validation(lane) {
                let validation = validate_table_state(&table).await?;
                schema_hash = validation.schema_hash;
                semantic_state_digest = Some(validation.digest);
                validation_summary = Some(validation.summary);
            }
            let sample = SampleMetrics::base(
                Some(metrics.num_deleted_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table_version,
            )
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: None,
                files_pruned: None,
                bytes_scanned: None,
                scan_time_ms: Some(metrics.scan_time_ms),
                rewrite_time_ms: Some(metrics.rewrite_time_ms),
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
                semantic_state_digest,
                validation_summary,
            });
            Ok(sample)
        }
        DmlOperation::UpdateLiteral => {
            let predicate = case_predicate(case).ok_or_else(|| {
                BenchError::InvalidArgument(format!("missing predicate for {}", case.name))
            })?;
            let (table, metrics) = table
                .update()
                .with_predicate(predicate.as_str())
                .with_update("value_i64", "7")
                .await?;
            let table_version = table.version().map(|v| v as u64);
            let result_hash = hash_json(&json!({
                "operation": "update_literal",
                "rows_affected": metrics.num_updated_rows as u64,
                "files_added": metrics.num_added_files as u64,
                "files_removed": metrics.num_removed_files as u64,
                "table_version": table_version,
            }))?;
            let mut schema_hash = hash_json(&json!([
                "operation:string",
                "rows_affected:u64",
                "files_added:u64",
                "files_removed:u64",
                "table_version:u64",
            ]))?;
            let mut semantic_state_digest = None;
            let mut validation_summary = None;
            if lane_requires_semantic_validation(lane) {
                let validation = validate_table_state(&table).await?;
                schema_hash = validation.schema_hash;
                semantic_state_digest = Some(validation.digest);
                validation_summary = Some(validation.summary);
            }
            let sample = SampleMetrics::base(
                Some(metrics.num_updated_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table_version,
            )
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: None,
                files_pruned: None,
                bytes_scanned: None,
                scan_time_ms: Some(metrics.scan_time_ms),
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
                semantic_state_digest,
                validation_summary,
            });
            Ok(sample)
        }
        DmlOperation::UpdateExpression => {
            let predicate = case_predicate(case).ok_or_else(|| {
                BenchError::InvalidArgument(format!("missing predicate for {}", case.name))
            })?;
            let (table, metrics) = table
                .update()
                .with_predicate(predicate.as_str())
                .with_update("value_i64", "value_i64 + 1")
                .await?;
            let table_version = table.version().map(|v| v as u64);
            let result_hash = hash_json(&json!({
                "operation": "update_expression",
                "rows_affected": metrics.num_updated_rows as u64,
                "files_added": metrics.num_added_files as u64,
                "files_removed": metrics.num_removed_files as u64,
                "table_version": table_version,
            }))?;
            let mut schema_hash = hash_json(&json!([
                "operation:string",
                "rows_affected:u64",
                "files_added:u64",
                "files_removed:u64",
                "table_version:u64",
            ]))?;
            let mut semantic_state_digest = None;
            let mut validation_summary = None;
            if lane_requires_semantic_validation(lane) {
                let validation = validate_table_state(&table).await?;
                schema_hash = validation.schema_hash;
                semantic_state_digest = Some(validation.digest);
                validation_summary = Some(validation.summary);
            }
            let sample = SampleMetrics::base(
                Some(metrics.num_updated_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table_version,
            )
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: None,
                files_pruned: None,
                bytes_scanned: None,
                scan_time_ms: Some(metrics.scan_time_ms),
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
                semantic_state_digest,
                validation_summary,
            });
            Ok(sample)
        }
        DmlOperation::UpdateAllExpression => {
            let (table, metrics) = table
                .update()
                .with_update("value_i64", "value_i64 + 10")
                .await?;
            let table_version = table.version().map(|v| v as u64);
            let result_hash = hash_json(&json!({
                "operation": "update_all_expression",
                "rows_affected": metrics.num_updated_rows as u64,
                "files_added": metrics.num_added_files as u64,
                "files_removed": metrics.num_removed_files as u64,
                "table_version": table_version,
            }))?;
            let mut schema_hash = hash_json(&json!([
                "operation:string",
                "rows_affected:u64",
                "files_added:u64",
                "files_removed:u64",
                "table_version:u64",
            ]))?;
            let mut semantic_state_digest = None;
            let mut validation_summary = None;
            if lane_requires_semantic_validation(lane) {
                let validation = validate_table_state(&table).await?;
                schema_hash = validation.schema_hash;
                semantic_state_digest = Some(validation.digest);
                validation_summary = Some(validation.summary);
            }
            let sample = SampleMetrics::base(
                Some(metrics.num_updated_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table_version,
            )
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: None,
                files_pruned: None,
                bytes_scanned: None,
                scan_time_ms: Some(metrics.scan_time_ms),
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
                semantic_state_digest,
                validation_summary,
            });
            Ok(sample)
        }
    }
}

fn case_predicate(case: DeleteUpdateCase) -> Option<String> {
    let fraction = case.rows_matched_fraction?;
    let scatter_divisor = ((1.0 / fraction).round() as u64).max(1);
    if case.partition_localized {
        let localized_divisor = ((1.0 / (fraction * 6.0)).round() as u64).max(1);
        Some(format!("region = 'us' AND id % {localized_divisor} = 0"))
    } else {
        Some(format!("id % {scatter_divisor} = 0"))
    }
}

async fn prepare_iteration(
    source_table_path: &Path,
    storage: &StorageConfig,
) -> BenchResult<IterationSetup> {
    let temp = tempfile::tempdir()?;
    let table_dir = temp.path().join("table");
    copy_dir_all(source_table_path, &table_dir)?;
    let table_url = Url::from_directory_path(&table_dir).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create table URL for {}",
            table_dir.display()
        ))
    })?;
    let table = storage.open_table(table_url).await?;

    Ok(IterationSetup { _temp: temp, table })
}
