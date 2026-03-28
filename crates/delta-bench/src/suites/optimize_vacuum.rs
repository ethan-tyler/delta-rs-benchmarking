use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use serde_json::json;
use url::Url;

use deltalake_core::DeltaTable;

use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::fixtures::{
    load_rows, optimize_compacted_table_path, optimize_small_files_table_path,
    vacuum_ready_table_path, write_delta_table, write_delta_table_small_files,
    write_vacuum_ready_table,
};
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::hash_json;
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics};
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;
use crate::validation::{lane_requires_semantic_validation, validate_table_state};

const OPTIMIZE_COMPACT_TARGET_SIZE: u64 = 1_000_000;
const OPTIMIZE_HEAVY_TARGET_SIZE: u64 = 64_000;

struct IterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
}

pub fn case_names() -> Vec<String> {
    vec![
        "optimize_compact_small_files".to_string(),
        "optimize_noop_already_compact".to_string(),
        "optimize_heavy_compaction".to_string(),
        "vacuum_dry_run_lite".to_string(),
        "vacuum_execute_lite".to_string(),
    ]
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
        let optimize_source = optimize_small_files_table_path(fixtures_dir, scale);
        let optimize_compacted_source = optimize_compacted_table_path(fixtures_dir, scale);
        let vacuum_source = vacuum_ready_table_path(fixtures_dir, scale);

        if !optimize_source.exists()
            || !optimize_compacted_source.exists()
            || !vacuum_source.exists()
        {
            return Ok(fixture_error_cases(
                case_names(),
                "missing optimize/vacuum fixture tables; run bench data first",
            ));
        }

        let mut out = Vec::new();

        let optimize = run_case_async_with_async_setup(
            "optimize_compact_small_files",
            warmup,
            iterations,
            || {
                let source = optimize_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_optimize_case(setup.table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(optimize));

        let noop = run_case_async_with_async_setup(
            "optimize_noop_already_compact",
            warmup,
            iterations,
            || {
                let source = optimize_compacted_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_optimize_case(setup.table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(noop));

        let heavy = run_case_async_with_async_setup(
            "optimize_heavy_compaction",
            warmup,
            iterations,
            || {
                let source = optimize_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_optimize_case(setup.table, OPTIMIZE_HEAVY_TARGET_SIZE, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(heavy));

        let dry_run = run_case_async_with_async_setup(
            "vacuum_dry_run_lite",
            warmup,
            iterations,
            || {
                let source = vacuum_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_vacuum_case(setup.table, true, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(dry_run));

        let execute = run_case_async_with_async_setup(
            "vacuum_execute_lite",
            warmup,
            iterations,
            || {
                let source = vacuum_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_vacuum_case(setup.table, false, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(execute));

        return Ok(out);
    }

    let rows = match load_rows(fixtures_dir, scale) {
        Ok(rows) => Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(case_names(), &e.to_string())),
    };
    let optimize_seed_rows = Arc::new(
        rows.iter()
            .take((rows.len() / 2).max(2048))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let vacuum_seed_rows = Arc::new(
        rows.iter()
            .take((rows.len() / 3).max(1024))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let mut out = Vec::new();

    let optimize = run_case_async_with_async_setup(
        "optimize_compact_small_files",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = Arc::clone(&optimize_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(
                        scale,
                        "optimize_small_files_delta",
                        "optimize_compact_small_files",
                    )
                    .map_err(|e| e.to_string())?;
                write_delta_table_small_files(table_url.clone(), rows.as_slice(), 128, &storage)
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
            run_optimize_case(table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(optimize));

    let noop = run_case_async_with_async_setup(
        "optimize_noop_already_compact",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = Arc::clone(&optimize_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(
                        scale,
                        "optimize_compacted_delta",
                        "optimize_noop_already_compact",
                    )
                    .map_err(|e| e.to_string())?;
                write_delta_table(table_url.clone(), rows.as_slice(), &storage)
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
            run_optimize_case(table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(noop));

    let heavy = run_case_async_with_async_setup(
        "optimize_heavy_compaction",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = Arc::clone(&optimize_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(
                        scale,
                        "optimize_small_files_delta",
                        "optimize_heavy_compaction",
                    )
                    .map_err(|e| e.to_string())?;
                write_delta_table_small_files(table_url.clone(), rows.as_slice(), 128, &storage)
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
            run_optimize_case(table, OPTIMIZE_HEAVY_TARGET_SIZE, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(heavy));

    let dry_run = run_case_async_with_async_setup(
        "vacuum_dry_run_lite",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = Arc::clone(&vacuum_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(scale, "vacuum_ready_delta", "vacuum_dry_run_lite")
                    .map_err(|e| e.to_string())?;
                write_vacuum_ready_table(table_url.clone(), rows.as_slice(), &storage)
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
            run_vacuum_case(table, true, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(dry_run));

    let execute = run_case_async_with_async_setup(
        "vacuum_execute_lite",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = Arc::clone(&vacuum_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(scale, "vacuum_ready_delta", "vacuum_execute_lite")
                    .map_err(|e| e.to_string())?;
                write_vacuum_ready_table(table_url.clone(), rows.as_slice(), &storage)
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
            run_vacuum_case(table, false, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(execute));

    Ok(out)
}

async fn run_optimize_case(
    table: DeltaTable,
    target_size: u64,
    lane: BenchmarkLane,
) -> BenchResult<SampleMetrics> {
    let (table, metrics) = table
        .optimize()
        .with_target_size(normalize_target_size(target_size)?.into())
        .await?;
    let table_version = table.version().map(|v| v as u64);
    let result_hash = hash_json(&json!({
        "operation": "optimize",
        "target_size": target_size,
        "files_considered": metrics.total_considered_files as u64,
        "files_skipped": metrics.total_files_skipped as u64,
        "files_added": metrics.num_files_added,
        "files_removed": metrics.num_files_removed,
        "table_version": table_version,
    }))?;
    let mut schema_hash = hash_json(&json!([
        "operation:string",
        "target_size:u64",
        "files_considered:u64",
        "files_skipped:u64",
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
    Ok(SampleMetrics::base(
        Some(metrics.total_considered_files as u64),
        None,
        Some(metrics.num_files_added + metrics.num_files_removed),
        table_version,
    )
    .with_scan_rewrite(ScanRewriteMetrics {
        files_scanned: Some(metrics.total_considered_files as u64),
        files_pruned: Some(metrics.total_files_skipped as u64),
        bytes_scanned: None,
        scan_time_ms: None,
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
    }))
}

fn normalize_target_size(target_size: u64) -> BenchResult<NonZeroU64> {
    NonZeroU64::new(target_size).ok_or_else(|| {
        BenchError::InvalidArgument("target size must be greater than zero".to_string())
    })
}

async fn run_vacuum_case(
    table: DeltaTable,
    dry_run: bool,
    lane: BenchmarkLane,
) -> BenchResult<SampleMetrics> {
    let (table, metrics) = table
        .vacuum()
        .with_dry_run(dry_run)
        .with_retention_period(ChronoDuration::seconds(0))
        .with_enforce_retention_duration(false)
        .await?;
    let table_version = table.version().map(|v| v as u64);
    let result_hash = hash_json(&json!({
        "operation": "vacuum",
        "dry_run": dry_run,
        "files_deleted": metrics.files_deleted.len() as u64,
        "table_version": table_version,
    }))?;
    let mut schema_hash = hash_json(&json!([
        "operation:string",
        "dry_run:bool",
        "files_deleted:u64",
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
    Ok(SampleMetrics::base(
        Some(metrics.files_deleted.len() as u64),
        None,
        Some(1),
        table_version,
    )
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
    }))
}

#[cfg(test)]
mod tests {
    use super::normalize_target_size;
    use crate::error::BenchError;

    #[test]
    fn normalize_target_size_accepts_positive_values() {
        let target = normalize_target_size(64_000).expect("positive target size should work");
        assert_eq!(target.get(), 64_000);
    }

    #[test]
    fn normalize_target_size_rejects_zero() {
        let err = normalize_target_size(0).expect_err("zero target size should fail");
        assert!(matches!(
            err,
            BenchError::InvalidArgument(message) if message.contains("target size must be greater than zero")
        ));
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
