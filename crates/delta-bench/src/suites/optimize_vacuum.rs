use std::fs;
use std::path::Path;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use url::Url;

use crate::data::fixtures::{
    load_rows, optimize_compacted_table_path, optimize_small_files_table_path,
    vacuum_ready_table_path, write_delta_table, write_delta_table_small_files,
    write_vacuum_ready_table,
};
use crate::error::{BenchError, BenchResult};
use crate::results::{CaseFailure, CaseResult, SampleMetrics};
use crate::runner::{
    run_case_async_with_async_setup, run_case_async_with_setup, CaseExecutionResult,
};
use crate::storage::StorageConfig;

const OPTIMIZE_COMPACT_TARGET_SIZE: u64 = 1_000_000;
const OPTIMIZE_HEAVY_TARGET_SIZE: u64 = 64_000;

struct IterationSetup {
    _temp: tempfile::TempDir,
    table_url: Url,
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
                "missing optimize/vacuum fixture tables; run bench data first",
            ));
        }

        let mut out = Vec::new();

        let optimize = run_case_async_with_setup(
            "optimize_compact_small_files",
            warmup,
            iterations,
            || prepare_iteration(&optimize_source).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    run_optimize_case(table_url, OPTIMIZE_COMPACT_TARGET_SIZE, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
        .await;
        out.push(into_case_result(optimize));

        let noop = run_case_async_with_setup(
            "optimize_noop_already_compact",
            warmup,
            iterations,
            || prepare_iteration(&optimize_compacted_source).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    run_optimize_case(table_url, OPTIMIZE_COMPACT_TARGET_SIZE, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
        .await;
        out.push(into_case_result(noop));

        let heavy = run_case_async_with_setup(
            "optimize_heavy_compaction",
            warmup,
            iterations,
            || prepare_iteration(&optimize_source).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    run_optimize_case(table_url, OPTIMIZE_HEAVY_TARGET_SIZE, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
        .await;
        out.push(into_case_result(heavy));

        let dry_run = run_case_async_with_setup(
            "vacuum_dry_run_lite",
            warmup,
            iterations,
            || prepare_iteration(&vacuum_source).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    run_vacuum_case(table_url, true, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
        .await;
        out.push(into_case_result(dry_run));

        let execute = run_case_async_with_setup(
            "vacuum_execute_lite",
            warmup,
            iterations,
            || prepare_iteration(&vacuum_source).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    run_vacuum_case(table_url, false, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
        .await;
        out.push(into_case_result(execute));

        return Ok(out);
    }

    let rows = match load_rows(fixtures_dir, scale) {
        Ok(rows) => Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(&e.to_string())),
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
                Ok::<Url, String>(table_url)
            }
        },
        |table_url| {
            let storage = storage.clone();
            async move {
                run_optimize_case(table_url, OPTIMIZE_COMPACT_TARGET_SIZE, &storage)
                    .await
                    .map_err(|e| e.to_string())
            }
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
                Ok::<Url, String>(table_url)
            }
        },
        |table_url| {
            let storage = storage.clone();
            async move {
                run_optimize_case(table_url, OPTIMIZE_COMPACT_TARGET_SIZE, &storage)
                    .await
                    .map_err(|e| e.to_string())
            }
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
                Ok::<Url, String>(table_url)
            }
        },
        |table_url| {
            let storage = storage.clone();
            async move {
                run_optimize_case(table_url, OPTIMIZE_HEAVY_TARGET_SIZE, &storage)
                    .await
                    .map_err(|e| e.to_string())
            }
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
                Ok::<Url, String>(table_url)
            }
        },
        |table_url| {
            let storage = storage.clone();
            async move {
                run_vacuum_case(table_url, true, &storage)
                    .await
                    .map_err(|e| e.to_string())
            }
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
                Ok::<Url, String>(table_url)
            }
        },
        |table_url| {
            let storage = storage.clone();
            async move {
                run_vacuum_case(table_url, false, &storage)
                    .await
                    .map_err(|e| e.to_string())
            }
        },
    )
    .await;
    out.push(into_case_result(execute));

    Ok(out)
}

async fn run_optimize_case(
    table_url: Url,
    target_size: u64,
    storage: &StorageConfig,
) -> BenchResult<SampleMetrics> {
    let table = storage.open_table(table_url).await?;
    let (table, metrics) = table.optimize().with_target_size(target_size).await?;
    Ok(SampleMetrics::base(
        Some(metrics.total_considered_files as u64),
        None,
        Some(metrics.num_files_added + metrics.num_files_removed),
        table.version().map(|v| v as u64),
    )
    .with_scan_rewrite_metrics(
        Some(metrics.total_considered_files as u64),
        Some(metrics.total_files_skipped as u64),
        None,
        None,
        None,
    ))
}

async fn run_vacuum_case(
    table_url: Url,
    dry_run: bool,
    storage: &StorageConfig,
) -> BenchResult<SampleMetrics> {
    let table = storage.open_table(table_url).await?;
    let (table, metrics) = table
        .vacuum()
        .with_dry_run(dry_run)
        .with_retention_period(ChronoDuration::seconds(0))
        .with_enforce_retention_duration(false)
        .await?;
    Ok(SampleMetrics::base(
        Some(metrics.files_deleted.len() as u64),
        None,
        Some(1),
        table.version().map(|v| v as u64),
    ))
}

fn prepare_iteration(source_table_path: &Path) -> BenchResult<IterationSetup> {
    let temp = tempfile::tempdir()?;
    let table_dir = temp.path().join("table");
    copy_dir_all(source_table_path, &table_dir)?;
    let table_url = Url::from_directory_path(&table_dir).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create table URL for {}",
            table_dir.display()
        ))
    })?;
    Ok(IterationSetup {
        _temp: temp,
        table_url,
    })
}

fn copy_dir_all(src: &Path, dst: &Path) -> BenchResult<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(BenchError::InvalidArgument(format!(
                "symlinks are not allowed in fixture tree: {}",
                entry.path().display()
            )));
        }
        let to = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

fn into_case_result(result: CaseExecutionResult) -> CaseResult {
    match result {
        CaseExecutionResult::Success(c) | CaseExecutionResult::Failure(c) => c,
    }
}

fn fixture_error_cases(message: &str) -> Vec<CaseResult> {
    case_names()
        .into_iter()
        .map(|case| CaseResult {
            case,
            success: false,
            samples: Vec::new(),
            failure: Some(CaseFailure {
                message: format!("fixture load failed: {message}"),
            }),
        })
        .collect()
}
