use std::fs;
use std::path::Path;

use url::Url;

use crate::data::fixtures::{narrow_sales_table_path, narrow_sales_table_url};
use crate::error::{BenchError, BenchResult};
use crate::results::{CaseResult, SampleMetrics};
use crate::runner::{run_case_async, run_case_async_with_setup, CaseExecutionResult};
use crate::storage::StorageConfig;

struct MetadataIterationSetup {
    _temp: tempfile::TempDir,
    table_url: Url,
}

pub fn case_names() -> Vec<String> {
    vec![
        "metadata_table_load".to_string(),
        "metadata_time_travel_v0".to_string(),
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
        let table_path = narrow_sales_table_path(fixtures_dir, scale);
        let mut out = Vec::new();

        let c1 = run_case_async_with_setup(
            "metadata_table_load",
            warmup,
            iterations,
            || prepare_metadata_iteration(&table_path).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    let table = storage
                        .open_table(table_url)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<SampleMetrics, String>(SampleMetrics {
                        rows_processed: None,
                        bytes_processed: None,
                        operations: Some(1),
                        table_version: table.version().map(|v| v as u64),
                        files_scanned: None,
                        files_pruned: None,
                        bytes_scanned: None,
                        scan_time_ms: None,
                        rewrite_time_ms: None,
                    })
                }
            },
        )
        .await;
        out.push(into_case_result(c1));

        let c2 = run_case_async_with_setup(
            "metadata_time_travel_v0",
            warmup,
            iterations,
            || prepare_metadata_iteration(&table_path).map_err(|e| e.to_string()),
            |setup| {
                let storage = storage.clone();
                async move {
                    let table_url = setup.table_url.clone();
                    let _keep_temp = setup;
                    let mut table = storage
                        .try_from_url_for_write(table_url)
                        .await
                        .map_err(|e| e.to_string())?;
                    table.load_version(0).await.map_err(|e| e.to_string())?;
                    Ok::<SampleMetrics, String>(SampleMetrics {
                        rows_processed: None,
                        bytes_processed: None,
                        operations: Some(1),
                        table_version: table.version().map(|v| v as u64),
                        files_scanned: None,
                        files_pruned: None,
                        bytes_scanned: None,
                        scan_time_ms: None,
                        rewrite_time_ms: None,
                    })
                }
            },
        )
        .await;
        out.push(into_case_result(c2));

        return Ok(out);
    }

    let table_url = narrow_sales_table_url(fixtures_dir, scale, storage)?;
    let mut out = Vec::new();

    let c1 = run_case_async("metadata_table_load", warmup, iterations, || {
        let storage = storage.clone();
        let table_url = table_url.clone();
        async move {
            let table = storage
                .open_table(table_url)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<SampleMetrics, String>(SampleMetrics {
                rows_processed: None,
                bytes_processed: None,
                operations: Some(1),
                table_version: table.version().map(|v| v as u64),
                files_scanned: None,
                files_pruned: None,
                bytes_scanned: None,
                scan_time_ms: None,
                rewrite_time_ms: None,
            })
        }
    })
    .await;
    out.push(into_case_result(c1));

    let c2 = run_case_async("metadata_time_travel_v0", warmup, iterations, || {
        let storage = storage.clone();
        let table_url = table_url.clone();
        async move {
            let mut table = storage
                .try_from_url_for_write(table_url)
                .await
                .map_err(|e| e.to_string())?;
            table.load_version(0).await.map_err(|e| e.to_string())?;
            Ok::<SampleMetrics, String>(SampleMetrics {
                rows_processed: None,
                bytes_processed: None,
                operations: Some(1),
                table_version: table.version().map(|v| v as u64),
                files_scanned: None,
                files_pruned: None,
                bytes_scanned: None,
                scan_time_ms: None,
                rewrite_time_ms: None,
            })
        }
    })
    .await;
    out.push(into_case_result(c2));

    Ok(out)
}

fn prepare_metadata_iteration(source_table_path: &Path) -> BenchResult<MetadataIterationSetup> {
    let temp = tempfile::tempdir()?;
    let table_dir = temp.path().join("table");
    copy_dir_all(source_table_path, &table_dir)?;
    let table_url = Url::from_directory_path(&table_dir).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create table URL for {}",
            table_dir.display()
        ))
    })?;
    Ok(MetadataIterationSetup {
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
