use std::path::Path;
use std::sync::Arc;

use deltalake_core::protocol::SaveMode;
use deltalake_core::DeltaTable;
use url::Url;

use super::{fixture_error_cases, into_case_result};
use crate::data::fixtures::{load_rows, rows_to_batch};
use crate::error::{BenchError, BenchResult};
use crate::results::{CaseResult, SampleMetrics};
use crate::runner::run_case_async;
use crate::storage::StorageConfig;

pub fn case_names() -> Vec<String> {
    vec![
        "write_append_small_batches".to_string(),
        "write_append_large_batches".to_string(),
        "write_overwrite".to_string(),
    ]
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    warmup: u32,
    iterations: u32,
    _storage: &StorageConfig, // TODO: support remote write benchmarks
) -> BenchResult<Vec<CaseResult>> {
    let rows = match load_rows(fixtures_dir, scale) {
        Ok(rows) => Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(case_names(), &e.to_string())),
    };
    let mut results = Vec::new();

    let small = run_case_async("write_append_small_batches", warmup, iterations, || {
        let rows = Arc::clone(&rows);
        async move {
            run_append_case(rows.as_slice(), 128)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .await;
    results.push(into_case_result(small));

    let large = run_case_async("write_append_large_batches", warmup, iterations, || {
        let rows = Arc::clone(&rows);
        async move {
            run_append_case(rows.as_slice(), 4096)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .await;
    results.push(into_case_result(large));

    let overwrite = run_case_async("write_overwrite", warmup, iterations, || {
        let rows = Arc::clone(&rows);
        async move {
            run_overwrite_case(rows.as_slice())
                .await
                .map_err(|e| e.to_string())
        }
    })
    .await;
    results.push(into_case_result(overwrite));

    Ok(results)
}

async fn run_append_case(
    rows: &[crate::data::datasets::NarrowSaleRow],
    chunk: usize,
) -> BenchResult<SampleMetrics> {
    let temp = tempfile::tempdir()?;
    let table_url = Url::from_directory_path(temp.path()).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create URL for {}",
            temp.path().display()
        ))
    })?;

    let mut operations = 0_u64;
    let mut table = DeltaTable::try_from_url(table_url).await?;
    for (idx, r) in rows.chunks(chunk).enumerate() {
        operations += 1;
        let mode = if idx == 0 {
            SaveMode::Overwrite
        } else {
            SaveMode::Append
        };
        let batch = rows_to_batch(r)?;
        table = table.write(vec![batch]).with_save_mode(mode).await?;
    }

    Ok(SampleMetrics::base(
        Some(rows.len() as u64),
        None,
        Some(operations),
        table.version().map(|v| v as u64),
    ))
}

async fn run_overwrite_case(
    rows: &[crate::data::datasets::NarrowSaleRow],
) -> BenchResult<SampleMetrics> {
    let temp = tempfile::tempdir()?;
    let table_url = Url::from_directory_path(temp.path()).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create URL for {}",
            temp.path().display()
        ))
    })?;
    let mut table = DeltaTable::try_from_url(table_url).await?;

    let first = rows_to_batch(rows)?;
    table = table
        .write(vec![first])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    let next = rows_to_batch(rows)?;
    table = table
        .write(vec![next])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    Ok(SampleMetrics::base(
        Some((rows.len() as u64) * 2),
        None,
        Some(2),
        table.version().map(|v| v as u64),
    ))
}
