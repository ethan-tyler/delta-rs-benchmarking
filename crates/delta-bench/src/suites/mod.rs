use std::path::Path;

use crate::error::{BenchError, BenchResult};
use crate::results::CaseResult;
use crate::storage::StorageConfig;

pub mod merge_dml;
pub mod metadata;
pub mod optimize_vacuum;
pub mod read_scan;
mod scan_metrics;
pub mod tpcds;
pub mod write;

pub fn list_targets() -> &'static [&'static str] {
    &[
        "read_scan",
        "write",
        "merge_dml",
        "metadata",
        "optimize_vacuum",
        "tpcds",
        "all",
    ]
}

pub fn list_cases_for_target(target: &str) -> BenchResult<Vec<String>> {
    match target {
        "read_scan" => Ok(read_scan::case_names()),
        "write" => Ok(write::case_names()),
        "merge_dml" => Ok(merge_dml::case_names()),
        "metadata" => Ok(metadata::case_names()),
        "optimize_vacuum" => Ok(optimize_vacuum::case_names()),
        "tpcds" => Ok(tpcds::case_names()),
        "all" => {
            let mut names = Vec::new();
            names.extend(read_scan::case_names());
            names.extend(write::case_names());
            names.extend(merge_dml::case_names());
            names.extend(metadata::case_names());
            names.extend(optimize_vacuum::case_names());
            names.extend(tpcds::case_names());
            Ok(names)
        }
        other => Err(BenchError::InvalidArgument(format!(
            "unknown suite target: {other}"
        ))),
    }
}

pub async fn run_target(
    fixtures_dir: &Path,
    target: &str,
    scale: &str,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    match target {
        "read_scan" => read_scan::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "write" => write::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "merge_dml" => merge_dml::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "metadata" => metadata::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "optimize_vacuum" => {
            optimize_vacuum::run(fixtures_dir, scale, warmup, iterations, storage).await
        }
        "tpcds" => tpcds::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "all" => {
            let mut out = Vec::new();
            out.extend(read_scan::run(fixtures_dir, scale, warmup, iterations, storage).await?);
            out.extend(write::run(fixtures_dir, scale, warmup, iterations, storage).await?);
            out.extend(merge_dml::run(fixtures_dir, scale, warmup, iterations, storage).await?);
            out.extend(metadata::run(fixtures_dir, scale, warmup, iterations, storage).await?);
            out.extend(
                optimize_vacuum::run(fixtures_dir, scale, warmup, iterations, storage).await?,
            );
            out.extend(tpcds::run(fixtures_dir, scale, warmup, iterations, storage).await?);
            Ok(out)
        }
        other => Err(BenchError::InvalidArgument(format!(
            "unknown suite target: {other}"
        ))),
    }
}
