use std::fs;
use std::path::Path;

use crate::error::{BenchError, BenchResult};
use crate::results::{CaseFailure, CaseResult};
use crate::runner::CaseExecutionResult;
use crate::storage::StorageConfig;

pub(crate) fn copy_dir_all(src: &Path, dst: &Path) -> BenchResult<()> {
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

pub(crate) fn into_case_result(result: CaseExecutionResult) -> CaseResult {
    match result {
        CaseExecutionResult::Success(c) | CaseExecutionResult::Failure(c) => c,
    }
}

pub(crate) fn fixture_error_cases(case_names: Vec<String>, message: &str) -> Vec<CaseResult> {
    case_names
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

pub mod delete_update_dml;
pub mod merge_dml;
pub mod metadata;
pub mod optimize_vacuum;
pub mod read_scan;
mod scan_metrics;
pub mod tpcds;
pub mod write;

/// Single source of truth for suite names. Adding a new suite requires updating
/// this array, `list_cases_for_target`, and `run_target`.
const SUITE_NAMES: [&str; 7] = [
    "read_scan",
    "write",
    "delete_update_dml",
    "merge_dml",
    "metadata",
    "optimize_vacuum",
    "tpcds",
];

pub fn list_targets() -> Vec<&'static str> {
    let mut targets: Vec<&str> = SUITE_NAMES.to_vec();
    targets.push("all");
    targets
}

pub fn list_cases_for_target(target: &str) -> BenchResult<Vec<String>> {
    match target {
        "read_scan" => Ok(read_scan::case_names()),
        "write" => Ok(write::case_names()),
        "delete_update_dml" => Ok(delete_update_dml::case_names()),
        "merge_dml" => Ok(merge_dml::case_names()),
        "metadata" => Ok(metadata::case_names()),
        "optimize_vacuum" => Ok(optimize_vacuum::case_names()),
        "tpcds" => Ok(tpcds::case_names()),
        "all" => {
            let mut names = Vec::new();
            for suite in SUITE_NAMES {
                names.extend(list_cases_for_target(suite)?);
            }
            Ok(names)
        }
        other => Err(BenchError::InvalidArgument(format!(
            "unknown suite target: {other}"
        ))),
    }
}

async fn run_single_suite(
    fixtures_dir: &Path,
    suite: &str,
    scale: &str,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    match suite {
        "read_scan" => read_scan::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "write" => write::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "delete_update_dml" => {
            delete_update_dml::run(fixtures_dir, scale, warmup, iterations, storage).await
        }
        "merge_dml" => merge_dml::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "metadata" => metadata::run(fixtures_dir, scale, warmup, iterations, storage).await,
        "optimize_vacuum" => {
            optimize_vacuum::run(fixtures_dir, scale, warmup, iterations, storage).await
        }
        "tpcds" => tpcds::run(fixtures_dir, scale, warmup, iterations, storage).await,
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
    if target == "all" {
        let mut out = Vec::new();
        for suite in SUITE_NAMES {
            out.extend(
                run_single_suite(fixtures_dir, suite, scale, warmup, iterations, storage).await?,
            );
        }
        Ok(out)
    } else {
        run_single_suite(fixtures_dir, target, scale, warmup, iterations, storage).await
    }
}
