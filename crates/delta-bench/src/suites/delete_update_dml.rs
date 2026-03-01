use std::path::Path;
use std::sync::Arc;

use url::Url;

use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::data::fixtures::{
    delete_update_small_files_table_path, load_rows, read_partitioned_table_path,
    write_delta_table_partitioned_small_files,
};
use crate::error::{BenchError, BenchResult};
use crate::results::{CaseResult, SampleMetrics};
use crate::runner::{run_case_async_with_async_setup, run_case_async_with_setup};
use crate::storage::StorageConfig;

#[derive(Clone, Copy)]
enum DmlOperation {
    Delete,
    UpdateLiteral,
    UpdateExpression,
    UpdateAllExpression,
}

#[derive(Clone, Copy)]
struct DeleteUpdateCase {
    name: &'static str,
    operation: DmlOperation,
    rows_matched_fraction: Option<f64>,
    partition_localized: bool,
    small_files_seed: bool,
}

struct IterationSetup {
    _temp: tempfile::TempDir,
    table_url: Url,
}

const DELETE_UPDATE_CASES: [DeleteUpdateCase; 7] = [
    DeleteUpdateCase {
        name: "delete_rowsMatchedFraction_0.01_partition_localized",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.01),
        partition_localized: true,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "delete_rowsMatchedFraction_0.05_scattered",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.05),
        partition_localized: false,
        small_files_seed: true,
    },
    DeleteUpdateCase {
        name: "delete_rowsMatchedFraction_0.50_broad",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.50),
        partition_localized: false,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "update_literal_rowsMatchedFraction_0.01_partition_localized",
        operation: DmlOperation::UpdateLiteral,
        rows_matched_fraction: Some(0.01),
        partition_localized: true,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "update_literal_rowsMatchedFraction_0.05_scattered",
        operation: DmlOperation::UpdateLiteral,
        rows_matched_fraction: Some(0.05),
        partition_localized: false,
        small_files_seed: true,
    },
    DeleteUpdateCase {
        name: "update_expression_rowsMatchedFraction_0.50_broad",
        operation: DmlOperation::UpdateExpression,
        rows_matched_fraction: Some(0.50),
        partition_localized: false,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "update_all_rows_expression",
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
                &small_files_source
            } else {
                &standard_source
            };

            let c = run_case_async_with_setup(
                case.name,
                warmup,
                iterations,
                || prepare_iteration(source).map_err(|e| e.to_string()),
                |setup| {
                    let storage = storage.clone();
                    async move {
                        let table_url = setup.table_url.clone();
                        let _keep_temp = setup;
                        run_delete_update_case(table_url, case, &storage)
                            .await
                            .map_err(|e| e.to_string())
                    }
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
                    Ok::<Url, String>(table_url)
                }
            },
            |table_url| {
                let storage = storage.clone();
                async move {
                    run_delete_update_case(table_url, case, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
        )
        .await;
        out.push(into_case_result(c));
    }

    Ok(out)
}

async fn run_delete_update_case(
    table_url: Url,
    case: DeleteUpdateCase,
    storage: &StorageConfig,
) -> BenchResult<SampleMetrics> {
    let table = storage.open_table(table_url).await?;

    match case.operation {
        DmlOperation::Delete => {
            let predicate = case_predicate(case).ok_or_else(|| {
                BenchError::InvalidArgument(format!("missing predicate for {}", case.name))
            })?;
            let (table, metrics) = table.delete().with_predicate(predicate.as_str()).await?;
            Ok(SampleMetrics::base(
                Some(metrics.num_deleted_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table.version().map(|v| v as u64),
            )
            .with_scan_rewrite_metrics(
                None,
                None,
                None,
                Some(metrics.scan_time_ms),
                Some(metrics.rewrite_time_ms),
            ))
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
            Ok(SampleMetrics::base(
                Some(metrics.num_updated_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table.version().map(|v| v as u64),
            )
            .with_scan_rewrite_metrics(
                None,
                None,
                None,
                Some(metrics.scan_time_ms),
                None,
            ))
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
            Ok(SampleMetrics::base(
                Some(metrics.num_updated_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table.version().map(|v| v as u64),
            )
            .with_scan_rewrite_metrics(
                None,
                None,
                None,
                Some(metrics.scan_time_ms),
                None,
            ))
        }
        DmlOperation::UpdateAllExpression => {
            let (table, metrics) = table
                .update()
                .with_update("value_i64", "value_i64 + 10")
                .await?;
            Ok(SampleMetrics::base(
                Some(metrics.num_updated_rows as u64),
                None,
                Some((metrics.num_added_files + metrics.num_removed_files) as u64),
                table.version().map(|v| v as u64),
            )
            .with_scan_rewrite_metrics(
                None,
                None,
                None,
                Some(metrics.scan_time_ms),
                None,
            ))
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
