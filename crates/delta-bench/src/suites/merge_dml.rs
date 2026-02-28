use std::fs;
use std::path::Path;
use std::sync::Arc;

use deltalake_core::arrow;
use deltalake_core::datafusion::logical_expr::col;
use deltalake_core::datafusion::prelude::{DataFrame, SessionContext};
use deltalake_core::protocol::SaveMode;
use url::Url;

use crate::data::datasets::NarrowSaleRow;
use crate::data::fixtures::{load_rows, merge_target_table_path};
use crate::error::{BenchError, BenchResult};
use crate::results::{CaseFailure, CaseResult, SampleMetrics};
use crate::runner::{
    run_case_async_with_async_setup, run_case_async_with_setup, CaseExecutionResult,
};
use crate::storage::StorageConfig;

#[derive(Clone, Copy, Debug)]
pub struct MergeCase {
    pub name: &'static str,
    pub match_ratio: f64,
    pub mode: MergeMode,
}

#[derive(Clone, Copy, Debug)]
pub enum MergeMode {
    Upsert,
    Delete,
}

struct MergeIterationSetup {
    _temp: tempfile::TempDir,
    table_url: Url,
}

const MERGE_CASES: [MergeCase; 5] = [
    MergeCase {
        name: "delete_only_filesMatchedFraction_0.05_rowsMatchedFraction_0.05",
        match_ratio: 0.05,
        mode: MergeMode::Delete,
    },
    MergeCase {
        name: "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.1_rowsNotMatchedFraction_0.1",
        match_ratio: 0.1,
        mode: MergeMode::Upsert,
    },
    MergeCase {
        name: "merge_upsert_10pct",
        match_ratio: 0.10,
        mode: MergeMode::Upsert,
    },
    MergeCase {
        name: "merge_upsert_50pct",
        match_ratio: 0.50,
        mode: MergeMode::Upsert,
    },
    MergeCase {
        name: "merge_upsert_90pct",
        match_ratio: 0.90,
        mode: MergeMode::Upsert,
    },
];

pub fn case_names() -> Vec<String> {
    MERGE_CASES.iter().map(|c| c.name.to_string()).collect()
}

pub fn merge_case_names() -> Vec<&'static str> {
    MERGE_CASES.iter().map(|c| c.name).collect()
}

pub fn merge_case_by_name(name: &str) -> Option<&'static MergeCase> {
    MERGE_CASES
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    let rows = match load_rows(fixtures_dir, scale) {
        Ok(rows) => Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(&e.to_string())),
    };
    if storage.is_local() {
        let fixture_table_dir = merge_target_table_path(fixtures_dir, scale);
        let mut out = Vec::new();
        for case in MERGE_CASES {
            let c = run_case_async_with_setup(
                case.name,
                warmup,
                iterations,
                || prepare_merge_iteration(&fixture_table_dir).map_err(|e| e.to_string()),
                |setup| {
                    let rows = Arc::clone(&rows);
                    let storage = storage.clone();
                    async move {
                        let table_url = setup.table_url.clone();
                        let _keep_temp = setup;
                        run_merge_case(rows.as_slice(), table_url, case, &storage)
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

    let mut out = Vec::new();
    for case in MERGE_CASES {
        let c = run_case_async_with_async_setup(
            case.name,
            warmup,
            iterations,
            || {
                let rows = Arc::clone(&rows);
                let storage = storage.clone();
                async move {
                    let table_url = storage
                        .isolated_table_url(scale, "merge_target_delta", case.name)
                        .map_err(|e| e.to_string())?;
                    seed_merge_target_table(rows.as_slice(), table_url.clone(), &storage)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok::<Url, String>(table_url)
                }
            },
            |table_url| {
                let rows = Arc::clone(&rows);
                let storage = storage.clone();
                async move {
                    run_merge_case(rows.as_slice(), table_url, case, &storage)
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

fn prepare_merge_iteration(fixture_table_dir: &Path) -> BenchResult<MergeIterationSetup> {
    let temp = tempfile::tempdir()?;
    let table_dir = temp.path().join("target");
    copy_dir_all(fixture_table_dir, &table_dir)?;
    let table_url = Url::from_directory_path(&table_dir).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create table URL for {}",
            table_dir.display()
        ))
    })?;

    Ok(MergeIterationSetup {
        _temp: temp,
        table_url,
    })
}

async fn run_merge_case(
    rows: &[NarrowSaleRow],
    table_url: Url,
    case: MergeCase,
    storage: &StorageConfig,
) -> BenchResult<SampleMetrics> {
    let table = storage.open_table(table_url).await?;
    let (source, source_rows) = build_source_df(rows, case.match_ratio, case.mode)?;

    let predicate = col("target.id").eq(col("source.id"));

    let (table, merge_metrics) = match case.mode {
        MergeMode::Delete => {
            table
                .merge(source, predicate)
                .with_source_alias("source")
                .with_target_alias("target")
                .when_matched_delete(|delete| delete)?
                .await?
        }
        MergeMode::Upsert => {
            table
                .merge(source, predicate)
                .with_source_alias("source")
                .with_target_alias("target")
                .when_matched_update(|update| {
                    update
                        .update("value_i64", col("source.value_i64"))
                        .update("flag", col("source.flag"))
                })?
                .when_not_matched_insert(|insert| {
                    insert
                        .set("id", col("source.id"))
                        .set("ts_ms", col("source.ts_ms"))
                        .set("region", col("source.region"))
                        .set("value_i64", col("source.value_i64"))
                        .set("flag", col("source.flag"))
                })?
                .await?
        }
    };

    Ok(SampleMetrics::base(
        Some(source_rows as u64),
        None,
        Some(1),
        table.version().map(|v| v as u64),
    )
    .with_scan_rewrite_metrics(
        Some(merge_metrics.num_target_files_scanned as u64),
        Some(merge_metrics.num_target_files_skipped_during_scan as u64),
        None,
        Some(merge_metrics.scan_time_ms),
        Some(merge_metrics.rewrite_time_ms),
    ))
}

async fn seed_merge_target_table(
    rows: &[NarrowSaleRow],
    table_url: Url,
    storage: &StorageConfig,
) -> BenchResult<()> {
    let seed_rows = rows
        .iter()
        .take((rows.len() / 4).max(1024))
        .cloned()
        .collect::<Vec<_>>();
    let _ = storage
        .try_from_url_for_write(table_url)
        .await?
        .write(vec![rows_to_batch(&seed_rows)?])
        .with_save_mode(SaveMode::Overwrite)
        .await?;
    Ok(())
}

fn build_source_df(
    rows: &[NarrowSaleRow],
    match_ratio: f64,
    mode: MergeMode,
) -> BenchResult<(DataFrame, usize)> {
    let mut source_rows = Vec::new();
    let matched = ((rows.len() as f64) * match_ratio).round() as usize;
    let matched = matched.clamp(1, rows.len().max(1));

    for row in rows.iter().take(matched) {
        let mut next = row.clone();
        next.value_i64 += 7;
        source_rows.push(next);
    }

    if matches!(mode, MergeMode::Upsert) {
        for row in rows.iter().take((matched / 10).max(1)) {
            let mut next = row.clone();
            next.id = next.id.saturating_add(1_000_000_000);
            source_rows.push(next);
        }
    }

    let batch = rows_to_batch(&source_rows)?;
    let ctx = SessionContext::new();
    Ok((ctx.read_batch(batch)?, source_rows.len()))
}

fn rows_to_batch(rows: &[NarrowSaleRow]) -> BenchResult<arrow::record_batch::RecordBatch> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("ts_ms", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("value_i64", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("flag", arrow::datatypes::DataType::Boolean, false),
    ]));

    let ids: Vec<i64> = rows.iter().map(|r| r.id as i64).collect();
    let ts_ms: Vec<i64> = rows.iter().map(|r| r.ts_ms).collect();
    let regions: Vec<String> = rows.iter().map(|r| r.region.clone()).collect();
    let values: Vec<i64> = rows.iter().map(|r| r.value_i64).collect();
    let flags: Vec<bool> = rows.iter().map(|r| r.flag).collect();

    Ok(arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(ids)),
            Arc::new(arrow::array::Int64Array::from(ts_ms)),
            Arc::new(arrow::array::StringArray::from(regions)),
            Arc::new(arrow::array::Int64Array::from(values)),
            Arc::new(arrow::array::BooleanArray::from(flags)),
        ],
    )?)
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
