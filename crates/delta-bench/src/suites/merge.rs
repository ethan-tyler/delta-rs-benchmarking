use std::path::Path;
use std::sync::Arc;

use deltalake_core::datafusion::logical_expr::col;
use deltalake_core::datafusion::prelude::{DataFrame, SessionContext};
use serde_json::json;
use url::Url;

use deltalake_core::DeltaTable;

use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::datasets::NarrowSaleRow;
use crate::data::fixtures::{
    load_rows, merge_partitioned_target_table_path, merge_target_table_path, rows_to_batch,
    write_delta_table, write_delta_table_partitioned_small_files,
};
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::hash_json;
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics};
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;
use crate::validation::{lane_requires_semantic_validation, validate_table_state};
use crate::version_compat::optional_table_version_to_u64;

#[derive(Clone, Copy, Debug)]
pub struct MergeCase {
    pub name: &'static str,
    pub match_ratio: f64,
    pub mode: MergeMode,
    pub target_profile: MergeTargetProfile,
    pub source_region: Option<&'static str>,
    pub include_partition_predicate: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum MergeMode {
    Upsert,
    Delete,
}

#[derive(Clone, Copy, Debug)]
pub enum MergeTargetProfile {
    Standard,
    Partitioned,
}

struct MergeIterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
    source: DataFrame,
    source_rows: usize,
}

const MERGE_CASES: [MergeCase; 6] = [
    MergeCase {
        name: "merge_delete_5pct",
        match_ratio: 0.05,
        mode: MergeMode::Delete,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_upsert_10pct_insert_10pct",
        match_ratio: 0.1,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_upsert_10pct",
        match_ratio: 0.10,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_upsert_50pct",
        match_ratio: 0.50,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_upsert_90pct",
        match_ratio: 0.90,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_localized_1pct",
        match_ratio: 0.01,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Partitioned,
        source_region: Some("us"),
        include_partition_predicate: true,
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
    lane: BenchmarkLane,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    let rows = match load_rows(fixtures_dir, scale) {
        Ok(rows) => Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(case_names(), &e.to_string())),
    };
    if storage.is_local() {
        let standard_fixture = merge_target_table_path(fixtures_dir, scale)?;
        let partitioned_fixture = merge_partitioned_target_table_path(fixtures_dir, scale);
        if !standard_fixture.exists() || !partitioned_fixture.exists() {
            return Ok(fixture_error_cases(
                case_names(),
                "missing merge fixture tables; run bench data first",
            ));
        }

        let mut out = Vec::new();
        for case in MERGE_CASES {
            let fixture_table_dir =
                merge_fixture_table_path(fixtures_dir, scale, case.target_profile)?;
            let c = run_case_async_with_async_setup(
                case.name,
                warmup,
                iterations,
                || {
                    let fixture_table_dir = fixture_table_dir.clone();
                    let rows = Arc::clone(&rows);
                    let storage = storage.clone();
                    async move {
                        prepare_merge_iteration(&fixture_table_dir, rows.as_slice(), case, &storage)
                            .await
                            .map_err(|e| e.to_string())
                    }
                },
                |setup| async move {
                    let _keep_temp = setup._temp;
                    run_merge_case(setup.table, setup.source, setup.source_rows, case, lane)
                        .await
                        .map_err(|e| e.to_string())
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
                    let base_table_name = match case.target_profile {
                        MergeTargetProfile::Standard => "merge_target_delta",
                        MergeTargetProfile::Partitioned => "merge_partitioned_target_delta",
                    };
                    let table_url = storage
                        .isolated_table_url(scale, base_table_name, case.name)
                        .map_err(|e| e.to_string())?;
                    seed_merge_target_table(rows.as_slice(), table_url.clone(), case, &storage)
                        .await
                        .map_err(|e| e.to_string())?;
                    let table = storage
                        .open_table(table_url)
                        .await
                        .map_err(|e| e.to_string())?;
                    let (source, source_rows) = build_source_df(
                        rows.as_slice(),
                        case.match_ratio,
                        case.mode,
                        case.source_region,
                    )
                    .map_err(|e| e.to_string())?;
                    Ok::<(DeltaTable, DataFrame, usize), String>((table, source, source_rows))
                }
            },
            |(table, source, source_rows)| async move {
                run_merge_case(table, source, source_rows, case, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(c));
    }

    Ok(out)
}

pub(crate) fn merge_fixture_table_path(
    fixtures_dir: &Path,
    scale: &str,
    profile: MergeTargetProfile,
) -> BenchResult<std::path::PathBuf> {
    match profile {
        MergeTargetProfile::Standard => merge_target_table_path(fixtures_dir, scale),
        MergeTargetProfile::Partitioned => {
            Ok(merge_partitioned_target_table_path(fixtures_dir, scale))
        }
    }
}

async fn prepare_merge_iteration(
    fixture_table_dir: &Path,
    rows: &[NarrowSaleRow],
    case: MergeCase,
    storage: &StorageConfig,
) -> BenchResult<MergeIterationSetup> {
    let temp = tempfile::tempdir()?;
    let table_dir = temp.path().join("target");
    copy_dir_all(fixture_table_dir, &table_dir)?;
    let table_url = Url::from_directory_path(&table_dir).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create table URL for {}",
            table_dir.display()
        ))
    })?;
    let table = storage.open_table(table_url).await?;
    let (source, source_rows) =
        build_source_df(rows, case.match_ratio, case.mode, case.source_region)?;

    Ok(MergeIterationSetup {
        _temp: temp,
        table,
        source,
        source_rows,
    })
}

pub(crate) async fn run_merge_case(
    table: DeltaTable,
    source: DataFrame,
    source_rows: usize,
    case: MergeCase,
    lane: BenchmarkLane,
) -> BenchResult<SampleMetrics> {
    let mut predicate = col("target.id").eq(col("source.id"));
    if case.include_partition_predicate {
        predicate = predicate.and(col("target.region").eq(col("source.region")));
    }

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

    let table_version = optional_table_version_to_u64(table.version())?;
    let result_hash = hash_json(&json!({
        "source_rows": source_rows as u64,
        "table_version": table_version,
        "target_files_scanned": merge_metrics.num_target_files_scanned as u64,
        "target_files_pruned": merge_metrics.num_target_files_skipped_during_scan as u64,
    }))?;
    let mut schema_hash = hash_json(&json!([
        "source_rows:u64",
        "table_version:u64",
        "target_files_scanned:u64",
        "target_files_pruned:u64",
    ]))?;
    let mut semantic_state_digest = None;
    let mut validation_summary = None;
    if lane_requires_semantic_validation(lane) {
        let validation = validate_table_state(&table).await?;
        schema_hash = validation.schema_hash;
        semantic_state_digest = Some(validation.digest);
        validation_summary = Some(validation.summary);
    }

    Ok(
        SampleMetrics::base(Some(source_rows as u64), None, Some(1), table_version)
            .with_scan_rewrite(ScanRewriteMetrics {
                files_scanned: Some(merge_metrics.num_target_files_scanned as u64),
                files_pruned: Some(merge_metrics.num_target_files_skipped_during_scan as u64),
                bytes_scanned: None,
                scan_time_ms: Some(merge_metrics.scan_time_ms),
                rewrite_time_ms: Some(merge_metrics.rewrite_time_ms),
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
            }),
    )
}

pub(crate) async fn seed_merge_target_table(
    rows: &[NarrowSaleRow],
    table_url: Url,
    case: MergeCase,
    storage: &StorageConfig,
) -> BenchResult<()> {
    let seed_rows = rows
        .iter()
        .take((rows.len() / 4).max(1024))
        .cloned()
        .collect::<Vec<_>>();
    match case.target_profile {
        MergeTargetProfile::Standard => {
            write_delta_table(table_url, &seed_rows, storage).await?;
        }
        MergeTargetProfile::Partitioned => {
            write_delta_table_partitioned_small_files(
                table_url,
                &seed_rows,
                64,
                &["region"],
                storage,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) fn build_source_df(
    rows: &[NarrowSaleRow],
    match_ratio: f64,
    mode: MergeMode,
    source_region: Option<&str>,
) -> BenchResult<(DataFrame, usize)> {
    let candidate_rows = rows
        .iter()
        .filter(|row| match source_region {
            Some(region) => row.region == region,
            None => true,
        })
        .collect::<Vec<_>>();
    if candidate_rows.is_empty() {
        return Err(BenchError::InvalidArgument(
            "merge source selection produced no rows".to_string(),
        ));
    }

    let mut source_rows = Vec::new();
    let matched = ((candidate_rows.len() as f64) * match_ratio).round() as usize;
    let matched = matched.clamp(1, candidate_rows.len().max(1));

    for row in candidate_rows.iter().take(matched) {
        let mut next = (*row).clone();
        next.value_i64 += 7;
        source_rows.push(next);
    }

    if matches!(mode, MergeMode::Upsert) {
        for row in candidate_rows.iter().take((matched / 10).max(1)) {
            let mut next = (*row).clone();
            next.id = next.id.saturating_add(1_000_000_000);
            source_rows.push(next);
        }
    }

    let batch = rows_to_batch(&source_rows)?;
    let ctx = SessionContext::new();
    Ok((ctx.read_batch(batch)?, source_rows.len()))
}
