use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use deltalake_core::datafusion::prelude::DataFrame;
use deltalake_core::DeltaTable;

use super::merge::{
    build_source_df, merge_fixture_table_path, run_merge_case, seed_merge_target_table, MergeCase,
    MergeMode, MergeTargetProfile,
};
use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::datasets::NarrowSaleRow;
use crate::data::fixtures::{
    load_rows, merge_partitioned_target_table_path, merge_target_table_path,
};
use crate::error::{BenchError, BenchResult};
use crate::results::CaseResult;
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;

const MERGE_PERF_DELAY_ENV: &str = "DELTA_BENCH_MERGE_PERF_DELAY_MS";
const MERGE_PERF_ALLOW_DELAY_ENV: &str = "DELTA_BENCH_ALLOW_MERGE_PERF_DELAY";
const MERGE_PERF_VALIDATION_CANARY_CASE_ID: &str = "merge_perf_upsert_50pct";

struct MergePerfIterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
    source: DataFrame,
    source_rows: usize,
}

const MERGE_PERF_CASES: [MergeCase; 4] = [
    MergeCase {
        name: "merge_perf_upsert_10pct",
        match_ratio: 0.10,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_perf_upsert_50pct",
        match_ratio: 0.50,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
    MergeCase {
        name: "merge_perf_localized_1pct",
        match_ratio: 0.01,
        mode: MergeMode::Upsert,
        target_profile: MergeTargetProfile::Partitioned,
        source_region: Some("us"),
        include_partition_predicate: true,
    },
    MergeCase {
        name: "merge_perf_delete_5pct",
        match_ratio: 0.05,
        mode: MergeMode::Delete,
        target_profile: MergeTargetProfile::Standard,
        source_region: None,
        include_partition_predicate: false,
    },
];

pub fn case_names() -> Vec<String> {
    MERGE_PERF_CASES
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
        for case in MERGE_PERF_CASES {
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
                        prepare_merge_perf_iteration(
                            &fixture_table_dir,
                            rows.as_slice(),
                            case,
                            &storage,
                        )
                        .await
                        .map_err(|e| e.to_string())
                    }
                },
                |setup| async move {
                    let _keep_temp = setup._temp;
                    apply_validation_delay(case.name)
                        .await
                        .map_err(|e| e.to_string())?;
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
    for case in MERGE_PERF_CASES {
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
                apply_validation_delay(case.name)
                    .await
                    .map_err(|e| e.to_string())?;
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

async fn prepare_merge_perf_iteration(
    fixture_table_dir: &Path,
    rows: &[NarrowSaleRow],
    case: MergeCase,
    storage: &StorageConfig,
) -> BenchResult<MergePerfIterationSetup> {
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

    Ok(MergePerfIterationSetup {
        _temp: temp,
        table,
        source,
        source_rows,
    })
}

async fn apply_validation_delay(case_id: &str) -> BenchResult<()> {
    let Some(delay) = parse_validation_delay(case_id)? else {
        return Ok(());
    };
    tokio::time::sleep(delay).await;
    Ok(())
}

fn parse_validation_delay(case_id: &str) -> BenchResult<Option<Duration>> {
    let Some(raw) = std::env::var_os(MERGE_PERF_DELAY_ENV) else {
        return Ok(None);
    };
    if std::env::var(MERGE_PERF_ALLOW_DELAY_ENV).as_deref() != Ok("1") {
        return Err(BenchError::InvalidArgument(format!(
            "validation-only merge_perf delay injection requires {MERGE_PERF_ALLOW_DELAY_ENV}=1"
        )));
    }
    if case_id != MERGE_PERF_VALIDATION_CANARY_CASE_ID {
        return Ok(None);
    }
    let raw = raw.into_string().map_err(|_| {
        BenchError::InvalidArgument(format!("{MERGE_PERF_DELAY_ENV} must be valid UTF-8"))
    })?;
    let delay_ms = raw.parse::<u64>().map_err(|_| {
        BenchError::InvalidArgument(format!(
            "{MERGE_PERF_DELAY_ENV} must be an unsigned integer number of milliseconds"
        ))
    })?;
    Ok(Some(Duration::from_millis(delay_ms)))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use super::{
        parse_validation_delay, MERGE_PERF_ALLOW_DELAY_ENV, MERGE_PERF_DELAY_ENV,
        MERGE_PERF_VALIDATION_CANARY_CASE_ID,
    };

    struct EnvRestoreGuard {
        previous: Vec<(String, Option<OsString>)>,
    }

    impl EnvRestoreGuard {
        fn set(entries: &[(&str, &str)]) -> Self {
            let previous = entries
                .iter()
                .map(|(key, _)| ((*key).to_string(), std::env::var_os(key)))
                .collect::<Vec<_>>();
            for (key, value) in entries {
                unsafe { std::env::set_var(key, value) };
            }
            Self { previous }
        }
    }

    impl Drop for EnvRestoreGuard {
        fn drop(&mut self) {
            for (key, value) in self.previous.drain(..) {
                if let Some(value) = value {
                    unsafe { std::env::set_var(&key, value) };
                } else {
                    unsafe { std::env::remove_var(&key) };
                }
            }
        }
    }

    fn env_mutex() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn merge_perf_delay_requires_explicit_validation_opt_in() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (MERGE_PERF_ALLOW_DELAY_ENV, ""),
            (MERGE_PERF_DELAY_ENV, "150"),
        ]);

        let err = parse_validation_delay(MERGE_PERF_VALIDATION_CANARY_CASE_ID)
            .expect_err("delay injection should fail closed without opt-in");

        assert!(
            err.to_string().contains(
                "validation-only merge_perf delay injection requires DELTA_BENCH_ALLOW_MERGE_PERF_DELAY=1"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn merge_perf_delay_targets_only_validation_canary_case() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (MERGE_PERF_ALLOW_DELAY_ENV, "1"),
            (MERGE_PERF_DELAY_ENV, "150"),
        ]);

        assert_eq!(
            parse_validation_delay(MERGE_PERF_VALIDATION_CANARY_CASE_ID)
                .expect("canary case delay should parse"),
            Some(Duration::from_millis(150))
        );
        assert_eq!(
            parse_validation_delay("merge_perf_upsert_10pct")
                .expect("non-canary cases should stay unchanged"),
            None
        );
    }
}
