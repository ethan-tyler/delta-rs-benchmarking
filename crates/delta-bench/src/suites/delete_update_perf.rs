use std::path::Path;
use std::time::Duration;

use url::Url;

use deltalake_core::DeltaTable;

use super::delete_update::{run_delete_update_case, DeleteUpdateCase, DmlOperation};
use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::fixtures::{delete_update_small_files_table_path, read_partitioned_table_path};
use crate::error::{BenchError, BenchResult};
use crate::results::CaseResult;
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;

const DELETE_UPDATE_PERF_DELAY_ENV: &str = "DELTA_BENCH_DELETE_UPDATE_PERF_DELAY_MS";
const DELETE_UPDATE_PERF_ALLOW_DELAY_ENV: &str = "DELTA_BENCH_ALLOW_DELETE_UPDATE_PERF_DELAY";
const DELETE_UPDATE_PERF_VALIDATION_CANARY_CASE_ID: &str = "delete_perf_scattered_5pct_small_files";

struct IterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
}

const DELETE_UPDATE_PERF_CASES: [DeleteUpdateCase; 4] = [
    DeleteUpdateCase {
        name: "delete_perf_localized_1pct",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.01),
        partition_localized: true,
        small_files_seed: false,
    },
    DeleteUpdateCase {
        name: "delete_perf_scattered_5pct_small_files",
        operation: DmlOperation::Delete,
        rows_matched_fraction: Some(0.05),
        partition_localized: false,
        small_files_seed: true,
    },
    DeleteUpdateCase {
        name: "update_perf_literal_5pct_scattered",
        operation: DmlOperation::UpdateLiteral,
        rows_matched_fraction: Some(0.05),
        partition_localized: false,
        small_files_seed: true,
    },
    DeleteUpdateCase {
        name: "update_perf_all_rows_expr",
        operation: DmlOperation::UpdateAllExpression,
        rows_matched_fraction: None,
        partition_localized: false,
        small_files_seed: false,
    },
];

pub fn case_names() -> Vec<String> {
    DELETE_UPDATE_PERF_CASES
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
        for case in DELETE_UPDATE_PERF_CASES {
            let source = if case.small_files_seed {
                small_files_source.clone()
            } else {
                standard_source.clone()
            };

            let c = run_case_async_with_async_setup(
                case.name,
                warmup,
                iterations,
                || {
                    let source = source.clone();
                    let storage = storage.clone();
                    async move {
                        prepare_iteration(&source, &storage)
                            .await
                            .map_err(|e| e.to_string())
                    }
                },
                |setup| async move {
                    let _keep_temp = setup._temp;
                    apply_validation_delay(case.name)
                        .await
                        .map_err(|e| e.to_string())?;
                    run_delete_update_case(setup.table, case, lane)
                        .await
                        .map_err(|e| e.to_string())
                },
            )
            .await;
            out.push(into_case_result(c));
        }

        return Ok(out);
    }

    let rows = match crate::data::fixtures::load_rows(fixtures_dir, scale) {
        Ok(rows) => std::sync::Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(case_names(), &e.to_string())),
    };

    let mut out = Vec::new();
    for case in DELETE_UPDATE_PERF_CASES {
        let c = run_case_async_with_async_setup(
            case.name,
            warmup,
            iterations,
            || {
                let storage = storage.clone();
                let seed_rows = std::sync::Arc::clone(&rows);
                async move {
                    let (base_table_name, chunk_size) = if case.small_files_seed {
                        ("delete_update_small_files_delta", 64)
                    } else {
                        ("read_partitioned_delta", 128)
                    };
                    let table_url = storage
                        .isolated_table_url(scale, base_table_name, case.name)
                        .map_err(|e| e.to_string())?;
                    crate::data::fixtures::write_delta_table_partitioned_small_files(
                        table_url.clone(),
                        seed_rows.as_slice(),
                        chunk_size,
                        &["region"],
                        &storage,
                    )
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
                apply_validation_delay(case.name)
                    .await
                    .map_err(|e| e.to_string())?;
                run_delete_update_case(table, case, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(c));
    }

    Ok(out)
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

async fn apply_validation_delay(case_id: &str) -> BenchResult<()> {
    let Some(delay) = parse_validation_delay(case_id)? else {
        return Ok(());
    };
    tokio::time::sleep(delay).await;
    Ok(())
}

fn parse_validation_delay(case_id: &str) -> BenchResult<Option<Duration>> {
    let Some(raw) = std::env::var_os(DELETE_UPDATE_PERF_DELAY_ENV) else {
        return Ok(None);
    };
    if std::env::var(DELETE_UPDATE_PERF_ALLOW_DELAY_ENV).as_deref() != Ok("1") {
        return Err(BenchError::InvalidArgument(format!(
            "validation-only delete_update_perf delay injection requires {DELETE_UPDATE_PERF_ALLOW_DELAY_ENV}=1"
        )));
    }
    if case_id != DELETE_UPDATE_PERF_VALIDATION_CANARY_CASE_ID {
        return Ok(None);
    }
    let raw = raw.into_string().map_err(|_| {
        BenchError::InvalidArgument(format!(
            "{DELETE_UPDATE_PERF_DELAY_ENV} must be valid UTF-8"
        ))
    })?;
    let delay_ms = raw.parse::<u64>().map_err(|_| {
        BenchError::InvalidArgument(format!(
            "{DELETE_UPDATE_PERF_DELAY_ENV} must be an unsigned integer number of milliseconds"
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
        parse_validation_delay, DELETE_UPDATE_PERF_ALLOW_DELAY_ENV, DELETE_UPDATE_PERF_DELAY_ENV,
        DELETE_UPDATE_PERF_VALIDATION_CANARY_CASE_ID,
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
    fn delete_update_perf_delay_requires_explicit_validation_opt_in() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (DELETE_UPDATE_PERF_ALLOW_DELAY_ENV, ""),
            (DELETE_UPDATE_PERF_DELAY_ENV, "150"),
        ]);

        let err = parse_validation_delay(DELETE_UPDATE_PERF_VALIDATION_CANARY_CASE_ID)
            .expect_err("delay injection should fail closed without opt-in");

        assert!(
            err.to_string().contains(
                "validation-only delete_update_perf delay injection requires DELTA_BENCH_ALLOW_DELETE_UPDATE_PERF_DELAY=1"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn delete_update_perf_delay_targets_only_validation_canary_case() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (DELETE_UPDATE_PERF_ALLOW_DELAY_ENV, "1"),
            (DELETE_UPDATE_PERF_DELAY_ENV, "150"),
        ]);

        assert_eq!(
            parse_validation_delay(DELETE_UPDATE_PERF_VALIDATION_CANARY_CASE_ID)
                .expect("canary case delay should parse"),
            Some(Duration::from_millis(150))
        );
        assert_eq!(
            parse_validation_delay("delete_perf_localized_1pct")
                .expect("non-canary cases should stay unchanged"),
            None
        );
    }
}
