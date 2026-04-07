use std::path::Path;
use std::time::Duration;

use url::Url;

use deltalake_core::DeltaTable;

use super::optimize_vacuum::{run_optimize_case, run_vacuum_case, OPTIMIZE_COMPACT_TARGET_SIZE};
use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::fixtures::{
    optimize_compacted_table_path, optimize_small_files_table_path, vacuum_ready_table_path,
};
use crate::error::{BenchError, BenchResult};
use crate::results::CaseResult;
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;

const OPTIMIZE_PERF_DELAY_ENV: &str = "DELTA_BENCH_OPTIMIZE_PERF_DELAY_MS";
const OPTIMIZE_PERF_ALLOW_DELAY_ENV: &str = "DELTA_BENCH_ALLOW_OPTIMIZE_PERF_DELAY";
const OPTIMIZE_PERF_VALIDATION_CANARY_CASE_ID: &str = "optimize_perf_compact_small_files";

struct IterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
}

pub fn case_names() -> Vec<String> {
    vec![
        "optimize_perf_compact_small_files".to_string(),
        "optimize_perf_noop_already_compact".to_string(),
        "vacuum_perf_execute_lite".to_string(),
    ]
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
        let optimize_source = optimize_small_files_table_path(fixtures_dir, scale);
        let optimize_compacted_source = optimize_compacted_table_path(fixtures_dir, scale);
        let vacuum_source = vacuum_ready_table_path(fixtures_dir, scale);

        if !optimize_source.exists()
            || !optimize_compacted_source.exists()
            || !vacuum_source.exists()
        {
            return Ok(fixture_error_cases(
                case_names(),
                "missing optimize/vacuum fixture tables; run bench data first",
            ));
        }

        let mut out = Vec::new();

        let optimize = run_case_async_with_async_setup(
            "optimize_perf_compact_small_files",
            warmup,
            iterations,
            || {
                let source = optimize_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                apply_validation_delay("optimize_perf_compact_small_files")
                    .await
                    .map_err(|e| e.to_string())?;
                run_optimize_case(setup.table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(optimize));

        let noop = run_case_async_with_async_setup(
            "optimize_perf_noop_already_compact",
            warmup,
            iterations,
            || {
                let source = optimize_compacted_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_optimize_case(setup.table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(noop));

        let execute = run_case_async_with_async_setup(
            "vacuum_perf_execute_lite",
            warmup,
            iterations,
            || {
                let source = vacuum_source.clone();
                let storage = storage.clone();
                async move {
                    prepare_iteration(&source, &storage)
                        .await
                        .map_err(|e| e.to_string())
                }
            },
            |setup| async move {
                let _keep_temp = setup._temp;
                run_vacuum_case(setup.table, false, lane)
                    .await
                    .map_err(|e| e.to_string())
            },
        )
        .await;
        out.push(into_case_result(execute));

        return Ok(out);
    }

    let rows = match crate::data::fixtures::load_rows(fixtures_dir, scale) {
        Ok(rows) => std::sync::Arc::new(rows),
        Err(e) => return Ok(fixture_error_cases(case_names(), &e.to_string())),
    };
    let optimize_seed_rows = std::sync::Arc::new(
        rows.iter()
            .take((rows.len() / 2).max(2048))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let vacuum_seed_rows = std::sync::Arc::new(
        rows.iter()
            .take((rows.len() / 3).max(1024))
            .cloned()
            .collect::<Vec<_>>(),
    );
    let mut out = Vec::new();

    let optimize = run_case_async_with_async_setup(
        "optimize_perf_compact_small_files",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = std::sync::Arc::clone(&optimize_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(
                        scale,
                        "optimize_small_files_delta",
                        "optimize_perf_compact_small_files",
                    )
                    .map_err(|e| e.to_string())?;
                crate::data::fixtures::write_delta_table_small_files(
                    table_url.clone(),
                    rows.as_slice(),
                    128,
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
            apply_validation_delay("optimize_perf_compact_small_files")
                .await
                .map_err(|e| e.to_string())?;
            run_optimize_case(table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(optimize));

    let noop = run_case_async_with_async_setup(
        "optimize_perf_noop_already_compact",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = std::sync::Arc::clone(&optimize_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(
                        scale,
                        "optimize_compacted_delta",
                        "optimize_perf_noop_already_compact",
                    )
                    .map_err(|e| e.to_string())?;
                crate::data::fixtures::write_delta_table(
                    table_url.clone(),
                    rows.as_slice(),
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
            run_optimize_case(table, OPTIMIZE_COMPACT_TARGET_SIZE, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(noop));

    let execute = run_case_async_with_async_setup(
        "vacuum_perf_execute_lite",
        warmup,
        iterations,
        || {
            let storage = storage.clone();
            let rows = std::sync::Arc::clone(&vacuum_seed_rows);
            async move {
                let table_url = storage
                    .isolated_table_url(scale, "vacuum_ready_delta", "vacuum_perf_execute_lite")
                    .map_err(|e| e.to_string())?;
                crate::data::fixtures::write_vacuum_ready_table(
                    table_url.clone(),
                    rows.as_slice(),
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
            run_vacuum_case(table, false, lane)
                .await
                .map_err(|e| e.to_string())
        },
    )
    .await;
    out.push(into_case_result(execute));

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
    let Some(raw) = std::env::var_os(OPTIMIZE_PERF_DELAY_ENV) else {
        return Ok(None);
    };
    if std::env::var(OPTIMIZE_PERF_ALLOW_DELAY_ENV).as_deref() != Ok("1") {
        return Err(BenchError::InvalidArgument(format!(
            "validation-only optimize_perf delay injection requires {OPTIMIZE_PERF_ALLOW_DELAY_ENV}=1"
        )));
    }
    if case_id != OPTIMIZE_PERF_VALIDATION_CANARY_CASE_ID {
        return Ok(None);
    }
    let raw = raw.into_string().map_err(|_| {
        BenchError::InvalidArgument(format!("{OPTIMIZE_PERF_DELAY_ENV} must be valid UTF-8"))
    })?;
    let delay_ms = raw.parse::<u64>().map_err(|_| {
        BenchError::InvalidArgument(format!(
            "{OPTIMIZE_PERF_DELAY_ENV} must be an unsigned integer number of milliseconds"
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
        parse_validation_delay, OPTIMIZE_PERF_ALLOW_DELAY_ENV, OPTIMIZE_PERF_DELAY_ENV,
        OPTIMIZE_PERF_VALIDATION_CANARY_CASE_ID,
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
    fn optimize_perf_delay_requires_explicit_validation_opt_in() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (OPTIMIZE_PERF_ALLOW_DELAY_ENV, ""),
            (OPTIMIZE_PERF_DELAY_ENV, "150"),
        ]);

        let err = parse_validation_delay(OPTIMIZE_PERF_VALIDATION_CANARY_CASE_ID)
            .expect_err("delay injection should fail closed without opt-in");

        assert!(
            err.to_string().contains(
                "validation-only optimize_perf delay injection requires DELTA_BENCH_ALLOW_OPTIMIZE_PERF_DELAY=1"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn optimize_perf_delay_targets_only_validation_canary_case() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (OPTIMIZE_PERF_ALLOW_DELAY_ENV, "1"),
            (OPTIMIZE_PERF_DELAY_ENV, "150"),
        ]);

        assert_eq!(
            parse_validation_delay(OPTIMIZE_PERF_VALIDATION_CANARY_CASE_ID)
                .expect("canary case delay should parse"),
            Some(Duration::from_millis(150))
        );
        assert_eq!(
            parse_validation_delay("vacuum_perf_execute_lite")
                .expect("non-canary cases should stay unchanged"),
            None
        );
    }
}
