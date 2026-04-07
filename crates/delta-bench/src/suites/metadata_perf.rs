use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use deltalake_core::datafusion::datasource::TableProvider;
use deltalake_core::kernel::Snapshot;
use deltalake_core::DeltaTable;
use serde_json::json;
use url::Url;

use super::{copy_dir_all, fixture_error_cases, into_case_result};
use crate::cli::BenchmarkLane;
use crate::data::fixtures::{
    metadata_checkpointed_table_path, metadata_checkpointed_table_url,
    metadata_long_history_table_path, metadata_long_history_table_url,
    metadata_uncheckpointed_table_path, metadata_uncheckpointed_table_url,
};
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::hash_json;
use crate::replay_snapshot::clone_plain_snapshot_from_loaded_table;
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics};
use crate::runner::{run_case_async, run_case_async_with_setup};
use crate::storage::StorageConfig;
use crate::validation::{lane_requires_semantic_validation, validate_table_state};

const METADATA_PERF_DELAY_ENV: &str = "DELTA_BENCH_METADATA_PERF_DELAY_MS";
const METADATA_PERF_ALLOW_DELAY_ENV: &str = "DELTA_BENCH_ALLOW_METADATA_PERF_DELAY";
const METADATA_PERF_VALIDATION_CANARY_CASE_ID: &str = "metadata_perf_load_head_long_history";

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataReplayVariant {
    LongHistory,
    CheckpointedHead,
    UncheckpointedHead,
}

impl MetadataReplayVariant {
    const fn fixture_name(self) -> &'static str {
        match self {
            Self::LongHistory => "long_history",
            Self::CheckpointedHead => "checkpointed_head",
            Self::UncheckpointedHead => "uncheckpointed_head",
        }
    }
}

#[derive(Clone, Copy)]
enum MetadataPerfOperation {
    LoadHead,
    TimeTravelVersionZero,
}

#[derive(Clone, Copy)]
struct MetadataPerfCase {
    name: &'static str,
    variant: MetadataReplayVariant,
    operation: MetadataPerfOperation,
}

const METADATA_PERF_CASES: [MetadataPerfCase; 4] = [
    MetadataPerfCase {
        name: "metadata_perf_load_head_long_history",
        variant: MetadataReplayVariant::LongHistory,
        operation: MetadataPerfOperation::LoadHead,
    },
    MetadataPerfCase {
        name: "metadata_perf_time_travel_v0_long_history",
        variant: MetadataReplayVariant::LongHistory,
        operation: MetadataPerfOperation::TimeTravelVersionZero,
    },
    MetadataPerfCase {
        name: "metadata_perf_load_checkpointed_head",
        variant: MetadataReplayVariant::CheckpointedHead,
        operation: MetadataPerfOperation::LoadHead,
    },
    MetadataPerfCase {
        name: "metadata_perf_load_uncheckpointed_head",
        variant: MetadataReplayVariant::UncheckpointedHead,
        operation: MetadataPerfOperation::LoadHead,
    },
];

struct MetadataIterationSetup {
    _temp: tempfile::TempDir,
    table_url: Url,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct MetadataReplayCaseSpec {
    table_url: Url,
    table_log_dir: PathBuf,
    variant: MetadataReplayVariant,
}

impl MetadataReplayCaseSpec {
    pub fn table_log_dir(&self) -> &Path {
        &self.table_log_dir
    }

    pub fn variant(&self) -> MetadataReplayVariant {
        self.variant
    }
}

#[doc(hidden)]
pub struct LoadedMetadataReplayTable {
    table: DeltaTable,
}

pub fn case_names() -> Vec<String> {
    METADATA_PERF_CASES
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
        let required_sources = [
            metadata_long_history_table_path(fixtures_dir, scale),
            metadata_checkpointed_table_path(fixtures_dir, scale),
            metadata_uncheckpointed_table_path(fixtures_dir, scale),
        ];
        if required_sources
            .iter()
            .any(|path| !path.join("_delta_log").exists())
        {
            return Ok(fixture_error_cases(
                case_names(),
                "missing metadata history fixture tables; run bench data --dataset-id many_versions first",
            ));
        }

        let mut out = Vec::new();
        for case in METADATA_PERF_CASES {
            let source = source_table_path(fixtures_dir, scale, case.variant);
            let c = run_case_async_with_setup(
                case.name,
                warmup,
                iterations,
                || prepare_metadata_iteration(&source).map_err(|e| e.to_string()),
                |setup| {
                    let storage = storage.clone();
                    async move {
                        let table_url = setup.table_url.clone();
                        let _keep_temp = setup;
                        apply_validation_delay(case.name)
                            .await
                            .map_err(|e| e.to_string())?;
                        run_metadata_case(&storage, table_url, case, lane)
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
    for case in METADATA_PERF_CASES {
        let table_url = source_table_url(fixtures_dir, scale, case.variant, storage)?;
        let c = run_case_async(case.name, warmup, iterations, || {
            let storage = storage.clone();
            let table_url = table_url.clone();
            async move {
                apply_validation_delay(case.name)
                    .await
                    .map_err(|e| e.to_string())?;
                run_metadata_case(&storage, table_url, case, lane)
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await;
        out.push(into_case_result(c));
    }

    Ok(out)
}

#[doc(hidden)]
pub fn benchmark_case_spec(
    fixtures_dir: &Path,
    scale: &str,
    variant: MetadataReplayVariant,
    storage: &StorageConfig,
) -> BenchResult<MetadataReplayCaseSpec> {
    let table_url = source_table_url(fixtures_dir, scale, variant, storage)?;
    let table_log_dir = source_table_path(fixtures_dir, scale, variant).join("_delta_log");
    Ok(MetadataReplayCaseSpec {
        table_url,
        table_log_dir,
        variant,
    })
}

#[doc(hidden)]
pub fn benchmark_has_last_checkpoint_hint(spec: &MetadataReplayCaseSpec) -> bool {
    spec.table_log_dir.join("_last_checkpoint").exists()
}

#[doc(hidden)]
pub async fn benchmark_load_case(
    storage: &StorageConfig,
    spec: MetadataReplayCaseSpec,
) -> BenchResult<LoadedMetadataReplayTable> {
    Ok(LoadedMetadataReplayTable {
        table: storage.open_table(spec.table_url).await?,
    })
}

#[doc(hidden)]
pub fn benchmark_clone_plain_snapshot(loaded: &LoadedMetadataReplayTable) -> BenchResult<Snapshot> {
    clone_plain_snapshot_from_loaded_table(&loaded.table)
}

#[doc(hidden)]
pub async fn benchmark_provider_from_snapshot(
    loaded: &LoadedMetadataReplayTable,
    snapshot: Snapshot,
) -> BenchResult<Arc<dyn TableProvider>> {
    Ok(loaded
        .table
        .table_provider()
        .with_snapshot(snapshot)
        .await?)
}

#[doc(hidden)]
pub async fn benchmark_control_provider_from_loaded(
    loaded: &LoadedMetadataReplayTable,
) -> BenchResult<Arc<dyn TableProvider>> {
    Ok(loaded.table.table_provider().await?)
}

#[doc(hidden)]
pub async fn benchmark_snapshot_at_version(
    loaded: &LoadedMetadataReplayTable,
    version: u64,
) -> BenchResult<Snapshot> {
    let version = i64::try_from(version).map_err(|_| {
        BenchError::InvalidArgument(format!("snapshot version {version} does not fit into i64"))
    })?;
    Ok(Snapshot::try_new(
        loaded.table.log_store().as_ref(),
        Default::default(),
        Some(version),
    )
    .await?)
}

async fn run_metadata_case(
    storage: &StorageConfig,
    table_url: Url,
    case: MetadataPerfCase,
    lane: BenchmarkLane,
) -> BenchResult<SampleMetrics> {
    let (table_version, schema_hash, semantic_state_digest, validation_summary) =
        match case.operation {
            MetadataPerfOperation::LoadHead => {
                let table = storage.open_table(table_url).await?;
                build_metadata_observation(&table, lane).await?
            }
            MetadataPerfOperation::TimeTravelVersionZero => {
                let mut table = storage.try_from_url_for_write(table_url).await?;
                table.load_version(0).await?;
                build_metadata_observation(&table, lane).await?
            }
        };

    let result_hash = hash_json(&json!({
        "operation": case.name,
        "fixture": case.variant.fixture_name(),
        "table_version": table_version,
    }))?;

    Ok(metadata_metrics(
        table_version,
        result_hash,
        schema_hash,
        semantic_state_digest,
        validation_summary,
    ))
}

async fn build_metadata_observation(
    table: &DeltaTable,
    lane: BenchmarkLane,
) -> BenchResult<(Option<u64>, String, Option<String>, Option<String>)> {
    let table_version = table.version().map(|version| version as u64);
    let mut schema_hash = hash_json(&json!(["operation:string", "table_version:u64"]))?;
    let mut semantic_state_digest = None;
    let mut validation_summary = None;
    if lane_requires_semantic_validation(lane) {
        let validation = validate_table_state(table).await?;
        schema_hash = validation.schema_hash;
        semantic_state_digest = Some(validation.digest);
        validation_summary = Some(validation.summary);
    }
    Ok((
        table_version,
        schema_hash,
        semantic_state_digest,
        validation_summary,
    ))
}

fn metadata_metrics(
    table_version: Option<u64>,
    result_hash: String,
    schema_hash: String,
    semantic_state_digest: Option<String>,
    validation_summary: Option<String>,
) -> SampleMetrics {
    SampleMetrics::base(None, None, Some(1), table_version).with_runtime_io(RuntimeIOMetrics {
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
    })
}

fn source_table_path(fixtures_dir: &Path, scale: &str, variant: MetadataReplayVariant) -> PathBuf {
    match variant {
        MetadataReplayVariant::LongHistory => metadata_long_history_table_path(fixtures_dir, scale),
        MetadataReplayVariant::CheckpointedHead => {
            metadata_checkpointed_table_path(fixtures_dir, scale)
        }
        MetadataReplayVariant::UncheckpointedHead => {
            metadata_uncheckpointed_table_path(fixtures_dir, scale)
        }
    }
}

fn source_table_url(
    fixtures_dir: &Path,
    scale: &str,
    variant: MetadataReplayVariant,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    match variant {
        MetadataReplayVariant::LongHistory => {
            metadata_long_history_table_url(fixtures_dir, scale, storage)
        }
        MetadataReplayVariant::CheckpointedHead => {
            metadata_checkpointed_table_url(fixtures_dir, scale, storage)
        }
        MetadataReplayVariant::UncheckpointedHead => {
            metadata_uncheckpointed_table_url(fixtures_dir, scale, storage)
        }
    }
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

async fn apply_validation_delay(case_id: &str) -> BenchResult<()> {
    let Some(delay) = parse_validation_delay(case_id)? else {
        return Ok(());
    };
    tokio::time::sleep(delay).await;
    Ok(())
}

fn parse_validation_delay(case_id: &str) -> BenchResult<Option<Duration>> {
    let Some(raw) = std::env::var_os(METADATA_PERF_DELAY_ENV) else {
        return Ok(None);
    };
    if std::env::var(METADATA_PERF_ALLOW_DELAY_ENV).as_deref() != Ok("1") {
        return Err(BenchError::InvalidArgument(format!(
            "validation-only metadata_perf delay injection requires {METADATA_PERF_ALLOW_DELAY_ENV}=1"
        )));
    }
    if case_id != METADATA_PERF_VALIDATION_CANARY_CASE_ID {
        return Ok(None);
    }
    let raw = raw.into_string().map_err(|_| {
        BenchError::InvalidArgument(format!("{METADATA_PERF_DELAY_ENV} must be valid UTF-8"))
    })?;
    let delay_ms = raw.parse::<u64>().map_err(|_| {
        BenchError::InvalidArgument(format!(
            "{METADATA_PERF_DELAY_ENV} must be an unsigned integer number of milliseconds"
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
        parse_validation_delay, METADATA_PERF_ALLOW_DELAY_ENV, METADATA_PERF_DELAY_ENV,
        METADATA_PERF_VALIDATION_CANARY_CASE_ID,
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
    fn metadata_perf_delay_requires_explicit_validation_opt_in() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (METADATA_PERF_ALLOW_DELAY_ENV, ""),
            (METADATA_PERF_DELAY_ENV, "150"),
        ]);

        let err = parse_validation_delay(METADATA_PERF_VALIDATION_CANARY_CASE_ID)
            .expect_err("delay injection should fail closed without opt-in");

        assert!(
            err.to_string().contains(
                "validation-only metadata_perf delay injection requires DELTA_BENCH_ALLOW_METADATA_PERF_DELAY=1"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn metadata_perf_delay_targets_only_validation_canary_case() {
        let _env_guard = env_mutex().lock().expect("env mutex");
        let _restore_guard = EnvRestoreGuard::set(&[
            (METADATA_PERF_ALLOW_DELAY_ENV, "1"),
            (METADATA_PERF_DELAY_ENV, "150"),
        ]);

        assert_eq!(
            parse_validation_delay(METADATA_PERF_VALIDATION_CANARY_CASE_ID)
                .expect("canary case delay should parse"),
            Some(Duration::from_millis(150))
        );
        assert_eq!(
            parse_validation_delay("metadata_perf_load_checkpointed_head")
                .expect("non-canary cases should stay unchanged"),
            None
        );
    }
}
