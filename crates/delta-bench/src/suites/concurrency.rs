use std::future::Future;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::kernel::transaction::{CommitConflictError, TransactionError};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField, StructType};
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaTable, DeltaTableError};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Barrier;
use url::Url;

use super::{copy_dir_all, fixture_error_cases};
use crate::data::datasets::NarrowSaleRow;
use crate::data::fixtures::{
    delete_update_small_files_table_path, load_rows, optimize_small_files_table_path, rows_to_batch,
};
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::hash_json;
use crate::results::{
    CaseFailure, CaseResult, ContentionMetrics, ElapsedStats, IterationSample, PerfStatus,
    RuntimeIOMetrics, SampleMetrics,
};
use crate::stats::compute_stats;
use crate::storage::StorageConfig;
use crate::version_compat::optional_table_version_to_u64;

const CREATE_WORKER_COUNT: usize = 4;
const APPEND_WORKER_COUNT: usize = 4;
const CONTENDED_WORKER_COUNT: usize = 2;
const CONTENDED_RACE_COUNT: usize = 3;
const APPEND_ROWS_PER_WORKER: usize = 512;
const CONTENDED_OPTIMIZE_TARGET_SIZE: u64 = 1_000_000;

fn update_vs_compaction_predicate() -> &'static str {
    "region = 'us' AND id % 17 = 0"
}

fn delete_vs_compaction_predicate() -> &'static str {
    "id % 20 = 0"
}

fn contended_optimize_target_size() -> NonZeroU64 {
    NonZeroU64::new(CONTENDED_OPTIMIZE_TARGET_SIZE).expect("target size must be non-zero")
}

pub fn case_names() -> Vec<String> {
    vec![
        "concurrent_table_create".to_string(),
        "concurrent_append_multi".to_string(),
        "update_vs_compaction".to_string(),
        "delete_vs_compaction".to_string(),
        "optimize_vs_optimize_overlap".to_string(),
    ]
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    if !storage.is_local() {
        return Ok(fixture_error_cases(
            case_names(),
            "concurrency suite does not support non-local storage backend yet",
        ));
    }

    let mut out = Vec::new();

    out.push(
        run_concurrency_case_with_setup(
            "concurrent_table_create",
            warmup,
            iterations,
            || async { prepare_create_sample().await },
            |setup| async move { execute_concurrent_table_create(setup).await },
        )
        .await,
    );

    match load_rows(fixtures_dir, scale) {
        Ok(rows) => {
            let limited_rows = Arc::new(
                rows.into_iter()
                    .take(APPEND_WORKER_COUNT * APPEND_ROWS_PER_WORKER)
                    .collect::<Vec<_>>(),
            );
            out.push(
                run_concurrency_case_with_setup(
                    "concurrent_append_multi",
                    warmup,
                    iterations,
                    {
                        let limited_rows = Arc::clone(&limited_rows);
                        move || {
                            let limited_rows = Arc::clone(&limited_rows);
                            async move { prepare_append_sample(limited_rows.as_ref()).await }
                        }
                    },
                    |setup| async move { execute_concurrent_append_multi(setup).await },
                )
                .await,
            );
        }
        Err(error) => out.extend(fixture_error_cases(
            vec!["concurrent_append_multi".to_string()],
            &error.to_string(),
        )),
    }

    let delete_update_source = delete_update_small_files_table_path(fixtures_dir, scale);
    if delete_update_source.exists() {
        out.push(
            run_contended_case(
                "update_vs_compaction",
                warmup,
                iterations,
                &delete_update_source,
                storage,
                |setup| async move { execute_update_vs_compaction(setup).await },
            )
            .await,
        );
        out.push(
            run_contended_case(
                "delete_vs_compaction",
                warmup,
                iterations,
                &delete_update_source,
                storage,
                |setup| async move { execute_delete_vs_compaction(setup).await },
            )
            .await,
        );
    } else {
        out.extend(fixture_error_cases(
            vec![
                "update_vs_compaction".to_string(),
                "delete_vs_compaction".to_string(),
            ],
            "missing delete/update small-files fixture table; run bench data first",
        ));
    }

    let optimize_source = optimize_small_files_table_path(fixtures_dir, scale);
    if optimize_source.exists() {
        out.push(
            run_contended_case(
                "optimize_vs_optimize_overlap",
                warmup,
                iterations,
                &optimize_source,
                storage,
                |setup| async move { execute_optimize_vs_optimize_overlap(setup).await },
            )
            .await,
        );
    } else {
        out.extend(fixture_error_cases(
            vec!["optimize_vs_optimize_overlap".to_string()],
            "missing optimize small-files fixture table; run bench data first",
        ));
    }

    Ok(out)
}

struct CreateSampleSetup {
    _temp: TempDir,
    tables: Vec<DeltaTable>,
}

struct AppendWorker {
    table: DeltaTable,
    batch: RecordBatch,
}

struct AppendSampleSetup {
    _temp: TempDir,
    workers: Vec<AppendWorker>,
}

struct TwoWorkerRace {
    left: DeltaTable,
    right: DeltaTable,
}

struct ContendedSampleSetup {
    _temp: TempDir,
    races: Vec<TwoWorkerRace>,
}

#[derive(Clone, Debug)]
struct SampleExecution {
    metrics: SampleMetrics,
    failure: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ContentionErrorKind {
    Append,
    DeleteRead,
    DeleteDelete,
    MetadataChanged,
    ProtocolChanged,
    Transaction,
    VersionAlreadyExists,
    MaxCommitAttemptsExceeded,
}

#[derive(Clone, Debug)]
enum WorkerOutcome {
    Success { table_version: Option<u64> },
    Classified(ContentionErrorKind),
    Unexpected(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TableVersionPolicy {
    MaxObserved,
    Omit,
}

async fn run_contended_case<ExecF, ExecFut>(
    name: &str,
    warmup: u32,
    iterations: u32,
    source: &Path,
    storage: &StorageConfig,
    execute: ExecF,
) -> CaseResult
where
    ExecF: FnMut(ContendedSampleSetup) -> ExecFut,
    ExecFut: Future<Output = BenchResult<SampleExecution>>,
{
    let source = source.to_path_buf();
    let storage = storage.clone();
    run_concurrency_case_with_setup(
        name,
        warmup,
        iterations,
        {
            let source = source.clone();
            let storage = storage.clone();
            move || {
                let source = source.clone();
                let storage = storage.clone();
                async move { prepare_contended_sample(&source, &storage).await }
            }
        },
        execute,
    )
    .await
}

async fn prepare_create_sample() -> BenchResult<CreateSampleSetup> {
    let temp = tempfile::tempdir()?;
    let table_url = directory_url(temp.path())?;
    let mut tables = Vec::with_capacity(CREATE_WORKER_COUNT);
    for _ in 0..CREATE_WORKER_COUNT {
        tables.push(DeltaTable::try_from_url(table_url.clone()).await?);
    }
    Ok(CreateSampleSetup {
        _temp: temp,
        tables,
    })
}

async fn prepare_append_sample(rows: &[NarrowSaleRow]) -> BenchResult<AppendSampleSetup> {
    let temp = tempfile::tempdir()?;
    let table_url = directory_url(temp.path())?;
    let schema = concurrency_schema();
    let _ = DeltaTable::try_from_url(table_url.clone())
        .await?
        .create()
        .with_columns(schema.fields().cloned())
        .await?;

    let mut workers = Vec::with_capacity(APPEND_WORKER_COUNT);
    for chunk in rows.chunks(APPEND_ROWS_PER_WORKER) {
        let table = DeltaTable::try_from_url(table_url.clone()).await?;
        let batch = rows_to_batch(chunk)?;
        workers.push(AppendWorker { table, batch });
    }

    Ok(AppendSampleSetup {
        _temp: temp,
        workers,
    })
}

async fn prepare_contended_sample(
    source: &Path,
    storage: &StorageConfig,
) -> BenchResult<ContendedSampleSetup> {
    let temp = tempfile::tempdir()?;
    let mut races = Vec::with_capacity(CONTENDED_RACE_COUNT);
    for idx in 0..CONTENDED_RACE_COUNT {
        let race_path = temp.path().join(format!("race_{idx}"));
        copy_dir_all(source, &race_path)?;
        let table_url = storage.table_url_for(&race_path, "sf1", "ignored")?;
        let left = storage.open_table(table_url.clone()).await?;
        let right = storage.open_table(table_url).await?;
        races.push(TwoWorkerRace { left, right });
    }
    Ok(ContendedSampleSetup { _temp: temp, races })
}

async fn execute_concurrent_table_create(setup: CreateSampleSetup) -> BenchResult<SampleExecution> {
    let schema = Arc::new(concurrency_schema());
    let outcomes = run_barrier_race(
        setup.tables,
        Arc::new(move |table: DeltaTable| {
            let schema = Arc::clone(&schema);
            async move {
                classify_table_version_result(
                    table
                        .create()
                        .with_columns(schema.fields().cloned())
                        .with_save_mode(SaveMode::Ignore)
                        .await
                        .and_then(|table| checked_table_version(&table)),
                )
            }
        }),
    )
    .await?;
    Ok(aggregate_sample_execution(
        CREATE_WORKER_COUNT,
        1,
        outcomes,
        TableVersionPolicy::MaxObserved,
    ))
}

async fn execute_concurrent_append_multi(setup: AppendSampleSetup) -> BenchResult<SampleExecution> {
    let outcomes = run_barrier_race(
        setup.workers,
        Arc::new(|worker: AppendWorker| async move {
            classify_table_version_result(
                worker
                    .table
                    .write(vec![worker.batch])
                    .with_save_mode(SaveMode::Append)
                    .await
                    .and_then(|table| checked_table_version(&table)),
            )
        }),
    )
    .await?;
    Ok(aggregate_sample_execution(
        APPEND_WORKER_COUNT,
        1,
        outcomes,
        TableVersionPolicy::MaxObserved,
    ))
}

async fn execute_update_vs_compaction(setup: ContendedSampleSetup) -> BenchResult<SampleExecution> {
    enum Worker {
        Update(DeltaTable),
        Compact(DeltaTable),
    }

    let mut outcomes = Vec::new();
    for race in setup.races {
        outcomes.extend(
            run_barrier_race(
                vec![Worker::Update(race.left), Worker::Compact(race.right)],
                Arc::new(|worker| async move {
                    match worker {
                        Worker::Update(table) => classify_table_version_result(
                            table
                                .update()
                                .with_predicate(update_vs_compaction_predicate())
                                .with_update("value_i64", "value_i64 + 1")
                                .await
                                .and_then(|(table, _)| checked_table_version(&table)),
                        ),
                        Worker::Compact(table) => classify_table_version_result(
                            table
                                .optimize()
                                .with_target_size(contended_optimize_target_size().into())
                                .await
                                .and_then(|(table, _)| checked_table_version(&table)),
                        ),
                    }
                }),
            )
            .await?,
        );
    }

    Ok(aggregate_sample_execution(
        CONTENDED_WORKER_COUNT,
        CONTENDED_RACE_COUNT,
        outcomes,
        TableVersionPolicy::Omit,
    ))
}

async fn execute_delete_vs_compaction(setup: ContendedSampleSetup) -> BenchResult<SampleExecution> {
    enum Worker {
        Delete(DeltaTable),
        Compact(DeltaTable),
    }

    let mut outcomes = Vec::new();
    for race in setup.races {
        outcomes.extend(
            run_barrier_race(
                vec![Worker::Delete(race.left), Worker::Compact(race.right)],
                Arc::new(|worker| async move {
                    match worker {
                        Worker::Delete(table) => classify_table_version_result(
                            table
                                .delete()
                                .with_predicate(delete_vs_compaction_predicate())
                                .await
                                .and_then(|(table, _)| checked_table_version(&table)),
                        ),
                        Worker::Compact(table) => classify_table_version_result(
                            table
                                .optimize()
                                .with_target_size(contended_optimize_target_size().into())
                                .await
                                .and_then(|(table, _)| checked_table_version(&table)),
                        ),
                    }
                }),
            )
            .await?,
        );
    }

    Ok(aggregate_sample_execution(
        CONTENDED_WORKER_COUNT,
        CONTENDED_RACE_COUNT,
        outcomes,
        TableVersionPolicy::Omit,
    ))
}

async fn execute_optimize_vs_optimize_overlap(
    setup: ContendedSampleSetup,
) -> BenchResult<SampleExecution> {
    let mut outcomes = Vec::new();
    for race in setup.races {
        outcomes.extend(
            run_barrier_race(
                vec![race.left, race.right],
                Arc::new(|table: DeltaTable| async move {
                    classify_table_version_result(
                        table
                            .optimize()
                            .with_target_size(contended_optimize_target_size().into())
                            .await
                            .and_then(|(table, _)| checked_table_version(&table)),
                    )
                }),
            )
            .await?,
        );
    }

    Ok(aggregate_sample_execution(
        CONTENDED_WORKER_COUNT,
        CONTENDED_RACE_COUNT,
        outcomes,
        TableVersionPolicy::Omit,
    ))
}

async fn run_barrier_race<W, O, F, Fut>(workers: Vec<W>, op: Arc<F>) -> BenchResult<Vec<O>>
where
    W: Send + 'static,
    O: Send + 'static,
    F: Fn(W) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(workers.len()));
    let mut handles = Vec::with_capacity(workers.len());
    for worker in workers {
        let barrier = Arc::clone(&barrier);
        let op = Arc::clone(&op);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            op(worker).await
        }));
    }

    let mut out = Vec::with_capacity(handles.len());
    for handle in handles {
        out.push(handle.await.map_err(|error| {
            BenchError::InvalidArgument(format!("concurrency worker task failed: {error}"))
        })?);
    }
    Ok(out)
}

fn classify_table_version_result(result: Result<Option<u64>, DeltaTableError>) -> WorkerOutcome {
    match result {
        Ok(table_version) => WorkerOutcome::Success { table_version },
        Err(error) => classify_delta_error(error),
    }
}

fn checked_table_version(table: &DeltaTable) -> Result<Option<u64>, DeltaTableError> {
    optional_table_version_to_u64(table.version())
        .map_err(|error| DeltaTableError::Generic(error.to_string()))
}

fn classify_delta_error(error: DeltaTableError) -> WorkerOutcome {
    match error {
        DeltaTableError::VersionAlreadyExists(_) => {
            WorkerOutcome::Classified(ContentionErrorKind::VersionAlreadyExists)
        }
        DeltaTableError::Transaction { source } => classify_transaction_error(source),
        other => WorkerOutcome::Unexpected(other.to_string()),
    }
}

fn classify_transaction_error(error: TransactionError) -> WorkerOutcome {
    match error {
        TransactionError::VersionAlreadyExists(_) => {
            WorkerOutcome::Classified(ContentionErrorKind::VersionAlreadyExists)
        }
        TransactionError::MaxCommitAttempts(_) => {
            WorkerOutcome::Classified(ContentionErrorKind::MaxCommitAttemptsExceeded)
        }
        TransactionError::CommitConflict(conflict) => match conflict {
            CommitConflictError::ConcurrentAppend => {
                WorkerOutcome::Classified(ContentionErrorKind::Append)
            }
            CommitConflictError::ConcurrentDeleteRead => {
                WorkerOutcome::Classified(ContentionErrorKind::DeleteRead)
            }
            CommitConflictError::ConcurrentDeleteDelete => {
                WorkerOutcome::Classified(ContentionErrorKind::DeleteDelete)
            }
            CommitConflictError::MetadataChanged => {
                WorkerOutcome::Classified(ContentionErrorKind::MetadataChanged)
            }
            CommitConflictError::ProtocolChanged(_) => {
                WorkerOutcome::Classified(ContentionErrorKind::ProtocolChanged)
            }
            CommitConflictError::ConcurrentTransaction => {
                WorkerOutcome::Classified(ContentionErrorKind::Transaction)
            }
            other => WorkerOutcome::Unexpected(other.to_string()),
        },
        other => WorkerOutcome::Unexpected(other.to_string()),
    }
}

fn aggregate_sample_execution(
    worker_count: usize,
    race_count: usize,
    outcomes: Vec<WorkerOutcome>,
    table_version_policy: TableVersionPolicy,
) -> SampleExecution {
    let mut contention = ContentionMetrics {
        worker_count: worker_count as u64,
        race_count: race_count as u64,
        ..Default::default()
    };
    let mut versions = Vec::new();
    let mut unexpected = Vec::new();

    for outcome in outcomes {
        contention.ops_attempted += 1;
        match outcome {
            WorkerOutcome::Success { table_version } => {
                contention.ops_succeeded += 1;
                if let Some(version) = table_version {
                    versions.push(version);
                }
            }
            WorkerOutcome::Classified(kind) => {
                contention.ops_failed += 1;
                match kind {
                    ContentionErrorKind::Append => contention.conflict_append += 1,
                    ContentionErrorKind::DeleteRead => contention.conflict_delete_read += 1,
                    ContentionErrorKind::DeleteDelete => {
                        contention.conflict_delete_delete += 1;
                    }
                    ContentionErrorKind::MetadataChanged => {
                        contention.conflict_metadata_changed += 1;
                    }
                    ContentionErrorKind::ProtocolChanged => {
                        contention.conflict_protocol_changed += 1;
                    }
                    ContentionErrorKind::Transaction => {
                        contention.conflict_transaction += 1;
                    }
                    ContentionErrorKind::VersionAlreadyExists => {
                        contention.version_already_exists += 1;
                    }
                    ContentionErrorKind::MaxCommitAttemptsExceeded => {
                        contention.max_commit_attempts_exceeded += 1;
                    }
                }
            }
            WorkerOutcome::Unexpected(message) => {
                contention.ops_failed += 1;
                contention.other_errors += 1;
                unexpected.push(message);
            }
        }
    }

    let table_version = match table_version_policy {
        TableVersionPolicy::MaxObserved => versions.into_iter().max(),
        TableVersionPolicy::Omit => None,
    };

    SampleExecution {
        metrics: SampleMetrics::base(None, None, Some(contention.ops_attempted), table_version)
            .with_contention(contention),
        failure: (!unexpected.is_empty()).then(|| unexpected.join(" | ")),
    }
}

fn attach_concurrency_schema_hash(mut sample: SampleExecution) -> BenchResult<SampleExecution> {
    let schema_hash = hash_json(&json!([
        "operations:u64",
        "table_version:u64",
        "contention.worker_count:u64",
        "contention.race_count:u64",
        "contention.ops_attempted:u64",
        "contention.ops_succeeded:u64",
        "contention.ops_failed:u64",
        "contention.conflict_append:u64",
        "contention.conflict_delete_read:u64",
        "contention.conflict_delete_delete:u64",
        "contention.conflict_metadata_changed:u64",
        "contention.conflict_protocol_changed:u64",
        "contention.conflict_transaction:u64",
        "contention.version_already_exists:u64",
        "contention.max_commit_attempts_exceeded:u64",
        "contention.other_errors:u64",
    ]))?;
    sample.metrics = sample.metrics.with_runtime_io(RuntimeIOMetrics {
        peak_rss_mb: None,
        cpu_time_ms: None,
        bytes_read: None,
        bytes_written: None,
        files_touched: None,
        files_skipped: None,
        spill_bytes: None,
        result_hash: None,
        schema_hash: Some(schema_hash),
        semantic_state_digest: None,
        validation_summary: None,
    });
    Ok(sample)
}

async fn run_concurrency_case_with_setup<S, SetupF, SetupFut, ExecF, ExecFut>(
    name: &str,
    warmup: u32,
    iterations: u32,
    mut setup: SetupF,
    mut execute: ExecF,
) -> CaseResult
where
    SetupF: FnMut() -> SetupFut,
    SetupFut: Future<Output = BenchResult<S>>,
    ExecF: FnMut(S) -> ExecFut,
    ExecFut: Future<Output = BenchResult<SampleExecution>>,
{
    for warmup_idx in 0..warmup {
        let input = match setup().await {
            Ok(input) => input,
            Err(error) => {
                return failure_case_result(
                    name,
                    Vec::new(),
                    format!(
                        "warmup setup iteration {} failed: {}",
                        warmup_idx + 1,
                        error
                    ),
                );
            }
        };
        match execute(input)
            .await
            .and_then(attach_concurrency_schema_hash)
        {
            Ok(sample) if sample.failure.is_none() => {}
            Ok(sample) => {
                return failure_case_result(
                    name,
                    Vec::new(),
                    format!(
                        "warmup iteration {} failed: {}",
                        warmup_idx + 1,
                        sample
                            .failure
                            .unwrap_or_else(|| "unexpected failure".to_string())
                    ),
                );
            }
            Err(error) => {
                return failure_case_result(
                    name,
                    Vec::new(),
                    format!("warmup iteration {} failed: {}", warmup_idx + 1, error),
                );
            }
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let input = match setup().await {
            Ok(input) => input,
            Err(error) => return failure_case_result(name, samples, error.to_string()),
        };

        let start = Instant::now();
        let sample = match execute(input)
            .await
            .and_then(attach_concurrency_schema_hash)
        {
            Ok(sample) => sample,
            Err(error) => return failure_case_result(name, samples, error.to_string()),
        };
        append_sample(&mut samples, start.elapsed(), sample.metrics);
        if let Some(message) = sample.failure {
            return failure_case_result(name, samples, message);
        }
    }

    success_case_result(name, samples)
}

fn append_sample(samples: &mut Vec<IterationSample>, elapsed: Duration, metrics: SampleMetrics) {
    samples.push(IterationSample {
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        rows: metrics.rows_processed,
        bytes: metrics.bytes_processed,
        metrics: Some(metrics),
    });
}

fn success_case_result(name: &str, samples: Vec<IterationSample>) -> CaseResult {
    CaseResult {
        case: name.to_string(),
        success: true,
        validation_passed: true,
        perf_status: PerfStatus::Trusted,
        classification: "supported".to_string(),
        elapsed_stats: elapsed_stats_from_samples(&samples),
        run_summary: None,
        run_summaries: None,
        suite_manifest_hash: None,
        case_definition_hash: None,
        compatibility_key: None,
        supports_decision: None,
        required_runs: None,
        decision_threshold_pct: None,
        decision_metric: None,
        samples,
        failure_kind: None,
        failure: None,
    }
}

fn failure_case_result(name: &str, samples: Vec<IterationSample>, message: String) -> CaseResult {
    CaseResult {
        case: name.to_string(),
        success: false,
        validation_passed: false,
        perf_status: PerfStatus::Invalid,
        classification: "supported".to_string(),
        elapsed_stats: elapsed_stats_from_samples(&samples),
        run_summary: None,
        run_summaries: None,
        suite_manifest_hash: None,
        case_definition_hash: None,
        compatibility_key: None,
        supports_decision: None,
        required_runs: None,
        decision_threshold_pct: None,
        decision_metric: None,
        samples,
        failure_kind: Some("execution_error".to_string()),
        failure: Some(CaseFailure { message }),
    }
}

fn elapsed_stats_from_samples(samples: &[IterationSample]) -> Option<ElapsedStats> {
    let elapsed = samples
        .iter()
        .map(|sample| sample.elapsed_ms)
        .collect::<Vec<_>>();
    let stats = compute_stats(&elapsed)?;
    Some(ElapsedStats {
        min_ms: stats.min_ms,
        max_ms: stats.max_ms,
        mean_ms: stats.mean_ms,
        median_ms: stats.median_ms,
        stddev_ms: stats.stddev_ms,
        cv_pct: stats.cv_pct,
    })
}

fn directory_url(path: &Path) -> BenchResult<Url> {
    Url::from_directory_path(path).map_err(|()| {
        BenchError::InvalidArgument(format!("failed to create URL for {}", path.display()))
    })
}

/// Minimal schema used only by concurrency suite cases (create, append).
/// Intentionally separate from the fixture schemas in `data::fixtures` — this
/// schema only needs the columns that the concurrency operations touch.
fn concurrency_schema() -> StructType {
    StructType::try_new(vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Long), true),
        StructField::new("ts_ms", DataType::Primitive(PrimitiveType::Long), true),
        StructField::new("region", DataType::Primitive(PrimitiveType::String), true),
        StructField::new("value_i64", DataType::Primitive(PrimitiveType::Long), true),
        StructField::new("flag", DataType::Primitive(PrimitiveType::Boolean), true),
    ])
    .expect("static concurrency schema should be valid")
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{
        aggregate_sample_execution, delete_vs_compaction_predicate, run_barrier_race,
        run_concurrency_case_with_setup, update_vs_compaction_predicate, ContentionErrorKind,
        TableVersionPolicy, WorkerOutcome,
    };
    use crate::results::SampleMetrics;

    #[tokio::test(flavor = "multi_thread")]
    async fn setup_delay_is_not_counted_in_concurrency_runner_elapsed_time() {
        let case = run_concurrency_case_with_setup(
            "timing_case",
            0,
            1,
            || async {
                tokio::time::sleep(Duration::from_millis(25)).await;
                Ok::<(), crate::error::BenchError>(())
            },
            |_| async {
                Ok::<_, crate::error::BenchError>(aggregate_sample_execution(
                    1,
                    1,
                    vec![WorkerOutcome::Success {
                        table_version: Some(0),
                    }],
                    TableVersionPolicy::MaxObserved,
                ))
            },
        )
        .await;

        assert!(case.success, "unexpected failure: {:?}", case.failure);
        assert_eq!(case.samples.len(), 1);
        assert!(
            case.samples[0].elapsed_ms < 50.0,
            "setup delay leaked into measured time: {} ms",
            case.samples[0].elapsed_ms
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn barrier_race_releases_workers_together() {
        let starts = run_barrier_race(
            vec![0_u8, 1_u8, 2_u8],
            Arc::new(|_| async move { Instant::now() }),
        )
        .await
        .expect("barrier race should succeed");

        assert_eq!(starts.len(), 3);
        let earliest = starts.iter().min().expect("earliest start");
        let latest = starts.iter().max().expect("latest start");
        assert!(
            latest.duration_since(*earliest) < Duration::from_millis(30),
            "workers did not start close together: {:?}",
            latest.duration_since(*earliest)
        );
    }

    #[test]
    fn barrier_race_completes_when_runtime_has_fewer_worker_threads_than_tasks() {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("runtime should build");
            let result = runtime.block_on(async {
                run_barrier_race(
                    vec![0_u8, 1_u8],
                    Arc::new(|_| async move { Instant::now() }),
                )
                .await
                .map(|starts| starts.len())
            });
            let _ = tx.send(result);
        });

        let completed = rx.recv_timeout(Duration::from_secs(2)).expect(
            "barrier race should finish even when the runtime has fewer worker threads than tasks",
        );
        let started_workers = completed.expect("barrier race should succeed");
        assert_eq!(started_workers, 2);
    }

    #[test]
    fn update_vs_compaction_uses_localized_selective_predicate() {
        assert_eq!(
            update_vs_compaction_predicate(),
            "region = 'us' AND id % 17 = 0"
        );
    }

    #[test]
    fn delete_vs_compaction_uses_scattered_selective_predicate() {
        assert_eq!(delete_vs_compaction_predicate(), "id % 20 = 0");
    }

    #[test]
    fn aggregate_race_accounting_counts_all_attempts() {
        let sample = aggregate_sample_execution(
            2,
            3,
            vec![
                WorkerOutcome::Success {
                    table_version: Some(1),
                },
                WorkerOutcome::Classified(ContentionErrorKind::DeleteRead),
                WorkerOutcome::Classified(ContentionErrorKind::DeleteDelete),
                WorkerOutcome::Success {
                    table_version: Some(2),
                },
                WorkerOutcome::Classified(ContentionErrorKind::Append),
                WorkerOutcome::Success {
                    table_version: Some(3),
                },
            ],
            TableVersionPolicy::MaxObserved,
        );

        assert!(sample.failure.is_none());
        let metrics = sample
            .metrics
            .contention
            .as_ref()
            .expect("contention metrics should be present");
        assert_eq!(metrics.worker_count, 2);
        assert_eq!(metrics.race_count, 3);
        assert_eq!(metrics.ops_attempted, 6);
        assert_eq!(metrics.ops_succeeded, 3);
        assert_eq!(metrics.ops_failed, 3);
        assert_eq!(metrics.conflict_delete_read, 1);
        assert_eq!(metrics.conflict_delete_delete, 1);
        assert_eq!(metrics.conflict_append, 1);
        assert_eq!(sample.metrics.table_version, Some(3));
    }

    #[test]
    fn aggregate_cloned_races_omit_table_version() {
        let sample = aggregate_sample_execution(
            2,
            3,
            vec![
                WorkerOutcome::Success {
                    table_version: Some(7),
                },
                WorkerOutcome::Classified(ContentionErrorKind::DeleteRead),
                WorkerOutcome::Success {
                    table_version: Some(9),
                },
            ],
            TableVersionPolicy::Omit,
        );

        assert_eq!(sample.metrics.table_version, None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expected_classified_conflicts_do_not_fail_a_case() {
        let case = run_concurrency_case_with_setup(
            "classified_conflict_case",
            0,
            1,
            || async { Ok::<(), crate::error::BenchError>(()) },
            |_| async {
                Ok::<_, crate::error::BenchError>(aggregate_sample_execution(
                    2,
                    1,
                    vec![
                        WorkerOutcome::Success {
                            table_version: Some(1),
                        },
                        WorkerOutcome::Classified(ContentionErrorKind::DeleteRead),
                    ],
                    TableVersionPolicy::MaxObserved,
                ))
            },
        )
        .await;

        assert!(case.success, "classified conflicts should not fail cases");
        let metrics = case.samples[0]
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.contention.as_ref())
            .expect("contention metrics should be present");
        assert_eq!(metrics.ops_attempted, 2);
        assert_eq!(metrics.conflict_delete_read, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unclassified_errors_fail_a_case() {
        let case = run_concurrency_case_with_setup(
            "unexpected_error_case",
            0,
            1,
            || async { Ok::<(), crate::error::BenchError>(()) },
            |_| async {
                Ok::<_, crate::error::BenchError>(aggregate_sample_execution(
                    2,
                    1,
                    vec![
                        WorkerOutcome::Success {
                            table_version: Some(1),
                        },
                        WorkerOutcome::Unexpected("boom".to_string()),
                    ],
                    TableVersionPolicy::MaxObserved,
                ))
            },
        )
        .await;

        assert!(!case.success, "unexpected errors should fail cases");
        assert_eq!(
            case.samples.len(),
            1,
            "failing sample should still be recorded"
        );
        assert!(
            case.failure
                .as_ref()
                .map(|failure| failure.message.contains("boom"))
                .unwrap_or(false),
            "unexpected failure payload: {:?}",
            case.failure
        );
        let metrics = case.samples[0]
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.contention.as_ref())
            .expect("contention metrics should be present");
        assert_eq!(metrics.other_errors, 1);
    }

    #[test]
    fn sample_execution_keeps_contention_metrics_attached() {
        let sample = aggregate_sample_execution(
            1,
            1,
            vec![WorkerOutcome::Success {
                table_version: Some(1),
            }],
            TableVersionPolicy::MaxObserved,
        );

        let SampleMetrics { contention, .. } = sample.metrics;
        assert!(
            contention.is_some(),
            "contention metrics should be attached"
        );
    }
}
