use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use deltalake_core::datafusion::catalog::Session;
use deltalake_core::datafusion::logical_expr::Expr;
use deltalake_core::delta_datafusion::{bench_support, create_session, DataFusionMixins as _};
use deltalake_core::kernel::{Add, EagerSnapshot};
use deltalake_core::logstore::object_store::path::Path as ObjectStorePath;
use deltalake_core::logstore::{LogStore, LogStoreRef};
use url::Url;

use crate::data::fixtures::{delete_update_small_files_table_url, read_partitioned_table_url};
use crate::error::BenchResult;
use crate::storage::StorageConfig;

const PARTITION_ONLY_PREDICATE: &str = "region = 'us'";
const DELETE_DATA_PREDICATE: &str = "id % 20 = 0";
const UPDATE_DATA_PREDICATE: &str = "id % 2 = 0";

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileSelectionVariant {
    PartitionOnly,
    DeleteDataPredicate,
    UpdateDataPredicate,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct FileSelectionCaseSpec {
    table_url: Url,
    predicate_sql: &'static str,
}

#[doc(hidden)]
pub struct FileSelectionCaseContext {
    snapshot: EagerSnapshot,
    log_store: LogStoreRef,
    table_root_url: Url,
    session: Arc<dyn Session + Send + Sync>,
    predicate: Expr,
    total_active_files: usize,
}

#[doc(hidden)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileCandidateSet {
    pub candidate_urls: HashSet<String>,
    pub candidate_count: usize,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FindFilesBenchOutcome {
    pub candidate_count: usize,
    pub partition_scan: bool,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileSelectionBenchCount {
    pub candidate_count: usize,
}

#[doc(hidden)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FindFilesOutcome {
    pub candidate_urls: HashSet<String>,
    pub candidate_count: usize,
    pub partition_scan: bool,
}

#[doc(hidden)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MatchedFilesScanOutcome {
    pub candidate_urls: HashSet<String>,
    pub candidate_count: usize,
    pub predicate: String,
}

impl FileSelectionCaseContext {
    pub fn snapshot(&self) -> &EagerSnapshot {
        &self.snapshot
    }

    pub fn table_root_url(&self) -> &Url {
        &self.table_root_url
    }

    pub fn total_active_files(&self) -> usize {
        self.total_active_files
    }
}

#[doc(hidden)]
pub fn benchmark_case_spec(
    fixtures_dir: &Path,
    scale: &str,
    variant: FileSelectionVariant,
    storage: &StorageConfig,
) -> BenchResult<FileSelectionCaseSpec> {
    let table_url = match variant {
        FileSelectionVariant::PartitionOnly | FileSelectionVariant::UpdateDataPredicate => {
            read_partitioned_table_url(fixtures_dir, scale, storage)?
        }
        FileSelectionVariant::DeleteDataPredicate => {
            delete_update_small_files_table_url(fixtures_dir, scale, storage)?
        }
    };
    let predicate_sql = match variant {
        FileSelectionVariant::PartitionOnly => PARTITION_ONLY_PREDICATE,
        FileSelectionVariant::DeleteDataPredicate => DELETE_DATA_PREDICATE,
        FileSelectionVariant::UpdateDataPredicate => UPDATE_DATA_PREDICATE,
    };

    Ok(FileSelectionCaseSpec {
        table_url,
        predicate_sql,
    })
}

#[doc(hidden)]
pub async fn benchmark_case_context(
    storage: &StorageConfig,
    spec: FileSelectionCaseSpec,
) -> BenchResult<FileSelectionCaseContext> {
    let table = storage.open_table(spec.table_url).await?;
    let session: Arc<dyn Session + Send + Sync> = Arc::new(create_session().state());
    table.update_datafusion_session(session.as_ref())?;
    ensure_legacy_log_store_registered(session.as_ref(), table.log_store().as_ref());

    let snapshot = table.snapshot()?.snapshot().clone();
    let predicate = snapshot.parse_predicate_expression(spec.predicate_sql, session.as_ref())?;
    let mut table_root_url = table.log_store().root_url().clone();
    if !table_root_url.path().ends_with('/') {
        table_root_url.set_path(&format!("{}/", table_root_url.path()));
    }
    let total_active_files = snapshot.log_data().num_files();

    Ok(FileSelectionCaseContext {
        snapshot,
        log_store: table.log_store(),
        table_root_url,
        session,
        predicate,
        total_active_files,
    })
}

#[doc(hidden)]
pub fn benchmark_has_partition_mem_table(ctx: &FileSelectionCaseContext) -> BenchResult<bool> {
    Ok(bench_support::add_actions_partition_mem_table(&ctx.snapshot)?.is_some())
}

#[doc(hidden)]
pub async fn timed_find_files(
    ctx: &FileSelectionCaseContext,
) -> BenchResult<FindFilesBenchOutcome> {
    let result = find_files_result(ctx).await?;
    Ok(FindFilesBenchOutcome {
        candidate_count: result.candidates.len(),
        partition_scan: result.partition_scan,
    })
}

#[doc(hidden)]
pub async fn benchmark_find_files(ctx: &FileSelectionCaseContext) -> BenchResult<FindFilesOutcome> {
    let result = find_files_result(ctx).await?;

    Ok(FindFilesOutcome {
        candidate_urls: normalize_add_urls(&ctx.table_root_url, &result.candidates),
        candidate_count: result.candidates.len(),
        partition_scan: result.partition_scan,
    })
}

async fn find_files_result(
    ctx: &FileSelectionCaseContext,
) -> BenchResult<bench_support::FindFilesResult> {
    Ok(bench_support::find_files(
        &ctx.snapshot,
        ctx.log_store.clone(),
        ctx.session.as_ref(),
        Some(ctx.predicate.clone()),
    )
    .await?)
}

#[doc(hidden)]
pub async fn timed_find_files_scan(
    ctx: &FileSelectionCaseContext,
) -> BenchResult<FileSelectionBenchCount> {
    let candidates = find_files_scan_candidates(ctx).await?;
    Ok(FileSelectionBenchCount {
        candidate_count: candidates.len(),
    })
}

#[doc(hidden)]
pub async fn benchmark_find_files_scan(
    ctx: &FileSelectionCaseContext,
) -> BenchResult<FileCandidateSet> {
    let candidates = find_files_scan_candidates(ctx).await?;

    Ok(FileCandidateSet {
        candidate_urls: normalize_add_urls(&ctx.table_root_url, &candidates),
        candidate_count: candidates.len(),
    })
}

async fn find_files_scan_candidates(ctx: &FileSelectionCaseContext) -> BenchResult<Vec<Add>> {
    Ok(bench_support::find_files_scan(
        &ctx.snapshot,
        ctx.log_store.clone(),
        ctx.session.as_ref(),
        ctx.predicate.clone(),
    )
    .await?)
}

#[doc(hidden)]
pub async fn timed_scan_files_where_matches(
    ctx: &FileSelectionCaseContext,
) -> BenchResult<FileSelectionBenchCount> {
    let candidates = find_files_scan_candidates(ctx).await?;
    Ok(FileSelectionBenchCount {
        candidate_count: candidates.len(),
    })
}

#[doc(hidden)]
pub async fn benchmark_scan_files_where_matches(
    ctx: &FileSelectionCaseContext,
) -> BenchResult<MatchedFilesScanOutcome> {
    let candidates = find_files_scan_candidates(ctx).await?;
    Ok(MatchedFilesScanOutcome {
        candidate_count: candidates.len(),
        candidate_urls: normalize_add_urls(&ctx.table_root_url, &candidates),
        predicate: ctx.predicate.to_string(),
    })
}

fn normalize_add_urls(table_root_url: &Url, adds: &[Add]) -> HashSet<String> {
    adds.iter()
        .map(|add| {
            let candidate = table_root_url
                .join(add.path.as_str())
                .expect("add action path should join against table root");
            if candidate.scheme() == "file" {
                normalize_resolved_url(candidate)
            } else {
                // Fall back to the object-store path normalization when URLs are not file-based.
                normalize_resolved_url(
                    table_root_url
                        .join(ObjectStorePath::from(add.path.as_str()).as_ref())
                        .expect("normalized add action path should join against table root"),
                )
            }
        })
        .collect()
}

fn normalize_resolved_url(url: Url) -> String {
    if url.scheme() != "file" {
        return url.to_string();
    }

    match url.to_file_path() {
        Ok(path) => match fs::canonicalize(path) {
            Ok(path) => Url::from_file_path(path)
                .expect("canonicalized file path should convert to URL")
                .to_string(),
            Err(_) => url.to_string(),
        },
        Err(_) => url.to_string(),
    }
}

fn ensure_legacy_log_store_registered(session: &dyn Session, log_store: &dyn LogStore) {
    let object_store_url = log_store.object_store_url();
    if session
        .runtime_env()
        .object_store(&object_store_url)
        .is_err()
    {
        session
            .runtime_env()
            .register_object_store(object_store_url.as_ref(), log_store.object_store(None));
    }
}
