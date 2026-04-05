use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::kernel::Snapshot;
use deltalake_core::logstore::LogStoreRef;
use deltalake_core::DeltaTableBuilder;
use deltalake_core::DeltaTableConfig;
use futures::TryStreamExt;
use url::Url;

use crate::data::fixtures::{metadata_long_history_table_path, metadata_long_history_table_url};
use crate::error::{BenchError, BenchResult};
use crate::storage::StorageConfig;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataLogActionProfile {
    SimpleActions,
    WithStats,
    FullComplexity,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct MetadataLogCaseSpec {
    table_url: Url,
    table_log_dir: PathBuf,
}

impl MetadataLogCaseSpec {
    pub fn table_log_dir(&self) -> &Path {
        &self.table_log_dir
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct MetadataLogCaseContext {
    log_store: LogStoreRef,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct FilelessSnapshotInput {
    snapshot_without_files: Snapshot,
}

#[doc(hidden)]
pub struct MaterializedFiles {
    pub version: i64,
    pub files: Vec<RecordBatch>,
}

#[doc(hidden)]
pub fn benchmark_commit_log_bytes(profile: MetadataLogActionProfile, num_actions: usize) -> Bytes {
    let mut log_lines = Vec::with_capacity(num_actions + 2);
    log_lines.push(r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#.to_string());
    log_lines.push(r#"{"commitInfo":{"timestamp":1234567890}}"#.to_string());

    for i in 0..num_actions {
        let mut add_json = format!(
            r#"{{"path":"part-{:05}.parquet","size":{},"modificationTime":1234567890,"dataChange":true"#,
            i,
            1000 + i * 100
        );

        match profile {
            MetadataLogActionProfile::SimpleActions | MetadataLogActionProfile::WithStats => {
                add_json.push_str(r#","partitionValues":{}"#);
            }
            MetadataLogActionProfile::FullComplexity => {
                add_json.push_str(r#","partitionValues":{"year":"2024","month":"10","day":"09"}"#);
            }
        }

        if matches!(
            profile,
            MetadataLogActionProfile::WithStats | MetadataLogActionProfile::FullComplexity
        ) {
            add_json.push_str(&format!(
                r#","stats":"{{\"numRecords\":{},\"minValues\":{{\"id\":{},\"name\":\"aaa\",\"value\":{}.5}},\"maxValues\":{{\"id\":{},\"name\":\"zzz\",\"value\":{}.99}},\"nullCount\":{{\"id\":0,\"name\":0,\"value\":{}}}}}""#,
                1000 + i * 10,
                i,
                i,
                i + 1000,
                i + 1000,
                i % 10
            ));
        }

        if profile == MetadataLogActionProfile::FullComplexity {
            add_json.push_str(
                r#","deletionVector":{"storageType":"u","pathOrInlineDv":"vBn[lx{q8@P<9BNH/isA","offset":1,"sizeInBytes":36,"cardinality":2}"#,
            );
        }

        add_json.push('}');
        log_lines.push(format!(r#"{{"add":{add_json}}}"#));
    }

    Bytes::from(log_lines.join("\n"))
}

#[doc(hidden)]
pub fn benchmark_case_spec(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<MetadataLogCaseSpec> {
    Ok(MetadataLogCaseSpec {
        table_url: metadata_long_history_table_url(fixtures_dir, scale, storage)?,
        table_log_dir: metadata_long_history_table_path(fixtures_dir, scale).join("_delta_log"),
    })
}

#[doc(hidden)]
pub fn benchmark_history_file_count(spec: &MetadataLogCaseSpec) -> usize {
    fs::read_dir(spec.table_log_dir())
        .expect("metadata log history dir")
        .count()
}

#[doc(hidden)]
pub async fn benchmark_case_context(
    storage: &StorageConfig,
    spec: MetadataLogCaseSpec,
) -> BenchResult<MetadataLogCaseContext> {
    let mut builder = DeltaTableBuilder::from_url(spec.table_url)?;
    let options = storage.object_store_options();
    if !options.is_empty() {
        builder = builder.with_storage_options(options);
    }
    let table = builder.build()?;
    Ok(MetadataLogCaseContext {
        log_store: table.log_store(),
    })
}

#[doc(hidden)]
pub async fn benchmark_snapshot_try_new(
    ctx: &MetadataLogCaseContext,
    version: Option<u64>,
) -> BenchResult<Snapshot> {
    let version = version.map(version_to_i64).transpose()?;
    Ok(Snapshot::try_new(ctx.log_store.as_ref(), Default::default(), version).await?)
}

#[doc(hidden)]
pub async fn benchmark_fileless_snapshot_input(
    ctx: &MetadataLogCaseContext,
    version: Option<u64>,
) -> BenchResult<FilelessSnapshotInput> {
    let version = version.map(version_to_i64).transpose()?;
    let snapshot_without_files = Snapshot::try_new(
        ctx.log_store.as_ref(),
        DeltaTableConfig {
            require_files: false,
            ..Default::default()
        },
        version,
    )
    .await?;
    Ok(FilelessSnapshotInput {
        snapshot_without_files,
    })
}

#[doc(hidden)]
pub async fn benchmark_materialize_files(
    ctx: &MetadataLogCaseContext,
) -> BenchResult<MaterializedFiles> {
    let input = benchmark_fileless_snapshot_input(ctx, None).await?;
    benchmark_materialize_files_from_input(ctx, input).await
}

#[doc(hidden)]
pub async fn benchmark_materialize_files_from_input(
    ctx: &MetadataLogCaseContext,
    input: FilelessSnapshotInput,
) -> BenchResult<MaterializedFiles> {
    // `EagerSnapshot::with_files` is crate-private upstream. This helper times
    // only the file-materialization work that dominates that path and avoids
    // layering on local reconstruction steps that production code does not run.
    let files = input
        .snapshot_without_files
        .files(ctx.log_store.as_ref(), None)
        .try_collect::<Vec<RecordBatch>>()
        .await?;
    Ok(MaterializedFiles {
        version: input.snapshot_without_files.version(),
        files,
    })
}

fn version_to_i64(version: u64) -> BenchResult<i64> {
    i64::try_from(version).map_err(|_| {
        BenchError::InvalidArgument(format!("snapshot version {version} does not fit into i64"))
    })
}
