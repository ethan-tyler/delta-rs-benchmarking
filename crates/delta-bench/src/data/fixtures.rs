use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use deltalake_core::arrow;
use deltalake_core::protocol::SaveMode;
use url::Url;

use super::datasets::{FixtureManifest, NarrowSaleRow};
use super::generator::generate_narrow_sales_rows;
use crate::error::{BenchError, BenchResult};
use crate::storage::StorageConfig;

const NARROW_SALES_TABLE_DIR: &str = "narrow_sales_delta";
const MERGE_TARGET_TABLE_DIR: &str = "merge_target_delta";
const READ_PARTITIONED_TABLE_DIR: &str = "read_partitioned_delta";
const MERGE_PARTITIONED_TARGET_TABLE_DIR: &str = "merge_partitioned_target_delta";
const OPTIMIZE_SMALL_FILES_TABLE_DIR: &str = "optimize_small_files_delta";
const OPTIMIZE_COMPACTED_TABLE_DIR: &str = "optimize_compacted_delta";
const VACUUM_READY_TABLE_DIR: &str = "vacuum_ready_delta";

pub fn scale_to_row_count(scale: &str) -> BenchResult<usize> {
    match scale {
        "sf1" => Ok(10_000),
        "sf10" => Ok(100_000),
        "sf100" => Ok(1_000_000),
        _ => Err(BenchError::InvalidArgument(format!(
            "unknown scale '{scale}' (expected one of: sf1, sf10, sf100)"
        ))),
    }
}

pub fn validate_scale(scale: &str) -> BenchResult<()> {
    if scale.is_empty() {
        return Err(BenchError::InvalidArgument(
            "scale must not be empty".to_string(),
        ));
    }
    if matches!(scale, "." | "..") {
        return Err(BenchError::InvalidArgument(format!(
            "scale '{scale}' is not allowed"
        )));
    }
    if !scale
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        return Err(BenchError::InvalidArgument(
            "scale contains invalid characters; allowed: [A-Za-z0-9._-]".to_string(),
        ));
    }
    let _ = scale_to_row_count(scale)?;
    Ok(())
}

pub fn fixture_root(fixtures_dir: &Path, scale: &str) -> BenchResult<PathBuf> {
    validate_scale(scale)?;
    Ok(fixtures_dir.join(scale))
}

pub fn narrow_sales_table_path(fixtures_dir: &Path, scale: &str) -> BenchResult<PathBuf> {
    Ok(fixture_root(fixtures_dir, scale)?.join(NARROW_SALES_TABLE_DIR))
}

pub fn read_partitioned_table_path(fixtures_dir: &Path, scale: &str) -> PathBuf {
    fixture_root(fixtures_dir, scale).join(READ_PARTITIONED_TABLE_DIR)
}

pub fn merge_partitioned_target_table_path(fixtures_dir: &Path, scale: &str) -> PathBuf {
    fixture_root(fixtures_dir, scale).join(MERGE_PARTITIONED_TARGET_TABLE_DIR)
}

pub fn optimize_small_files_table_path(fixtures_dir: &Path, scale: &str) -> PathBuf {
    fixture_root(fixtures_dir, scale).join(OPTIMIZE_SMALL_FILES_TABLE_DIR)
}

pub fn optimize_compacted_table_path(fixtures_dir: &Path, scale: &str) -> PathBuf {
    fixture_root(fixtures_dir, scale).join(OPTIMIZE_COMPACTED_TABLE_DIR)
}

pub fn vacuum_ready_table_path(fixtures_dir: &Path, scale: &str) -> PathBuf {
    fixture_root(fixtures_dir, scale).join(VACUUM_READY_TABLE_DIR)
}

pub fn narrow_sales_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &narrow_sales_table_path(fixtures_dir, scale)?,
        scale,
        NARROW_SALES_TABLE_DIR,
    )
}

pub fn merge_target_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &merge_target_table_path(fixtures_dir, scale)?,
        scale,
        MERGE_TARGET_TABLE_DIR,
    )
}

pub fn read_partitioned_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &read_partitioned_table_path(fixtures_dir, scale),
        scale,
        READ_PARTITIONED_TABLE_DIR,
    )
}

pub fn merge_partitioned_target_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &merge_partitioned_target_table_path(fixtures_dir, scale),
        scale,
        MERGE_PARTITIONED_TARGET_TABLE_DIR,
    )
}

pub fn optimize_small_files_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &optimize_small_files_table_path(fixtures_dir, scale)?,
        scale,
        OPTIMIZE_SMALL_FILES_TABLE_DIR,
    )
}

pub fn optimize_compacted_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &optimize_compacted_table_path(fixtures_dir, scale),
        scale,
        OPTIMIZE_COMPACTED_TABLE_DIR,
    )
}

pub fn vacuum_ready_table_url(
    fixtures_dir: &Path,
    scale: &str,
    storage: &StorageConfig,
) -> BenchResult<Url> {
    storage.table_url_for(
        &vacuum_ready_table_path(fixtures_dir, scale)?,
        scale,
        VACUUM_READY_TABLE_DIR,
    )
}

pub async fn generate_fixtures(
    fixtures_dir: &Path,
    scale: &str,
    seed: u64,
    force: bool,
    storage: &StorageConfig,
) -> BenchResult<()> {
    let root = fixture_root(fixtures_dir, scale)?;
    let dataset_dir = root.join("narrow_sales");
    let data_path = dataset_dir.join("rows.jsonl");
    let manifest_path = root.join("manifest.json");
    let rows = scale_to_row_count(scale)?;

    if root.exists() && !force {
        if let Ok(existing) = load_manifest(fixtures_dir, scale) {
            let matches_request = existing.schema_version == 1
                && existing.seed == seed
                && existing.scale == scale
                && existing.rows == rows;
            if matches_request {
                return Ok(());
            }
        }
    }
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    fs::create_dir_all(&dataset_dir)?;

    let data = generate_narrow_sales_rows(seed, rows);
    write_rows_jsonl(&data_path, &data)?;

    write_delta_table(
        narrow_sales_table_url(fixtures_dir, scale, storage)?,
        &data,
        storage,
    )
    .await?;

    write_delta_table_partitioned_small_files(
        read_partitioned_table_url(fixtures_dir, scale, storage)?,
        &data,
        128,
        &["region"],
        storage,
    )
    .await?;

    let merge_rows = data
        .iter()
        .take((data.len() / 4).max(1024))
        .cloned()
        .collect::<Vec<_>>();
    write_delta_table(
        merge_target_table_url(fixtures_dir, scale, storage)?,
        &merge_rows,
        storage,
    )
    .await?;

    write_delta_table_partitioned_small_files(
        merge_partitioned_target_table_url(fixtures_dir, scale, storage)?,
        &merge_rows,
        64,
        &["region"],
        storage,
    )
    .await?;

    let optimize_rows = data
        .iter()
        .take((data.len() / 2).max(2048))
        .cloned()
        .collect::<Vec<_>>();
    write_delta_table_small_files(
        optimize_small_files_table_url(fixtures_dir, scale, storage)?,
        &optimize_rows,
        128,
        storage,
    )
    .await?;

    write_delta_table(
        optimize_compacted_table_url(fixtures_dir, scale, storage)?,
        &optimize_rows,
        storage,
    )
    .await?;

    let vacuum_rows = data
        .iter()
        .take((data.len() / 3).max(1024))
        .cloned()
        .collect::<Vec<_>>();
    write_vacuum_ready_table(
        vacuum_ready_table_url(fixtures_dir, scale, storage)?,
        &vacuum_rows,
        storage,
    )
    .await?;

    let manifest = FixtureManifest {
        schema_version: 1,
        seed,
        scale: scale.to_string(),
        rows,
    };
    fs::write(manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    Ok(())
}

fn write_rows_jsonl(path: &Path, rows: &[NarrowSaleRow]) -> BenchResult<()> {
    let mut file = fs::File::create(path)?;
    for row in rows {
        let line = serde_json::to_string(row)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
    }
    Ok(())
}

pub(crate) async fn write_delta_table(
    table_url: Url,
    rows: &[NarrowSaleRow],
    storage: &StorageConfig,
) -> BenchResult<()> {
    prepare_local_table_dir(&table_url)?;

    let batch = rows_to_batch(rows)?;
    let _ = storage
        .try_from_url_for_write(table_url)
        .await?
        .write(vec![batch])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    Ok(())
}

pub(crate) async fn write_delta_table_small_files(
    table_url: Url,
    rows: &[NarrowSaleRow],
    chunk_size: usize,
    storage: &StorageConfig,
) -> BenchResult<()> {
    prepare_local_table_dir(&table_url)?;

    let mut table = storage.try_from_url_for_write(table_url).await?;
    for (idx, chunk) in rows.chunks(chunk_size).enumerate() {
        let mode = if idx == 0 {
            SaveMode::Overwrite
        } else {
            SaveMode::Append
        };
        table = table
            .write(vec![rows_to_batch(chunk)?])
            .with_save_mode(mode)
            .await?;
    }

    Ok(())
}

pub(crate) async fn write_delta_table_partitioned_small_files(
    table_url: Url,
    rows: &[NarrowSaleRow],
    chunk_size: usize,
    partition_columns: &[&str],
    storage: &StorageConfig,
) -> BenchResult<()> {
    prepare_local_table_dir(&table_url)?;

    let mut table = storage.try_from_url_for_write(table_url).await?;
    for (idx, chunk) in rows.chunks(chunk_size).enumerate() {
        let mode = if idx == 0 {
            SaveMode::Overwrite
        } else {
            SaveMode::Append
        };
        table = table
            .write(vec![rows_to_batch(chunk)?])
            .with_save_mode(mode)
            .with_partition_columns(partition_columns.iter().copied())
            .await?;
    }

    Ok(())
}

pub(crate) async fn write_vacuum_ready_table(
    table_url: Url,
    rows: &[NarrowSaleRow],
    storage: &StorageConfig,
) -> BenchResult<()> {
    write_delta_table(table_url.clone(), rows, storage).await?;

    let retained = (rows.len() / 3).max(1);
    let _ = storage
        .try_from_url_for_write(table_url)
        .await?
        .write(vec![rows_to_batch(&rows[..retained])?])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    Ok(())
}

fn prepare_local_table_dir(table_url: &Url) -> BenchResult<()> {
    if table_url.scheme() != "file" {
        return Ok(());
    }

    let table_dir = table_url.to_file_path().map_err(|()| {
        BenchError::InvalidArgument(format!("failed to convert URL to file path: {table_url}"))
    })?;

    if table_dir.exists() {
        fs::remove_dir_all(&table_dir)?;
    }
    fs::create_dir_all(&table_dir)?;
    Ok(())
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

pub fn load_rows(fixtures_dir: &Path, scale: &str) -> BenchResult<Vec<NarrowSaleRow>> {
    let data_path = fixture_root(fixtures_dir, scale)?
        .join("narrow_sales")
        .join("rows.jsonl");

    let data = fs::read_to_string(data_path)?;
    let mut rows = Vec::new();
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let row: NarrowSaleRow = serde_json::from_str(line)?;
        rows.push(row);
    }
    Ok(rows)
}

pub fn load_manifest(fixtures_dir: &Path, scale: &str) -> BenchResult<FixtureManifest> {
    let path = fixture_root(fixtures_dir, scale)?.join("manifest.json");
    let manifest: FixtureManifest = serde_json::from_slice(&fs::read(path)?)?;
    Ok(manifest)
}
