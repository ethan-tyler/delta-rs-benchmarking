use std::cmp::min;
use std::path::Path;
use std::sync::Arc;

use deltalake_core::arrow::array::{Array, BooleanArray, Int32Array, Int64Array};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema};
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::protocol::SaveMode;
use deltalake_core::DeltaTable;
use serde_json::json;
use url::Url;

use super::into_case_result;
use crate::error::{BenchError, BenchResult};
use crate::fingerprint::hash_json;
use crate::results::{CaseResult, RuntimeIOMetrics, SampleMetrics};
use crate::runner::run_case_async_with_async_setup;
use crate::storage::StorageConfig;

const PARTITION_COLUMN_NAME: &str = "part";
const WRITE_PERF_BATCH_ROWS: usize = 131_072;

#[derive(Clone, Copy, Debug)]
struct WritePerfCaseSpec {
    id: &'static str,
    rows: usize,
    partition_count: Option<usize>,
}

const WRITE_PERF_CASES: [WritePerfCaseSpec; 4] = [
    WritePerfCaseSpec {
        id: "write_perf_partitioned_1m_parts_010",
        rows: 1_000_000,
        partition_count: Some(10),
    },
    WritePerfCaseSpec {
        id: "write_perf_partitioned_1m_parts_100",
        rows: 1_000_000,
        partition_count: Some(100),
    },
    WritePerfCaseSpec {
        id: "write_perf_partitioned_5m_parts_010",
        rows: 5_000_000,
        partition_count: Some(10),
    },
    WritePerfCaseSpec {
        id: "write_perf_unpartitioned_1m",
        rows: 1_000_000,
        partition_count: None,
    },
];

pub fn case_names() -> Vec<String> {
    WRITE_PERF_CASES
        .iter()
        .map(|case| case.id.to_string())
        .collect()
}

struct WritePerfIterationSetup {
    _temp: tempfile::TempDir,
    table: DeltaTable,
    batches: Arc<Vec<RecordBatch>>,
    spec: WritePerfCaseSpec,
}

pub async fn run(
    _fixtures_dir: &Path,
    _scale: &str,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    if !storage.is_local() {
        return Ok(super::fixture_error_cases(
            case_names(),
            "write_perf suite does not support non-local storage backend yet",
        ));
    }

    let mut results = Vec::with_capacity(WRITE_PERF_CASES.len());
    for spec in WRITE_PERF_CASES {
        let batches = Arc::new(generate_write_perf_batches(spec)?);
        let case = run_case_async_with_async_setup(
            spec.id,
            warmup,
            iterations,
            || async {
                prepare_write_perf_iteration(spec, Arc::clone(&batches))
                    .await
                    .map_err(|e| e.to_string())
            },
            |setup| async move { run_write_perf_case(setup).await.map_err(|e| e.to_string()) },
        )
        .await;
        results.push(into_case_result(case));
    }

    Ok(results)
}

async fn prepare_write_perf_iteration(
    spec: WritePerfCaseSpec,
    batches: Arc<Vec<RecordBatch>>,
) -> BenchResult<WritePerfIterationSetup> {
    let temp = tempfile::tempdir()?;
    let table_url = Url::from_directory_path(temp.path()).map_err(|()| {
        BenchError::InvalidArgument(format!(
            "failed to create URL for {}",
            temp.path().display()
        ))
    })?;
    let table = DeltaTable::try_from_url(table_url).await?;
    Ok(WritePerfIterationSetup {
        _temp: temp,
        table,
        batches,
        spec,
    })
}

async fn run_write_perf_case(setup: WritePerfIterationSetup) -> BenchResult<SampleMetrics> {
    let mut builder = setup
        .table
        .write(setup.batches.as_ref().clone())
        .with_save_mode(SaveMode::Overwrite);
    if setup.spec.partition_count.is_some() {
        builder = builder.with_partition_columns([PARTITION_COLUMN_NAME]);
    }
    let table = builder.await?;

    let table_version = table.version().map(|version| version as u64);
    let result_hash = hash_json(&json!({
        "rows_processed": setup.spec.rows as u64,
        "operations": 1_u64,
        "table_version": table_version,
        "partitioned": setup.spec.partition_count.is_some(),
        "partition_count": setup.spec.partition_count.unwrap_or_default() as u64,
        "input_batches": setup.batches.len() as u64,
    }))?;
    let schema_hash = hash_json(&json!([
        "rows_processed:u64",
        "operations:u64",
        "table_version:u64",
        "partitioned:bool",
        "partition_count:u64",
        "input_batches:u64",
    ]))?;

    Ok(
        SampleMetrics::base(Some(setup.spec.rows as u64), None, Some(1), table_version)
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
                semantic_state_digest: None,
                validation_summary: None,
            }),
    )
}

fn generate_write_perf_batches(spec: WritePerfCaseSpec) -> BenchResult<Vec<RecordBatch>> {
    let schema = write_perf_schema(spec.partition_count.is_some());
    let mut batches = Vec::new();
    let mut offset = 0usize;

    while offset < spec.rows {
        let rows = min(WRITE_PERF_BATCH_ROWS, spec.rows - offset);
        batches.push(generate_write_perf_batch(
            Arc::clone(&schema),
            offset,
            rows,
            spec.partition_count,
        )?);
        offset += rows;
    }

    Ok(batches)
}

fn write_perf_schema(partitioned: bool) -> Arc<Schema> {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value_i64", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
    ];
    if partitioned {
        fields.push(Field::new(PARTITION_COLUMN_NAME, DataType::Int32, false));
    }
    Arc::new(Schema::new(fields))
}

fn generate_write_perf_batch(
    schema: Arc<Schema>,
    row_offset: usize,
    rows: usize,
    partition_count: Option<usize>,
) -> BenchResult<RecordBatch> {
    let mut ids = Vec::with_capacity(rows);
    let mut values = Vec::with_capacity(rows);
    let mut flags = Vec::with_capacity(rows);
    let mut partitions = partition_count.map(|_| Vec::with_capacity(rows));

    for idx in 0..rows {
        let absolute = row_offset + idx;
        ids.push(absolute as i64);
        values.push(((absolute as i64 * 13) % 100_000) - 50_000);
        flags.push(absolute % 2 == 0);
        if let Some(partitions) = partitions.as_mut() {
            let partition_total = partition_count.expect("partition count");
            partitions.push((absolute % partition_total) as i32);
        }
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(Int64Array::from(values)),
        Arc::new(BooleanArray::from(flags)),
    ];
    if let Some(partitions) = partitions {
        columns.push(Arc::new(Int32Array::from(partitions)));
    }

    RecordBatch::try_new(schema, columns).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::{generate_write_perf_batches, WritePerfCaseSpec, WRITE_PERF_BATCH_ROWS};

    #[test]
    fn partitioned_write_perf_batches_include_partition_column() {
        let spec = WritePerfCaseSpec {
            id: "test",
            rows: 10,
            partition_count: Some(3),
        };
        let batches = generate_write_perf_batches(spec).expect("generate batches");
        let schema = batches[0].schema();
        assert!(schema.field_with_name("part").is_ok());
    }

    #[test]
    fn unpartitioned_write_perf_batches_omit_partition_column() {
        let spec = WritePerfCaseSpec {
            id: "test",
            rows: 10,
            partition_count: None,
        };
        let batches = generate_write_perf_batches(spec).expect("generate batches");
        let schema = batches[0].schema();
        assert!(schema.field_with_name("part").is_err());
    }

    #[test]
    fn large_write_perf_cases_are_chunked_into_multiple_batches() {
        let spec = WritePerfCaseSpec {
            id: "test",
            rows: WRITE_PERF_BATCH_ROWS + 1,
            partition_count: Some(10),
        };
        let batches = generate_write_perf_batches(spec).expect("generate batches");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), WRITE_PERF_BATCH_ROWS);
        assert_eq!(batches[1].num_rows(), 1);
    }
}
