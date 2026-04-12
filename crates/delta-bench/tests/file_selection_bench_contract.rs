use std::collections::HashSet;
use std::fs;
use std::path::Path;

use delta_bench::data::fixtures::generate_file_selection_fixtures;
use delta_bench::file_selection_bench_support::{
    self, FileSelectionCaseContext, FileSelectionVariant,
};
use delta_bench::storage::StorageConfig;
use deltalake_core::arrow::array::{Array, Int64Array};
use deltalake_core::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

fn expected_partition_urls(ctx: &FileSelectionCaseContext) -> HashSet<String> {
    ctx.snapshot()
        .log_data()
        .iter()
        .filter_map(|file| {
            let path = file.path();
            path.contains("region=us/")
                .then(|| {
                    ctx.table_root_url()
                        .join(path.as_ref())
                        .expect("join file URL")
                })
                .map(normalize_expected_url)
        })
        .collect()
}

fn file_contains_divisible_id(file_url: &str, divisor: i64) -> bool {
    let file_path = Url::parse(file_url)
        .expect("parse file URL")
        .to_file_path()
        .expect("convert file URL to local path");
    let reader = std::fs::File::open(&file_path).expect("open parquet file");
    let mut batches = ParquetRecordBatchReaderBuilder::try_new(reader)
        .expect("build parquet reader")
        .build()
        .expect("create parquet batch reader");

    batches.any(|batch_result| {
        let batch = batch_result.expect("read parquet batch");
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column should be Int64");
        (0..ids.len()).any(|idx| !ids.is_null(idx) && ids.value(idx) % divisor == 0)
    })
}

fn expected_data_urls(ctx: &FileSelectionCaseContext, divisor: i64) -> HashSet<String> {
    ctx.snapshot()
        .log_data()
        .iter()
        .map(|file| {
            ctx.table_root_url()
                .join(file.path().as_ref())
                .expect("join file URL")
        })
        .filter(|url| file_contains_divisible_id(url.as_str(), divisor))
        .map(normalize_expected_url)
        .collect()
}

fn normalize_expected_url(url: Url) -> String {
    if url.scheme() != "file" {
        return url.to_string();
    }

    let path = url.to_file_path().expect("convert file URL to local path");
    let canonical_path = fs::canonicalize(path).expect("canonicalize file path");
    Url::from_file_path(canonical_path)
        .expect("canonical file path should convert to file URL")
        .to_string()
}

async fn build_context(
    variant: FileSelectionVariant,
) -> (tempfile::TempDir, FileSelectionCaseContext) {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_file_selection_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("fixtures should be generated");

    let spec =
        file_selection_bench_support::benchmark_case_spec(temp.path(), "sf1", variant, &storage)
            .expect("file-selection case spec");
    let ctx = file_selection_bench_support::benchmark_case_context(&storage, spec)
        .await
        .expect("file-selection case context");
    (temp, ctx)
}

fn harness_root() -> std::path::PathBuf {
    let manifest_root = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonicalize crate root");
    if manifest_root.join("bench/methodologies").exists() {
        return manifest_root;
    }

    let parent = manifest_root
        .parent()
        .expect("synced delta-rs checkout should have a parent directory");
    if parent.join("bench/methodologies").exists() {
        return parent.to_path_buf();
    }

    panic!(
        "unable to locate harness root from {}",
        manifest_root.display()
    );
}

#[tokio::test]
async fn file_selection_bench_contract_partition_only_stays_on_partition_path() {
    let (_temp, ctx) = build_context(FileSelectionVariant::PartitionOnly).await;

    assert!(
        file_selection_bench_support::benchmark_has_partition_mem_table(&ctx)
            .expect("partition mem table helper"),
        "partition-only case should expose the partition mem table"
    );

    let result = file_selection_bench_support::benchmark_find_files(&ctx)
        .await
        .expect("find_files result");
    let expected_urls = expected_partition_urls(&ctx);

    assert!(
        result.partition_scan,
        "partition-only case should stay on the partition path"
    );
    assert_eq!(
        result.candidate_urls, expected_urls,
        "partition-only candidates should match the active us-partition files"
    );
    assert_eq!(
        result.candidate_count,
        expected_urls.len(),
        "partition-only candidate count should match the underlying fixture file set"
    );
    assert!(
        result.candidate_count < ctx.total_active_files(),
        "partition-only pruning should reduce the active file set"
    );
}

#[tokio::test]
async fn file_selection_bench_contract_data_predicate_stays_on_file_scan_path_for_delete() {
    let (_temp, ctx) = build_context(FileSelectionVariant::DeleteDataPredicate).await;

    let find_files = file_selection_bench_support::benchmark_find_files(&ctx)
        .await
        .expect("find_files result");
    let find_files_scan = file_selection_bench_support::benchmark_find_files_scan(&ctx)
        .await
        .expect("find_files_scan result");
    let matched_scan = file_selection_bench_support::benchmark_scan_files_where_matches(&ctx)
        .await
        .expect("scan_files_where_matches result");
    let expected_urls = expected_data_urls(&ctx, 20);

    assert!(
        !find_files.partition_scan,
        "data predicate should bypass the partition-only path"
    );
    assert_eq!(find_files.candidate_urls, expected_urls);
    assert_eq!(find_files.candidate_count, expected_urls.len());
    assert_eq!(find_files_scan.candidate_urls, expected_urls);
    assert_eq!(find_files_scan.candidate_count, expected_urls.len());
    assert_eq!(matched_scan.candidate_urls, expected_urls);
    assert_eq!(matched_scan.candidate_count, expected_urls.len());
    assert!(
        matched_scan.predicate.contains("id"),
        "matched scan should retain the data predicate"
    );
}

#[tokio::test]
async fn file_selection_bench_contract_data_predicate_stays_on_file_scan_path_for_update() {
    let (_temp, ctx) = build_context(FileSelectionVariant::UpdateDataPredicate).await;

    let find_files = file_selection_bench_support::benchmark_find_files(&ctx)
        .await
        .expect("find_files result");
    let matched_scan = file_selection_bench_support::benchmark_scan_files_where_matches(&ctx)
        .await
        .expect("scan_files_where_matches result");
    let expected_urls = expected_data_urls(&ctx, 2);

    assert!(
        !find_files.partition_scan,
        "update data predicate should stay on the file-scan path"
    );
    assert_eq!(find_files.candidate_urls, expected_urls);
    assert_eq!(find_files.candidate_count, expected_urls.len());
    assert_eq!(matched_scan.candidate_urls, expected_urls);
    assert_eq!(matched_scan.candidate_count, expected_urls.len());
}

#[test]
fn file_selection_bench_contract_profile_is_criterion_only() {
    let root = harness_root();
    let profile = root.join("bench/methodologies/file-selection-criterion.env");

    let profile_text = fs::read_to_string(&profile).expect("read file-selection criterion env");
    assert!(
        profile_text.contains("PROFILE_KIND=criterion"),
        "file-selection profile must stay criterion-only"
    );
    assert!(
        profile_text.contains("CRITERION_BENCH=file_selection_bench"),
        "file-selection profile must target the dedicated criterion bench"
    );
}
