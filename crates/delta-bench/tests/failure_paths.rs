#[path = "support/env_lock.rs"]
mod env_lock_support;
#[path = "support/env_vars.rs"]
mod env_vars_support;

use delta_bench::cli::BenchmarkLane;
use delta_bench::data::fixtures::{
    generate_fixtures, generate_fixtures_with_profile, FixtureProfile,
};
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{concurrency, delete_update, interop_py, merge, optimize_vacuum, write};

use env_lock_support::env_lock;
use env_vars_support::with_env_vars;

#[tokio::test]
async fn write_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = write::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}

#[tokio::test]
async fn write_suite_non_local_backend_returns_explicit_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut options = std::collections::HashMap::new();
    options.insert(
        "table_root".to_string(),
        "s3://bench-bucket/path".to_string(),
    );
    let storage = StorageConfig::new(delta_bench::cli::StorageBackend::S3, options)
        .expect("valid s3 storage config");

    let cases = write::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
    for case in cases {
        let failure = case
            .failure
            .expect("non-local backend should emit explicit failure");
        assert!(
            failure.message.contains("non-local storage backend"),
            "expected unsupported backend failure message, got: {}",
            failure.message
        );
    }
}

#[tokio::test]
async fn concurrency_suite_non_local_backend_returns_explicit_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut options = std::collections::HashMap::new();
    options.insert(
        "table_root".to_string(),
        "s3://bench-bucket/path".to_string(),
    );
    let storage = StorageConfig::new(delta_bench::cli::StorageBackend::S3, options)
        .expect("valid s3 storage config");

    let cases = concurrency::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
    for case in cases {
        let failure = case
            .failure
            .expect("non-local backend should emit explicit failure");
        assert!(
            failure.message.contains("non-local storage backend"),
            "expected unsupported backend failure message, got: {}",
            failure.message
        );
    }
}

#[tokio::test]
async fn merge_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = merge::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}

#[tokio::test]
async fn merge_suite_missing_partitioned_fixture_returns_fixture_failures_for_all_cases() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");
    std::fs::remove_dir_all(
        temp.path()
            .join("sf1")
            .join("merge_partitioned_target_delta"),
    )
    .expect("remove partitioned merge fixture");

    let cases = merge::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
    for case in cases {
        let failure = case
            .failure
            .expect("failure should be present for missing fixture");
        assert!(
            failure.message.contains("fixture load failed"),
            "expected normalized fixture failure for {}, got: {}",
            case.case,
            failure.message
        );
    }
}

#[tokio::test]
async fn optimize_vacuum_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = optimize_vacuum::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}

#[tokio::test]
async fn delete_update_suite_missing_fixtures_returns_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let cases = delete_update::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| !c.success));
}

#[tokio::test]
async fn concurrency_suite_missing_contended_fixtures_returns_targeted_case_failures() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");
    std::fs::remove_dir_all(
        temp.path()
            .join("sf1")
            .join("delete_update_small_files_delta"),
    )
    .expect("remove delete/update fixture");
    std::fs::remove_dir_all(temp.path().join("sf1").join("optimize_small_files_delta"))
        .expect("remove optimize fixture");

    let cases = concurrency::run(temp.path(), "sf1", 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert_eq!(cases.len(), 5);

    let create_case = cases
        .iter()
        .find(|case| case.case == "concurrent_table_create")
        .expect("create case should be present");
    assert!(create_case.success, "create case should still succeed");

    let append_case = cases
        .iter()
        .find(|case| case.case == "concurrent_append_multi")
        .expect("append case should be present");
    assert!(append_case.success, "append case should still succeed");

    for case_name in [
        "update_vs_compaction",
        "delete_vs_compaction",
        "optimize_vs_optimize_overlap",
    ] {
        let case = cases
            .iter()
            .find(|case| case.case == case_name)
            .unwrap_or_else(|| panic!("missing concurrency case '{case_name}'"));
        assert!(!case.success, "{case_name} should fail without its fixture");
        let failure = case
            .failure
            .as_ref()
            .expect("failure payload should be present");
        assert!(
            failure.message.contains("fixture load failed"),
            "expected normalized fixture failure for {case_name}, got: {}",
            failure.message
        );
    }
}

#[tokio::test]
async fn interop_py_non_local_backend_is_reported_as_expected_failure() {
    let temp = tempfile::tempdir().expect("tempdir");
    let mut options = std::collections::HashMap::new();
    options.insert(
        "table_root".to_string(),
        "s3://bench-bucket/path".to_string(),
    );
    let storage = StorageConfig::new(delta_bench::cli::StorageBackend::S3, options)
        .expect("valid s3 storage config");

    let cases = interop_py::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("suite should not hard-fail");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| c.success));
    assert!(cases.iter().all(|c| c.classification == "expected_failure"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tpcds_duckdb_profile_reports_missing_python_executable() {
    let _env_lock = env_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let script = temp.path().join("fake_tpcds_generator.py");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env python3
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--scale-factor", required=True)
parser.add_argument("--output-csv", required=True)
args = parser.parse_args()
with open(args.output_csv, "w", encoding="utf-8") as handle:
    handle.write("ss_customer_sk,ss_ext_sales_price,ss_item_sk,ss_quantity,ss_sold_date_sk\n1,1.0,1,1,2450815\n")
"#,
    )
    .expect("write fake script");

    let err = with_env_vars(
        &[
            ("DELTA_BENCH_DUCKDB_PYTHON", "python3-nope"),
            (
                "DELTA_BENCH_TPCDS_DUCKDB_SCRIPT",
                script.to_string_lossy().as_ref(),
            ),
        ],
        || async {
            generate_fixtures_with_profile(
                temp.path(),
                "sf1",
                42,
                true,
                FixtureProfile::TpcdsDuckdb,
                &storage,
            )
            .await
        },
    )
    .await
    .expect_err("missing python binary should fail");

    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("duckdb generator")
            && message.contains("python3-nope")
            && message.contains("delta_bench_duckdb_python"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tpcds_duckdb_profile_reports_timeout() {
    let _env_lock = env_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let script = temp.path().join("slow_tpcds_generator.py");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env python3
import argparse
import time
parser = argparse.ArgumentParser()
parser.add_argument("--scale-factor", required=True)
parser.add_argument("--output-csv", required=True)
args = parser.parse_args()
time.sleep(0.3)
with open(args.output_csv, "w", encoding="utf-8") as handle:
    handle.write("ss_customer_sk,ss_ext_sales_price,ss_item_sk,ss_quantity,ss_sold_date_sk\n1,1.0,1,1,2450815\n")
"#,
    )
    .expect("write slow script");

    let err = with_env_vars(
        &[
            ("DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS", "10"),
            (
                "DELTA_BENCH_TPCDS_DUCKDB_SCRIPT",
                script.to_string_lossy().as_ref(),
            ),
        ],
        || async {
            generate_fixtures_with_profile(
                temp.path(),
                "sf1",
                42,
                true,
                FixtureProfile::TpcdsDuckdb,
                &storage,
            )
            .await
        },
    )
    .await
    .expect_err("slow generator should time out");

    assert!(
        err.to_string().to_ascii_lowercase().contains("timed out"),
        "unexpected timeout error: {err}"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tpcds_duckdb_profile_reports_malformed_csv() {
    let _env_lock = env_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let script = temp.path().join("bad_tpcds_generator.py");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env python3
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--scale-factor", required=True)
parser.add_argument("--output-csv", required=True)
args = parser.parse_args()
with open(args.output_csv, "w", encoding="utf-8") as handle:
    handle.write("ss_customer_sk,ss_ext_sales_price,ss_item_sk,ss_quantity,ss_sold_date_sk\nx,not-a-number,1,1,2450815\n")
"#,
    )
    .expect("write malformed script");

    let err = with_env_vars(
        &[(
            "DELTA_BENCH_TPCDS_DUCKDB_SCRIPT",
            script.to_string_lossy().as_ref(),
        )],
        || async {
            generate_fixtures_with_profile(
                temp.path(),
                "sf1",
                42,
                true,
                FixtureProfile::TpcdsDuckdb,
                &storage,
            )
            .await
        },
    )
    .await
    .expect_err("malformed csv should fail");

    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("csv") || message.contains("parse"),
        "unexpected malformed CSV error: {err}"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tpcds_duckdb_profile_reports_csv_header_mismatch() {
    let _env_lock = env_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let script = temp.path().join("wrong_header_tpcds_generator.py");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env python3
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--scale-factor", required=True)
parser.add_argument("--output-csv", required=True)
args = parser.parse_args()
with open(args.output_csv, "w", encoding="utf-8") as handle:
    handle.write("wrong_header\n1,1.0,1,1,2450815\n")
"#,
    )
    .expect("write malformed header script");

    let err = with_env_vars(
        &[(
            "DELTA_BENCH_TPCDS_DUCKDB_SCRIPT",
            script.to_string_lossy().as_ref(),
        )],
        || async {
            generate_fixtures_with_profile(
                temp.path(),
                "sf1",
                42,
                true,
                FixtureProfile::TpcdsDuckdb,
                &storage,
            )
            .await
        },
    )
    .await
    .expect_err("header mismatch should fail");

    assert!(
        err.to_string().contains("header mismatch"),
        "unexpected header mismatch error: {err}"
    );
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tpcds_duckdb_profile_reports_csv_without_data_rows() {
    let _env_lock = env_lock();
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let script = temp.path().join("empty_rows_tpcds_generator.py");
    std::fs::write(
        &script,
        r#"#!/usr/bin/env python3
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--scale-factor", required=True)
parser.add_argument("--output-csv", required=True)
args = parser.parse_args()
with open(args.output_csv, "w", encoding="utf-8") as handle:
    handle.write("ss_customer_sk,ss_ext_sales_price,ss_item_sk,ss_quantity,ss_sold_date_sk\n")
"#,
    )
    .expect("write no-data script");

    let err = with_env_vars(
        &[(
            "DELTA_BENCH_TPCDS_DUCKDB_SCRIPT",
            script.to_string_lossy().as_ref(),
        )],
        || async {
            generate_fixtures_with_profile(
                temp.path(),
                "sf1",
                42,
                true,
                FixtureProfile::TpcdsDuckdb,
                &storage,
            )
            .await
        },
    )
    .await
    .expect_err("missing data rows should fail");

    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("no data rows"),
        "unexpected no-rows error: {err}"
    );
}
