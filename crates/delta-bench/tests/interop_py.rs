use delta_bench::cli::BenchmarkLane;
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::results::BenchRunResult;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::interop_py;
use std::fs;
use std::process::Command;

#[tokio::test]
async fn interop_py_suite_runs_with_deterministic_case_ids() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = interop_py::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("interop suite run");

    assert_eq!(
        cases.iter().map(|c| c.case.as_str()).collect::<Vec<_>>(),
        vec![
            "pandas_roundtrip_smoke",
            "polars_roundtrip_smoke",
            "pyarrow_dataset_scan_perf",
        ]
    );
    assert!(cases.iter().all(|c| c.success));
    assert!(
        cases
            .iter()
            .all(|c| matches!(c.classification.as_str(), "supported" | "expected_failure")),
        "unexpected classifications: {:?}",
        cases
            .iter()
            .map(|c| (&c.case, &c.classification, &c.failure))
            .collect::<Vec<_>>()
    );
}

#[test]
fn correctness_lane_interop_run_is_manifest_backed_semantic_validation() {
    let temp = tempfile::tempdir().expect("tempdir");
    let fixtures_dir = temp.path().join("fixtures");
    let results_dir = temp.path().join("results");
    let storage = StorageConfig::local();
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    runtime
        .block_on(generate_fixtures(&fixtures_dir, "sf1", 42, true, &storage))
        .expect("generate fixtures");

    let bin = env!("CARGO_BIN_EXE_delta-bench");
    let output = Command::new(bin)
        .arg("--fixtures-dir")
        .arg(&fixtures_dir)
        .arg("--results-dir")
        .arg(&results_dir)
        .arg("--label")
        .arg("interop-correctness")
        .arg("run")
        .arg("--scale")
        .arg("sf1")
        .arg("--target")
        .arg("interop_py")
        .arg("--runner")
        .arg("python")
        .arg("--lane")
        .arg("correctness")
        .output()
        .expect("run delta-bench binary");

    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let result_path = results_dir
        .join("interop-correctness")
        .join("interop_py.json");
    let payload = fs::read_to_string(result_path).expect("read result payload");
    let parsed: BenchRunResult = serde_json::from_str(&payload).expect("parse result payload");
    assert_eq!(parsed.context.validation_level.as_deref(), Some("semantic"));
    assert!(
        parsed
            .cases
            .iter()
            .all(|case| case.success && case.validation_passed),
        "correctness run should satisfy manifest-backed interop assertions: {:?}",
        parsed
            .cases
            .iter()
            .map(|case| (
                &case.case,
                case.success,
                case.validation_passed,
                &case.failure
            ))
            .collect::<Vec<_>>()
    );
}
