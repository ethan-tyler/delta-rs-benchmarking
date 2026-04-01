use delta_bench::cli::{BenchmarkLane, TimingPhase};
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{merge, optimize_vacuum, run_target, scan};

const REQUALIFIED_SCAN_PRUNING_HIT_RESULT_HASH: &str =
    "sha256:b333362484714c71fa268b017d1c773a466e417959ec16336a749be670961eea";

#[tokio::test]
async fn generated_fixtures_support_real_scan_suite() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = scan::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("scan suite run");
    assert!(!cases.is_empty());
    assert!(cases.iter().all(|c| c.success));
}

#[tokio::test]
async fn scan_samples_include_physical_scan_metrics() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = scan::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("scan suite run");
    assert!(!cases.is_empty());
    let sample_metrics = cases
        .iter()
        .filter(|case| case.success)
        .flat_map(|case| case.samples.iter())
        .filter_map(|sample| sample.metrics.as_ref())
        .collect::<Vec<_>>();
    assert!(
        !sample_metrics.is_empty(),
        "expected scan sample metrics; cases={:?}",
        cases
            .iter()
            .map(|case| (&case.case, case.success, &case.failure))
            .collect::<Vec<_>>()
    );
    assert!(sample_metrics.iter().any(|m| m.files_scanned.is_some()));
    assert!(sample_metrics.iter().any(|m| m.bytes_scanned.is_some()));
    assert!(sample_metrics.iter().any(|m| m.scan_time_ms.is_some()));
    for metrics in sample_metrics {
        if let (Some(files_scanned), Some(files_pruned)) =
            (metrics.files_scanned, metrics.files_pruned)
        {
            assert!(
                files_scanned + files_pruned > 0,
                "expected non-zero scan accounting"
            );
        }
    }
}

#[tokio::test]
async fn scan_pruning_hit_scans_fewer_files_than_miss() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = scan::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("scan suite run");

    let hit_case = cases
        .iter()
        .find(|case| case.case == "scan_pruning_hit")
        .expect("expected scan_pruning_hit case");
    let miss_case = cases
        .iter()
        .find(|case| case.case == "scan_pruning_miss")
        .expect("expected scan_pruning_miss case");

    assert!(
        hit_case.success,
        "hit case should succeed: {:?}",
        hit_case.failure
    );
    assert!(
        miss_case.success,
        "miss case should succeed: {:?}",
        miss_case.failure
    );

    let hit_metrics = hit_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("expected metrics for hit case");
    let miss_metrics = miss_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("expected metrics for miss case");
    assert_eq!(
        hit_metrics.result_hash.as_deref(),
        Some(REQUALIFIED_SCAN_PRUNING_HIT_RESULT_HASH),
        "scan_pruning_hit should stay on the requalified exact-result contract"
    );

    let mut compared = false;
    if let (Some(hit_scanned), Some(miss_scanned)) =
        (hit_metrics.files_scanned, miss_metrics.files_scanned)
    {
        compared = true;
        assert!(
            hit_scanned < miss_scanned,
            "expected hit files_scanned < miss files_scanned, got {hit_scanned} vs {miss_scanned}"
        );
    }
    if let (Some(hit_pruned), Some(miss_pruned)) =
        (hit_metrics.files_pruned, miss_metrics.files_pruned)
    {
        compared = true;
        assert!(
            hit_pruned > miss_pruned,
            "expected hit files_pruned > miss files_pruned, got {hit_pruned} vs {miss_pruned}"
        );
    }
    if !compared {
        if let (Some(hit_bytes), Some(miss_bytes)) =
            (hit_metrics.bytes_scanned, miss_metrics.bytes_scanned)
        {
            compared = true;
            assert!(
                hit_bytes < miss_bytes,
                "expected hit bytes_scanned < miss bytes_scanned, got {hit_bytes} vs {miss_bytes}"
            );
        }
    }
    if !compared {
        eprintln!(
            "scan pruning contrast metrics unavailable for this plan path: hit={hit_metrics:?} miss={miss_metrics:?}"
        );
    }
}

#[tokio::test]
async fn scan_plan_phase_preserves_case_identity_and_hashes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let load_cases = scan::run(temp.path(), "sf1", TimingPhase::Load, 0, 1, &storage)
        .await
        .expect("scan suite run");
    let plan_cases = scan::run(temp.path(), "sf1", TimingPhase::Plan, 0, 1, &storage)
        .await
        .expect("scan suite run");
    let execute_cases = scan::run(temp.path(), "sf1", TimingPhase::Execute, 0, 1, &storage)
        .await
        .expect("scan suite run");
    let validate_cases = scan::run(temp.path(), "sf1", TimingPhase::Validate, 0, 1, &storage)
        .await
        .expect("scan suite run");

    assert!(
        plan_cases.iter().all(|case| !case.case.contains("_plan_")),
        "plan timing should not invent ad hoc case ids: {:?}",
        plan_cases
            .iter()
            .map(|case| case.case.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        load_cases.iter().all(|case| !case.case.contains("_load_")),
        "load timing should not invent ad hoc case ids: {:?}",
        load_cases
            .iter()
            .map(|case| case.case.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        validate_cases
            .iter()
            .all(|case| !case.case.contains("_validate_")),
        "validate timing should not invent ad hoc case ids: {:?}",
        validate_cases
            .iter()
            .map(|case| case.case.as_str())
            .collect::<Vec<_>>()
    );

    let load_case = load_cases
        .iter()
        .find(|case| case.case == "scan_filter_flag")
        .expect("expected scan_filter_flag case");
    let planning_case = plan_cases
        .iter()
        .find(|case| case.case == "scan_filter_flag")
        .expect("expected scan_filter_flag case");
    let execute_case = execute_cases
        .iter()
        .find(|case| case.case == "scan_filter_flag")
        .expect("expected scan_filter_flag case");
    let validate_case = validate_cases
        .iter()
        .find(|case| case.case == "scan_filter_flag")
        .expect("expected scan_filter_flag case");
    assert!(
        load_case.success,
        "load-timed case should succeed: {:?}",
        load_case.failure
    );
    assert!(
        planning_case.success,
        "plan-timed case should succeed: {:?}",
        planning_case.failure
    );
    assert!(
        execute_case.success,
        "execute-timed case should succeed: {:?}",
        execute_case.failure
    );
    assert!(
        validate_case.success,
        "validate-timed case should succeed: {:?}",
        validate_case.failure
    );

    let load_metrics = load_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("load metrics should exist");
    let planning_metrics = planning_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("planning metrics should exist");
    let execute_metrics = execute_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("execute metrics should exist");
    let validate_metrics = validate_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("validate metrics should exist");
    assert!(
        planning_metrics.rows_processed.is_some(),
        "plan timing should preserve result metrics"
    );
    assert!(
        planning_metrics.result_hash.is_some(),
        "planning case should include a stable result hash"
    );
    assert!(
        planning_metrics.schema_hash.is_some(),
        "planning case should include a schema hash"
    );
    assert_eq!(
        planning_metrics.result_hash, execute_metrics.result_hash,
        "plan timing should preserve exact query result hashes"
    );
    assert_eq!(
        load_metrics.result_hash, execute_metrics.result_hash,
        "load timing should preserve exact query result hashes"
    );
    assert_eq!(
        validate_metrics.result_hash, execute_metrics.result_hash,
        "validate timing should preserve exact query result hashes"
    );
    assert_eq!(
        planning_metrics.schema_hash, execute_metrics.schema_hash,
        "plan timing should preserve output schema hashes"
    );
    assert_eq!(
        load_metrics.schema_hash, execute_metrics.schema_hash,
        "load timing should preserve output schema hashes"
    );
    assert_eq!(
        validate_metrics.schema_hash, execute_metrics.schema_hash,
        "validate timing should preserve output schema hashes"
    );
}

#[tokio::test]
async fn correctness_lane_emits_semantic_digests_for_stateful_suites() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    for suite in [
        "write",
        "delete_update",
        "merge",
        "metadata",
        "optimize_vacuum",
    ] {
        let cases = run_target(
            temp.path(),
            suite,
            "sf1",
            BenchmarkLane::Correctness,
            TimingPhase::Execute,
            0,
            1,
            &storage,
        )
        .await
        .expect("suite should run");
        assert!(
            cases.iter().all(|case| case.success),
            "correctness lane should succeed for {suite}: {:?}",
            cases
                .iter()
                .map(|case| (&case.case, &case.failure))
                .collect::<Vec<_>>()
        );
        for metrics in cases
            .iter()
            .flat_map(|case| case.samples.iter())
            .filter_map(|sample| sample.metrics.as_ref())
        {
            assert!(
                metrics.semantic_state_digest.is_some(),
                "semantic digest missing for {suite}: {metrics:?}"
            );
            assert!(
                metrics.validation_summary.is_some(),
                "validation summary missing for {suite}: {metrics:?}"
            );
        }
    }
}

#[tokio::test]
async fn generated_fixtures_support_optimize_vacuum_suite() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = optimize_vacuum::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("optimize_vacuum suite run");
    assert_eq!(cases.len(), 5);
    assert!(
        cases.iter().all(|c| c.success),
        "optimize_vacuum failures: {:?}",
        cases
            .iter()
            .map(|c| (&c.case, &c.failure))
            .collect::<Vec<_>>()
    );

    let optimize_case = cases
        .iter()
        .find(|c| c.case == "optimize_compact_small_files")
        .expect("optimize case should exist");
    let optimize_sample = optimize_case
        .samples
        .first()
        .expect("optimize sample should exist");
    let optimize_metrics = optimize_sample
        .metrics
        .as_ref()
        .expect("optimize metrics should exist");
    let files_scanned = optimize_metrics
        .files_scanned
        .expect("files_scanned should be present");
    let files_pruned = optimize_metrics
        .files_pruned
        .expect("files_pruned should be present");
    assert!(
        files_scanned >= files_pruned,
        "files_scanned should be >= files_pruned"
    );
    let compact_operations = optimize_metrics
        .operations
        .expect("compact optimize operations should be present");

    let noop_case = cases
        .iter()
        .find(|c| c.case == "optimize_noop_already_compact")
        .expect("noop optimize case should exist");
    let noop_metrics = noop_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("noop optimize metrics should exist");
    assert_eq!(
        noop_metrics.operations,
        Some(0),
        "noop optimize should not rewrite files"
    );
    assert_eq!(
        noop_metrics.files_scanned, noop_metrics.files_pruned,
        "noop optimize should skip all scanned files"
    );

    let heavy_case = cases
        .iter()
        .find(|c| c.case == "optimize_heavy_compaction")
        .expect("heavy optimize case should exist");
    let heavy_metrics = heavy_case
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("heavy optimize metrics should exist");
    let heavy_operations = heavy_metrics
        .operations
        .expect("heavy optimize operations should be present");
    assert!(heavy_operations > 0, "heavy optimize should rewrite files");
    assert!(
        heavy_operations > compact_operations,
        "heavy optimize should perform more rewrite operations than compact case, got {heavy_operations} vs {compact_operations}"
    );
    assert!(
        heavy_metrics.files_scanned.unwrap_or(0) >= heavy_metrics.files_pruned.unwrap_or(0),
        "heavy optimize should not prune more files than it scanned"
    );
}

#[tokio::test]
async fn merge_partition_localized_case_reports_pruned_files() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let cases = merge::run(temp.path(), "sf1", BenchmarkLane::Macro, 0, 1, &storage)
        .await
        .expect("run merge suite");
    let localized = cases
        .iter()
        .find(|case| case.case == "merge_localized_1pct")
        .expect("expected merge_localized_1pct case");
    assert!(
        localized.success,
        "localized merge case failed: {:?}",
        localized.failure
    );
    let metrics = localized
        .samples
        .first()
        .and_then(|sample| sample.metrics.as_ref())
        .expect("expected localized merge metrics");
    assert!(metrics.files_scanned.is_some());
    assert!(metrics.files_pruned.is_some());
    assert!(metrics.scan_time_ms.is_some());
    assert!(metrics.rewrite_time_ms.is_some());
    assert!(
        metrics.files_pruned.unwrap_or(0) > 0,
        "expected localized merge to prune files, got {:?}",
        metrics.files_pruned
    );
}
