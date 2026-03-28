use std::path::{Path, PathBuf};

use delta_bench::manifests::{load_manifest, DatasetId, ManifestAssertion};
use delta_bench::suites::list_cases_for_target;
use delta_bench::suites::tpcds::catalog::phase1_query_catalog;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn rust_manifest_path() -> PathBuf {
    repo_root().join("bench/manifests/core_rust.yaml")
}

fn python_manifest_path() -> PathBuf {
    repo_root().join("bench/manifests/core_python.yaml")
}

#[test]
fn loads_p0_rust_manifest_in_file_order() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");
    let ids = manifest
        .cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        ids,
        vec![
            "scan_full_narrow",
            "scan_projection_region",
            "scan_filter_flag",
            "scan_pruning_hit",
            "scan_pruning_miss",
            "write_append_small",
            "write_append_large",
            "write_overwrite",
            "write_perf_partitioned_1m_parts_010",
            "write_perf_partitioned_1m_parts_100",
            "write_perf_partitioned_5m_parts_010",
            "write_perf_unpartitioned_1m",
            "delete_1pct_localized",
            "delete_5pct_scattered",
            "delete_50pct_broad",
            "update_literal_1pct_localized",
            "update_literal_5pct_scattered",
            "update_expr_50pct_broad",
            "update_all_rows_expr",
            "merge_delete_5pct",
            "merge_upsert_10pct_insert_10pct",
            "merge_upsert_10pct",
            "merge_upsert_50pct",
            "merge_upsert_90pct",
            "merge_localized_1pct",
            "metadata_load",
            "metadata_time_travel_v0",
            "optimize_compact_small_files",
            "optimize_noop_already_compact",
            "optimize_heavy_compaction",
            "vacuum_dry_run_lite",
            "vacuum_execute_lite",
            "concurrent_table_create",
            "concurrent_append_multi",
            "update_vs_compaction",
            "delete_vs_compaction",
            "optimize_vs_optimize_overlap",
            "tpcds_q03",
            "tpcds_q07",
            "tpcds_q64",
        ]
    );
}

#[test]
fn p0_rust_manifest_includes_all_write_perf_cases() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");
    let expected_cases = list_cases_for_target("write_perf")
        .expect("write_perf should be a registered suite target");

    for case in expected_cases {
        let present = manifest
            .cases
            .iter()
            .any(|entry| entry.target == "write_perf" && entry.id == case);
        assert!(
            present,
            "missing write_perf manifest entry for case '{case}'"
        );
    }
}

#[test]
fn dataset_id_parsing_supports_p0_catalog() {
    assert_eq!(
        DatasetId::parse("tiny_smoke").expect("tiny_smoke"),
        DatasetId::TinySmoke
    );
    assert_eq!(
        DatasetId::parse("medium_selective").expect("medium_selective"),
        DatasetId::MediumSelective
    );
    assert_eq!(
        DatasetId::parse("small_files").expect("small_files"),
        DatasetId::SmallFiles
    );
    assert_eq!(
        DatasetId::parse("many_versions").expect("many_versions"),
        DatasetId::ManyVersions
    );
    assert_eq!(
        DatasetId::parse("tpcds_duckdb").expect("tpcds_duckdb"),
        DatasetId::TpcdsDuckdb
    );
    assert_eq!(DatasetId::TinySmoke.fixture_profile(), "standard");
    assert_eq!(DatasetId::ManyVersions.fixture_profile(), "many_versions");
    assert_eq!(DatasetId::TpcdsDuckdb.fixture_profile(), "tpcds_duckdb");
    assert_eq!(DatasetId::TpcdsDuckdb.scale(), "sf1");
}

#[test]
fn dataset_id_rejects_unknown_values() {
    let err = DatasetId::parse("not-real").expect_err("unknown dataset id must fail");
    assert!(
        err.to_string().contains("dataset_id"),
        "unexpected error: {err}"
    );
}

#[test]
fn manifest_assertions_parse_from_yaml() {
    let temp = tempfile::tempdir().expect("tempdir");
    let file = temp.path().join("manifest.yaml");
    std::fs::write(
        &file,
        r#"
id: test
description: test manifest
cases:
  - id: case1
    target: write
    runner: rust
    lane: correctness
    enabled: true
    supports_decision: false
    required_runs: 1
    decision_threshold_pct: 0.0
    decision_metric: median
    assertions:
      - type: expected_error_contains
        value: fixture load failed
      - type: version_monotonicity
"#,
    )
    .expect("write manifest");

    let manifest = load_manifest(&file).expect("manifest should parse");
    assert_eq!(manifest.cases.len(), 1);
    assert_eq!(manifest.cases[0].lane, "correctness");
    assert_eq!(manifest.cases[0].supports_decision, Some(false));
    assert_eq!(manifest.cases[0].required_runs, Some(1));
    assert_eq!(manifest.cases[0].decision_threshold_pct, Some(0.0));
    assert_eq!(manifest.cases[0].decision_metric.as_deref(), Some("median"));
    assert_eq!(manifest.cases[0].assertions.len(), 2);
    assert!(matches!(
        manifest.cases[0].assertions[0],
        ManifestAssertion::ExpectedErrorContains { .. }
    ));
    assert!(matches!(
        manifest.cases[0].assertions[1],
        ManifestAssertion::VersionMonotonicity
    ));
}

#[test]
fn manifest_rejects_unknown_lane_values() {
    let temp = tempfile::tempdir().expect("tempdir");
    let file = temp.path().join("manifest.yaml");
    std::fs::write(
        &file,
        r#"
id: test
description: invalid lane manifest
cases:
  - id: case1
    target: write
    runner: rust
    lane: macroo
"#,
    )
    .expect("write manifest");

    let err = load_manifest(&file).expect_err("unknown lane must fail");
    let message = err.to_string();
    assert!(
        message.contains("case1"),
        "case id should be named: {message}"
    );
    assert!(
        message.contains("macroo"),
        "invalid lane should be echoed back: {message}"
    );
    assert!(
        message.contains("smoke, correctness, macro"),
        "allowed lanes should be documented: {message}"
    );
}

#[test]
fn p0_rust_manifest_includes_all_delete_update_cases() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");
    let expected_cases = list_cases_for_target("delete_update")
        .expect("delete_update should be a registered suite target");

    for case in expected_cases {
        let present = manifest
            .cases
            .iter()
            .any(|entry| entry.target == "delete_update" && entry.id == case);
        assert!(
            present,
            "missing delete_update manifest entry for case '{case}'"
        );
    }
}

#[test]
fn p0_rust_manifest_enforces_hash_assertions_for_every_enabled_case() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    let missing = manifest
        .cases
        .iter()
        .filter(|case| case.enabled && case.runner == "rust")
        .filter(|case| {
            let has_result_hash = case
                .assertions
                .iter()
                .any(|assertion| matches!(assertion, ManifestAssertion::ExactResultHash { .. }));
            let has_schema_hash = case
                .assertions
                .iter()
                .any(|assertion| matches!(assertion, ManifestAssertion::SchemaHash { .. }));
            !has_schema_hash || (case.target != "concurrency" && !has_result_hash)
        })
        .map(|case| case.id.clone())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "every enabled rust case in core_rust.yaml should include schema_hash assertions, and all non-concurrency rust cases should include exact_result_hash assertions, missing={missing:?}"
    );
}

#[test]
fn p0_rust_manifest_excludes_unqualified_scan_case_from_authoritative_lane() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    let scan_pruning_miss = manifest
        .cases
        .iter()
        .find(|case| case.id == "scan_pruning_miss")
        .expect("scan_pruning_miss should stay listed for explicit review");
    assert!(
        !scan_pruning_miss.enabled,
        "scan_pruning_miss must stay disabled until its exact-result contract is requalified"
    );

    let enabled_decision_scan_cases = manifest
        .cases
        .iter()
        .filter(|case| {
            case.target == "scan" && case.enabled && case.supports_decision == Some(true)
        })
        .map(|case| case.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        enabled_decision_scan_cases,
        vec![
            "scan_full_narrow",
            "scan_projection_region",
            "scan_filter_flag",
            "scan_pruning_hit",
        ]
    );
}

#[test]
fn p0_rust_manifest_case_ids_match_suite_case_lists() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    for case in manifest
        .cases
        .iter()
        .filter(|entry| entry.enabled && entry.runner == "rust")
    {
        let suite_cases = list_cases_for_target(&case.target).unwrap_or_else(|error| {
            panic!(
                "manifest case '{}' references unknown target '{}': {error}",
                case.id, case.target
            )
        });
        assert!(
            suite_cases.contains(&case.id),
            "manifest case '{}' for target '{}' is missing from suite case_names(); suite_cases={suite_cases:?}",
            case.id,
            case.target
        );
    }
}

#[test]
fn p0_rust_manifest_includes_enabled_tpcds_cases() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    for spec in phase1_query_catalog()
        .into_iter()
        .filter(|spec| spec.enabled)
    {
        let case_id = format!("tpcds_{}", spec.id);
        let present = manifest
            .cases
            .iter()
            .any(|entry| entry.target == "tpcds" && entry.id == case_id);
        assert!(
            present,
            "missing enabled TPC-DS manifest entry for case '{case_id}'"
        );
    }
}
#[test]
fn p0_rust_manifest_includes_all_concurrency_cases() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");
    let expected_cases = list_cases_for_target("concurrency")
        .expect("concurrency should be a registered suite target");

    for case in expected_cases {
        let present = manifest
            .cases
            .iter()
            .any(|entry| entry.target == "concurrency" && entry.id == case);
        assert!(
            present,
            "missing concurrency manifest entry for case '{case}'"
        );
    }
}

#[test]
fn p0_rust_manifest_does_not_require_exact_hash_assertions_for_concurrency_cases() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    let concurrency_entries = manifest
        .cases
        .iter()
        .filter(|entry| entry.target == "concurrency")
        .collect::<Vec<_>>();
    assert!(
        !concurrency_entries.is_empty(),
        "expected concurrency entries to be present in the rust manifest"
    );
    assert!(
        concurrency_entries.iter().all(|entry| {
            entry
                .assertions
                .iter()
                .all(|assertion| !matches!(assertion, ManifestAssertion::ExactResultHash { .. }))
        }),
        "concurrency cases should not use exact_result_hash assertions"
    );
}

#[test]
fn p0_rust_manifest_scopes_version_monotonicity_to_shared_table_concurrency_cases() {
    let manifest_path = rust_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    let required_cases = ["concurrent_table_create", "concurrent_append_multi"];
    let missing = manifest
        .cases
        .iter()
        .filter(|entry| {
            entry.target == "concurrency" && required_cases.contains(&entry.id.as_str())
        })
        .filter(|entry| {
            !entry
                .assertions
                .iter()
                .any(|assertion| matches!(assertion, ManifestAssertion::VersionMonotonicity))
        })
        .map(|entry| entry.id.clone())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "shared-table concurrency cases should include version_monotonicity assertions, missing={missing:?}"
    );

    let unexpected = manifest
        .cases
        .iter()
        .filter(|entry| {
            entry.target == "concurrency" && !required_cases.contains(&entry.id.as_str())
        })
        .filter(|entry| {
            entry
                .assertions
                .iter()
                .any(|assertion| matches!(assertion, ManifestAssertion::VersionMonotonicity))
        })
        .map(|entry| entry.id.clone())
        .collect::<Vec<_>>();

    assert!(
        unexpected.is_empty(),
        "cloned-table concurrency cases should not include version_monotonicity assertions, unexpected={unexpected:?}"
    );
}

#[test]
fn p0_python_manifest_includes_all_interop_cases() {
    let manifest_path = python_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");
    let expected_cases = list_cases_for_target("interop_py")
        .expect("interop_py should be a registered suite target");

    for case in expected_cases {
        let present = manifest
            .cases
            .iter()
            .any(|entry| entry.target == "interop_py" && entry.id == case);
        assert!(present, "missing interop manifest entry for case '{case}'");
    }
}

#[test]
fn p0_python_manifest_enforces_hash_assertions_for_every_enabled_case() {
    let manifest_path = python_manifest_path();
    let manifest = load_manifest(&manifest_path).expect("manifest should load");

    let missing = manifest
        .cases
        .iter()
        .filter(|case| case.enabled && case.runner == "python")
        .filter(|case| {
            let has_result_hash = case
                .assertions
                .iter()
                .any(|assertion| matches!(assertion, ManifestAssertion::ExactResultHash { .. }));
            let has_schema_hash = case
                .assertions
                .iter()
                .any(|assertion| matches!(assertion, ManifestAssertion::SchemaHash { .. }));
            !(has_result_hash && has_schema_hash)
        })
        .map(|case| case.id.clone())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "every enabled python case in core_python.yaml should include both exact_result_hash and schema_hash assertions, missing={missing:?}"
    );
}
