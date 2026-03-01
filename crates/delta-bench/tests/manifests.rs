use delta_bench::manifests::{load_manifest, DatasetId, ManifestAssertion};

#[test]
fn loads_p0_rust_manifest_in_file_order() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest_path = root.join("bench/manifests/p0-rust.yaml");
    let manifest = load_manifest(&manifest_path).expect("manifest should load");
    let ids = manifest
        .cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        ids,
        vec![
            "read_full_scan_narrow",
            "read_projection_region",
            "read_filter_flag_true",
            "read_partition_pruning_hit",
            "read_partition_pruning_miss",
            "write_append_small_batches",
            "write_append_large_batches",
            "write_overwrite",
            "delete_only_filesMatchedFraction_0.05_rowsMatchedFraction_0.05",
            "upsert_filesMatchedFraction_0.05_rowsMatchedFraction_0.1_rowsNotMatchedFraction_0.1",
            "merge_upsert_10pct",
            "merge_upsert_50pct",
            "merge_upsert_90pct",
            "merge_partition_localized_1pct",
            "metadata_table_load",
            "metadata_time_travel_v0",
            "optimize_compact_small_files",
            "optimize_noop_already_compact",
            "optimize_heavy_compaction",
            "vacuum_dry_run_lite",
            "vacuum_execute_lite",
        ]
    );
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
    enabled: true
    assertions:
      - type: expected_error_contains
        value: fixture load failed
      - type: version_monotonicity
"#,
    )
    .expect("write manifest");

    let manifest = load_manifest(&file).expect("manifest should parse");
    assert_eq!(manifest.cases.len(), 1);
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
