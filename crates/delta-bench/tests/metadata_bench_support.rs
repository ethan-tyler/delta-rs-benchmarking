use std::fs;

use delta_bench::data::fixtures::{generate_fixtures_with_profile, FixtureProfile};
use delta_bench::metadata_bench_support::{
    benchmark_case_context, benchmark_case_spec, benchmark_commit_log_bytes,
    benchmark_history_file_count, benchmark_materialize_files, benchmark_snapshot_try_new,
    MetadataLogActionProfile,
};
use delta_bench::storage::StorageConfig;
use deltalake_core::logstore::get_actions;

#[test]
fn metadata_bench_support_builds_deterministic_get_actions_inputs() {
    for profile in [
        MetadataLogActionProfile::SimpleActions,
        MetadataLogActionProfile::WithStats,
        MetadataLogActionProfile::FullComplexity,
    ] {
        let left = benchmark_commit_log_bytes(profile, 1_000);
        let right = benchmark_commit_log_bytes(profile, 1_000);
        assert_eq!(left, right, "commit-log bytes should be deterministic");

        let actions = get_actions(0, &left).expect("generated commit log should parse");
        assert_eq!(
            actions.len(),
            1_002,
            "protocol + commitInfo + 1_000 add actions should be present"
        );
    }
}

#[tokio::test]
async fn metadata_bench_support_many_versions_snapshot_helpers_cover_head_and_version_zero() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_fixtures_with_profile(
        temp.path(),
        "sf1",
        42,
        true,
        FixtureProfile::ManyVersions,
        &storage,
    )
    .await
    .expect("fixtures should be generated");

    let spec = benchmark_case_spec(temp.path(), "sf1", &storage).expect("metadata log case spec");
    assert!(
        benchmark_history_file_count(&spec) > 20,
        "many_versions fixture should expose a long log history"
    );

    let ctx = benchmark_case_context(&storage, spec.clone())
        .await
        .expect("metadata log bench context");

    let head = benchmark_snapshot_try_new(&ctx, None)
        .await
        .expect("head snapshot");
    let version_zero = benchmark_snapshot_try_new(&ctx, Some(0))
        .await
        .expect("version-zero snapshot");
    assert!(
        head.version() > version_zero.version(),
        "head and version-zero snapshots should stay on distinct replay paths"
    );

    let materialized = benchmark_materialize_files(&ctx)
        .await
        .expect("materialized head files");
    let version_zero_version = u64::try_from(version_zero.version())
        .expect("version-zero snapshot version should be non-negative");
    assert!(
        materialized.version > version_zero_version,
        "materialization helper should stay on the head path"
    );
    assert!(
        !materialized.files.is_empty(),
        "materialization helper should expose file batches"
    );
}

#[test]
fn metadata_bench_support_profile_is_criterion_only() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let profile = root.join("bench/methodologies/metadata-log-criterion.env");
    let bench = root.join("crates/delta-bench/benches/metadata_log_bench.rs");

    let profile_text = fs::read_to_string(&profile).expect("read metadata log criterion env");
    assert!(
        profile_text.contains("PROFILE_KIND=criterion"),
        "metadata log profile must stay criterion-only"
    );
    assert!(
        profile_text.contains("CRITERION_BENCH=metadata_log_bench"),
        "metadata log profile must target the dedicated criterion bench"
    );

    let bench_text = fs::read_to_string(&bench).expect("read metadata log bench source");
    for expected in [
        "metadata_log/get_actions",
        "simple_actions_1000",
        "with_stats_1000",
        "full_complexity_1000",
        "metadata_log/snapshot",
        "try_new_head_many_versions",
        "try_new_version_0_many_versions",
        "metadata_log/eager_snapshot",
        "materialize_files_head_many_versions",
        "criterion_group!",
        "criterion_main!",
    ] {
        assert!(
            bench_text.contains(expected),
            "metadata log bench missing {expected}"
        );
    }
}
