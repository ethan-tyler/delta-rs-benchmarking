use delta_bench::data::fixtures::{fixture_root, generate_fixtures, load_manifest};
use delta_bench::storage::StorageConfig;

#[tokio::test]
async fn regenerates_when_requested_seed_changes_without_force() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, false, &storage)
        .await
        .expect("generate fixtures seed 42");
    let first = load_manifest(temp.path(), "sf1").expect("load first manifest");
    assert_eq!(first.seed, 42);

    generate_fixtures(temp.path(), "sf1", 43, false, &storage)
        .await
        .expect("regenerate fixtures seed 43");
    let second = load_manifest(temp.path(), "sf1").expect("load second manifest");
    assert_eq!(second.seed, 43);
}

#[tokio::test]
async fn rejects_unknown_scale() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();
    let err = generate_fixtures(temp.path(), "sf-nope", 42, false, &storage)
        .await
        .expect_err("unknown scale should fail");
    assert!(
        err.to_string().contains("unknown scale"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn generates_wave1_specialized_fixture_tables() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("generate fixtures");

    let root = temp.path().join("sf1");
    for table_name in [
        "read_partitioned_delta",
        "merge_partitioned_target_delta",
        "optimize_compacted_delta",
    ] {
        let table_path = root.join(table_name);
        assert!(
            table_path.exists(),
            "expected table dir: {}",
            table_path.display()
        );
        assert!(
            table_path.join("_delta_log").exists(),
            "expected delta log dir: {}",
            table_path.join("_delta_log").display()
        );
    }
}

#[tokio::test]
async fn regenerates_when_wave1_fixture_tables_are_missing_without_force() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("initial generate fixtures");

    let root = temp.path().join("sf1");
    for table_name in [
        "read_partitioned_delta",
        "merge_partitioned_target_delta",
        "optimize_compacted_delta",
    ] {
        std::fs::remove_dir_all(root.join(table_name))
            .unwrap_or_else(|err| panic!("remove {table_name}: {err}"));
    }

    generate_fixtures(temp.path(), "sf1", 42, false, &storage)
        .await
        .expect("should regenerate when wave1 fixture tables are missing");

    for table_name in [
        "read_partitioned_delta",
        "merge_partitioned_target_delta",
        "optimize_compacted_delta",
    ] {
        let table_path = root.join(table_name);
        assert!(
            table_path.join("_delta_log").exists(),
            "expected regenerated table dir: {}",
            table_path.display()
        );
    }
}
