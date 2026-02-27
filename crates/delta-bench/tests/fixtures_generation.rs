use delta_bench::data::fixtures::{generate_fixtures, load_manifest};
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
