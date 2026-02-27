use clap::Parser;

use delta_bench::cli::{parse_storage_options, Args, StorageBackend};

#[test]
fn cli_defaults_to_local_storage_backend() {
    let args = Args::parse_from(["delta-bench", "doctor"]);
    assert_eq!(args.storage_backend, StorageBackend::Local);
    assert!(args.storage_options.is_empty());
}

#[test]
fn cli_accepts_repeated_storage_options() {
    let args = Args::parse_from([
        "delta-bench",
        "--storage-backend",
        "s3",
        "--storage-option",
        "table_root=s3://bench-bucket/delta-bench",
        "--storage-option",
        "AWS_REGION=us-east-1",
        "doctor",
    ]);
    assert_eq!(args.storage_backend, StorageBackend::S3);
    assert_eq!(args.storage_options.len(), 2);
}

#[test]
fn parse_storage_options_rejects_invalid_entries() {
    let err =
        parse_storage_options(&["not-a-pair".to_string()]).expect_err("invalid entry should fail");
    assert!(
        err.to_string().contains("KEY=VALUE"),
        "unexpected error: {err}"
    );
}
