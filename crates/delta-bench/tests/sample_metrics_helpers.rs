use delta_bench::results::{
    ContentionMetrics, RuntimeIOMetrics, SampleMetrics, ScanRewriteMetrics,
};

#[test]
fn base_constructor_defaults_scan_and_rewrite_metrics_to_none() {
    let metrics = SampleMetrics::base(Some(11), None, Some(2), Some(5));

    assert_eq!(metrics.rows_processed, Some(11));
    assert_eq!(metrics.bytes_processed, None);
    assert_eq!(metrics.operations, Some(2));
    assert_eq!(metrics.table_version, Some(5));
    assert_eq!(metrics.files_scanned, None);
    assert_eq!(metrics.files_pruned, None);
    assert_eq!(metrics.bytes_scanned, None);
    assert_eq!(metrics.scan_time_ms, None);
    assert_eq!(metrics.rewrite_time_ms, None);
    assert_eq!(metrics.result_hash, None);
    assert_eq!(metrics.schema_hash, None);
}

#[test]
fn scan_rewrite_builder_populates_optional_fields() {
    let metrics = SampleMetrics::base(Some(20), None, Some(1), Some(7)).with_scan_rewrite(
        ScanRewriteMetrics {
            files_scanned: Some(8),
            files_pruned: Some(3),
            bytes_scanned: Some(4096),
            scan_time_ms: Some(14),
            rewrite_time_ms: Some(22),
        },
    );

    assert_eq!(metrics.rows_processed, Some(20));
    assert_eq!(metrics.operations, Some(1));
    assert_eq!(metrics.table_version, Some(7));
    assert_eq!(metrics.files_scanned, Some(8));
    assert_eq!(metrics.files_pruned, Some(3));
    assert_eq!(metrics.bytes_scanned, Some(4096));
    assert_eq!(metrics.scan_time_ms, Some(14));
    assert_eq!(metrics.rewrite_time_ms, Some(22));
}

#[test]
fn runtime_io_builder_populates_result_and_schema_hashes() {
    let metrics = SampleMetrics::base(Some(3), Some(99), Some(1), Some(2)).with_runtime_io(
        RuntimeIOMetrics {
            peak_rss_mb: Some(12),
            cpu_time_ms: Some(23),
            bytes_read: Some(34),
            bytes_written: Some(45),
            files_touched: Some(56),
            files_skipped: Some(67),
            spill_bytes: Some(78),
            result_hash: Some("sha256:result".to_string()),
            schema_hash: Some("sha256:schema".to_string()),
            semantic_state_digest: None,
            validation_summary: None,
        },
    );

    assert_eq!(metrics.result_hash.as_deref(), Some("sha256:result"));
    assert_eq!(metrics.schema_hash.as_deref(), Some("sha256:schema"));
}

#[test]
fn contention_builder_populates_nested_metrics() {
    let metrics = SampleMetrics::base(Some(3), Some(99), Some(1), Some(2)).with_contention(
        ContentionMetrics {
            worker_count: 3,
            race_count: 3,
            ops_attempted: 9,
            ops_succeeded: 6,
            ops_failed: 3,
            conflict_append: 1,
            conflict_delete_read: 1,
            conflict_delete_delete: 1,
            conflict_metadata_changed: 1,
            conflict_protocol_changed: 1,
            conflict_transaction: 1,
            version_already_exists: 1,
            max_commit_attempts_exceeded: 1,
            other_errors: 0,
        },
    );

    let contention = metrics
        .contention
        .as_ref()
        .expect("contention metrics should be present");
    assert_eq!(contention.worker_count, 3);
    assert_eq!(contention.race_count, 3);
    assert_eq!(contention.ops_attempted, 9);
    assert_eq!(contention.ops_succeeded, 6);
    assert_eq!(contention.ops_failed, 3);
    assert_eq!(contention.conflict_append, 1);
    assert_eq!(contention.conflict_delete_read, 1);
    assert_eq!(contention.conflict_delete_delete, 1);
    assert_eq!(contention.conflict_metadata_changed, 1);
    assert_eq!(contention.conflict_protocol_changed, 1);
    assert_eq!(contention.conflict_transaction, 1);
    assert_eq!(contention.version_already_exists, 1);
    assert_eq!(contention.max_commit_attempts_exceeded, 1);
    assert_eq!(contention.other_errors, 0);
}
