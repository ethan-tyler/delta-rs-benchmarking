use delta_bench::results::SampleMetrics;

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
}

#[test]
fn scan_rewrite_builder_populates_optional_fields() {
    let metrics = SampleMetrics::base(Some(20), None, Some(1), Some(7)).with_scan_rewrite_metrics(
        Some(8),
        Some(3),
        Some(4096),
        Some(14),
        Some(22),
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
