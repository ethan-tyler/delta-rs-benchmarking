use delta_bench::results::BenchRunResult;

#[test]
fn legacy_results_without_new_metric_fields_parse() {
    let payload = r#"
{
  "schema_version": 1,
  "context": {
    "schema_version": 1,
    "label": "legacy-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "read_scan",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 1
  },
  "cases": [
    {
      "case": "read_full_scan_narrow",
      "success": true,
      "samples": [
        {
          "elapsed_ms": 4.5,
          "rows": 1,
          "bytes": null,
          "metrics": {
            "rows_processed": 1,
            "bytes_processed": null,
            "operations": null,
            "table_version": null
          }
        }
      ],
      "failure": null
    }
  ]
}
"#;

    let parsed: BenchRunResult = serde_json::from_str(payload).expect("legacy json should parse");
    let metrics = parsed.cases[0].samples[0]
        .metrics
        .as_ref()
        .expect("metrics should be present");

    assert_eq!(metrics.files_scanned, None);
    assert_eq!(metrics.files_pruned, None);
    assert_eq!(metrics.bytes_scanned, None);
    assert_eq!(metrics.scan_time_ms, None);
    assert_eq!(metrics.rewrite_time_ms, None);
}

#[test]
fn results_with_new_metric_fields_parse() {
    let payload = r#"
{
  "schema_version": 1,
  "context": {
    "schema_version": 1,
    "label": "new-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "merge_dml",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 1
  },
  "cases": [
    {
      "case": "merge_upsert_50pct",
      "success": true,
      "samples": [
        {
          "elapsed_ms": 12.3,
          "rows": 100,
          "bytes": null,
          "metrics": {
            "rows_processed": 100,
            "bytes_processed": null,
            "operations": 1,
            "table_version": 2,
            "files_scanned": 7,
            "files_pruned": 3,
            "bytes_scanned": 8192,
            "scan_time_ms": 5,
            "rewrite_time_ms": 6
          }
        }
      ],
      "failure": null
    }
  ]
}
"#;

    let parsed: BenchRunResult = serde_json::from_str(payload).expect("new json should parse");
    let metrics = parsed.cases[0].samples[0]
        .metrics
        .as_ref()
        .expect("metrics should be present");

    assert_eq!(metrics.files_scanned, Some(7));
    assert_eq!(metrics.files_pruned, Some(3));
    assert_eq!(metrics.bytes_scanned, Some(8192));
    assert_eq!(metrics.scan_time_ms, Some(5));
    assert_eq!(metrics.rewrite_time_ms, Some(6));
}

#[test]
fn serde_round_trip_preserves_new_metric_fields() {
    let payload = r#"
{
  "schema_version": 1,
  "context": {
    "schema_version": 1,
    "label": "round-trip-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "merge_dml",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 1
  },
  "cases": [
    {
      "case": "merge_upsert_10pct",
      "success": true,
      "samples": [
        {
          "elapsed_ms": 9.9,
          "rows": 12,
          "bytes": null,
          "metrics": {
            "rows_processed": 12,
            "bytes_processed": null,
            "operations": 1,
            "table_version": 2,
            "files_scanned": 4,
            "files_pruned": 1,
            "bytes_scanned": 4096,
            "scan_time_ms": 4,
            "rewrite_time_ms": 5
          }
        }
      ],
      "failure": null
    }
  ]
}
"#;

    let parsed: BenchRunResult =
        serde_json::from_str(payload).expect("new json should parse before round-trip");
    let serialized = serde_json::to_string(&parsed).expect("serialize round-trip");
    let reparsed: BenchRunResult =
        serde_json::from_str(&serialized).expect("reparse round-trip payload");
    let metrics = reparsed.cases[0].samples[0]
        .metrics
        .as_ref()
        .expect("metrics should be present");

    assert_eq!(metrics.files_scanned, Some(4));
    assert_eq!(metrics.files_pruned, Some(1));
    assert_eq!(metrics.bytes_scanned, Some(4096));
    assert_eq!(metrics.scan_time_ms, Some(4));
    assert_eq!(metrics.rewrite_time_ms, Some(5));
}
