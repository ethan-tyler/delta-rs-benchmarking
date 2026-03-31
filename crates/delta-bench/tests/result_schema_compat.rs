use delta_bench::results::{BenchRunResult, PerfStatus};

#[test]
fn schema_v5_fields_parse_and_round_trip() {
    let payload = r#"
{
  "schema_version": 5,
  "context": {
    "schema_version": 5,
    "label": "v5-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "scan",
    "scale": "sf1",
    "iterations": 5,
    "warmup": 1,
    "timing_phase": "execute",
    "dataset_id": "tiny_smoke",
    "dataset_fingerprint": "sha256:dataset",
    "runner": "rust",
    "storage_backend": "local",
    "benchmark_mode": "perf",
    "lane": "macro",
    "measurement_kind": "phase_breakdown",
    "validation_level": "operational",
    "run_id": "run-123",
    "harness_revision": "harness-sha",
    "fixture_recipe_hash": "sha256:recipe",
    "fidelity_fingerprint": "sha256:fidelity"
  },
  "cases": [
    {
      "case": "scan_full_narrow",
      "success": true,
      "validation_passed": true,
      "perf_status": "trusted",
      "classification": "supported",
      "suite_manifest_hash": "sha256:manifest",
      "case_definition_hash": "sha256:case-def",
      "compatibility_key": "sha256:compat",
      "supports_decision": true,
      "required_runs": 5,
      "decision_threshold_pct": 5.0,
      "decision_metric": "median",
      "run_summary": {
        "sample_count": 5,
        "invalid_sample_count": 0,
        "min_ms": 9.9,
        "max_ms": 12.1,
        "mean_ms": 10.5,
        "median_ms": 10.2,
        "p95_ms": 12.1,
        "host_label": "test-host",
        "fidelity_fingerprint": "sha256:fidelity"
      },
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
            "rewrite_time_ms": 5,
            "peak_rss_mb": 256,
            "cpu_time_ms": 16,
            "bytes_read": 8192,
            "bytes_written": 4096,
            "files_touched": 9,
            "files_skipped": 3,
            "spill_bytes": 0,
            "result_hash": "sha256:def",
            "semantic_state_digest": "sha256:semantic",
            "validation_summary": "rows=12"
          }
        }
      ],
      "elapsed_stats": {
        "min_ms": 9.9,
        "max_ms": 9.9,
        "mean_ms": 9.9,
        "median_ms": 9.9,
        "stddev_ms": 0.0,
        "cv_pct": 0.0
      },
      "failure_kind": null,
      "failure": null
    }
  ]
}
"#;

    let parsed: BenchRunResult = serde_json::from_str(payload).expect("schema v5 should parse");
    assert_eq!(parsed.schema_version, 5);
    assert_eq!(parsed.context.schema_version, 5);
    assert_eq!(parsed.context.benchmark_mode.as_deref(), Some("perf"));
    assert_eq!(parsed.cases[0].perf_status, PerfStatus::Trusted);

    let serialized = serde_json::to_string(&parsed).expect("serialize round-trip");
    let reparsed: BenchRunResult =
        serde_json::from_str(&serialized).expect("reparse round-trip payload");
    assert_eq!(reparsed.schema_version, 5);
    assert_eq!(reparsed.context.schema_version, 5);
    assert_eq!(reparsed.cases[0].perf_status, PerfStatus::Trusted);
}

#[test]
fn schema_v4_payload_is_rejected() {
    let payload = r#"
{
  "schema_version": 4,
  "context": {
    "schema_version": 4,
    "label": "v4-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "scan",
    "scale": "sf1",
    "iterations": 5,
    "warmup": 1
  },
  "cases": []
}
"#;

    let err = serde_json::from_str::<BenchRunResult>(payload).expect_err("v4 must fail");
    assert!(
        err.to_string().contains("schema_version"),
        "unexpected error: {err}"
    );
}

#[test]
fn missing_perf_status_is_rejected() {
    let payload = r#"
{
  "schema_version": 5,
  "context": {
    "schema_version": 5,
    "label": "v5-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "scan",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 1
  },
  "cases": [
    {
      "case": "scan_full_narrow",
      "success": true,
      "validation_passed": true,
      "classification": "supported",
      "samples": [],
      "failure": null
    }
  ]
}
"#;

    let err =
        serde_json::from_str::<BenchRunResult>(payload).expect_err("missing perf_status must fail");
    assert!(
        err.to_string().contains("perf_status"),
        "unexpected error: {err}"
    );
}

#[test]
fn missing_case_classification_is_rejected() {
    let payload = r#"
{
  "schema_version": 5,
  "context": {
    "schema_version": 5,
    "label": "v5-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "merge",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 1
  },
  "cases": [
    {
      "case": "merge_upsert_10pct",
      "success": true,
      "validation_passed": true,
      "perf_status": "trusted",
      "samples": [],
      "failure": null
    }
  ]
}
"#;

    let err = serde_json::from_str::<BenchRunResult>(payload)
        .expect_err("missing classification must fail");
    assert!(
        err.to_string().contains("classification"),
        "unexpected error: {err}"
    );
}

#[test]
fn unknown_case_classification_is_rejected() {
    let payload = r#"
{
  "schema_version": 5,
  "context": {
    "schema_version": 5,
    "label": "v5-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "merge",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 1
  },
  "cases": [
    {
      "case": "merge_upsert_10pct",
      "success": true,
      "validation_passed": true,
      "perf_status": "trusted",
      "classification": "experimental",
      "samples": [],
      "failure": null
    }
  ]
}
"#;

    let err = serde_json::from_str::<BenchRunResult>(payload)
        .expect_err("unknown classification must fail");
    assert!(
        err.to_string().contains("classification"),
        "unexpected error: {err}"
    );
}

#[test]
fn schema_v5_elapsed_stats_parse_when_present() {
    let payload = r#"
{
  "schema_version": 5,
  "context": {
    "schema_version": 5,
    "label": "v5-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "scan",
    "scale": "sf1",
    "iterations": 3,
    "warmup": 1
  },
  "cases": [
    {
      "case": "scan_full_narrow",
      "success": true,
      "validation_passed": true,
      "perf_status": "trusted",
      "classification": "supported",
      "samples": [
        {
          "elapsed_ms": 9.9,
          "rows": 12,
          "bytes": null,
          "metrics": null
        }
      ],
      "elapsed_stats": {
        "min_ms": 9.9,
        "max_ms": 12.1,
        "mean_ms": 10.5,
        "median_ms": 10.2,
        "stddev_ms": 0.9,
        "cv_pct": 8.57
      },
      "failure": null
    }
  ]
}
"#;

    let parsed: BenchRunResult =
        serde_json::from_str(payload).expect("schema v5 with elapsed_stats should parse");
    let elapsed_stats = parsed.cases[0]
        .elapsed_stats
        .as_ref()
        .expect("elapsed_stats should be present");
    assert_eq!(elapsed_stats.min_ms, 9.9);
    assert_eq!(elapsed_stats.max_ms, 12.1);
    assert_eq!(elapsed_stats.mean_ms, 10.5);
    assert_eq!(elapsed_stats.median_ms, 10.2);
    assert_eq!(elapsed_stats.stddev_ms, 0.9);
    assert_eq!(elapsed_stats.cv_pct, Some(8.57));
}

#[test]
fn schema_v2_payload_is_rejected() {
    let payload = r#"
{
  "schema_version": 2,
  "context": {
    "schema_version": 2,
    "label": "v2-run",
    "git_sha": "abc123",
    "created_at": "2026-02-27T22:48:22.208400Z",
    "host": "test-host",
    "suite": "concurrency",
    "scale": "sf1",
    "iterations": 1,
    "warmup": 0
  },
  "cases": [
    {
      "case": "update_vs_compaction",
      "success": true,
      "classification": "supported",
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
            "contention": {
              "worker_count": 2,
              "race_count": 3,
              "ops_attempted": 6,
              "ops_succeeded": 3,
              "ops_failed": 3,
              "conflict_append": 0,
              "conflict_delete_read": 2,
              "conflict_delete_delete": 1,
              "conflict_metadata_changed": 0,
              "conflict_protocol_changed": 0,
              "conflict_transaction": 0,
              "version_already_exists": 0,
              "max_commit_attempts_exceeded": 0,
              "other_errors": 0
            }
          }
        }
      ],
      "failure": null
    }
  ]
}
"#;

    let err = serde_json::from_str::<BenchRunResult>(payload)
        .expect_err("schema v2 payloads should be rejected after the v5 cutover");
    assert!(
        err.to_string().contains("schema_version must be 5"),
        "unexpected error: {err}"
    );
}
