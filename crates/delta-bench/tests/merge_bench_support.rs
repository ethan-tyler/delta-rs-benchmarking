use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::merge_bench_support::{
    self, MergeFilterEarlyVariant, MergeFilterGeneralizeVariant,
};
use delta_bench::storage::StorageConfig;
use deltalake_core::datafusion::common::{Column, TableReference};
use deltalake_core::datafusion::logical_expr::col;
use deltalake_core::datafusion::logical_expr::expr::Placeholder;
use deltalake_core::datafusion::logical_expr::{Expr, Operator};
use deltalake_core::datafusion::prelude::lit;

fn harness_root() -> std::path::PathBuf {
    let manifest_root = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonicalize crate root");
    if manifest_root.join("bench/methodologies").exists() {
        return manifest_root;
    }

    let parent = manifest_root
        .parent()
        .expect("synced delta-rs checkout should have a parent directory");
    if parent.join("bench/methodologies").exists() {
        return parent.to_path_buf();
    }

    panic!(
        "unable to locate harness root from {}",
        manifest_root.display()
    );
}

#[test]
fn merge_bench_support_generalize_partition_eq_uses_partition_placeholder() {
    let ctx = merge_bench_support::benchmark_generalize_context(
        MergeFilterGeneralizeVariant::PartitionEqSourceTarget,
    );
    let outcome = merge_bench_support::benchmark_generalize_filter(&ctx)
        .expect("partition generalize filter");

    let target = TableReference::parse_str("target");
    let expected = Expr::Placeholder(Placeholder {
        id: "region_0".to_string(),
        field: None,
    })
    .eq(col(Column::new(Some(target), "region")));

    assert_eq!(outcome.filter, expected);
    assert_eq!(outcome.placeholder_aliases, vec!["region_0"]);
    assert_eq!(outcome.aggregate_placeholders, 0);
}

#[test]
fn merge_bench_support_generalize_non_partition_eq_uses_minmax_placeholders() {
    let ctx = merge_bench_support::benchmark_generalize_context(
        MergeFilterGeneralizeVariant::NonPartitionEqMinmax,
    );
    let outcome = merge_bench_support::benchmark_generalize_filter(&ctx)
        .expect("non-partition generalize filter");

    let target = TableReference::parse_str("target");
    let expected = col(Column::new(Some(target), "id")).between(
        Expr::Placeholder(Placeholder {
            id: "id_0_min".to_string(),
            field: None,
        }),
        Expr::Placeholder(Placeholder {
            id: "id_0_max".to_string(),
            field: None,
        }),
    );

    assert_eq!(outcome.filter, expected);
    assert_eq!(outcome.placeholder_aliases, vec!["id_0_min", "id_0_max"]);
    assert_eq!(outcome.aggregate_placeholders, 2);
}

#[tokio::test]
async fn merge_bench_support_early_filter_localized_partition_expansion_is_planning_only() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("fixtures should be generated");

    let ctx = merge_bench_support::benchmark_early_filter_context(
        temp.path(),
        "sf1",
        MergeFilterEarlyVariant::LocalizedPartitionExpansion,
        &storage,
    )
    .await
    .expect("localized merge filter context");
    let outcome = merge_bench_support::benchmark_try_construct_early_filter(&ctx)
        .await
        .expect("localized early filter");

    let mut partition_values = Vec::new();
    collect_partition_literals(&outcome.filter, &mut partition_values);
    let partition_set = partition_values.into_iter().collect::<BTreeSet<_>>();

    assert_eq!(
        partition_set,
        ["apac", "ca", "eu", "latam", "mea", "us"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "partition expansion path should cover every fixture partition"
    );
    assert_eq!(
        partition_set.len(),
        6,
        "partition expansion path should fan out to all six fixture partitions"
    );
    assert!(
        !outcome.filter.to_string().contains("source."),
        "planning helper should not leak source references into the target filter"
    );
}

#[tokio::test]
async fn merge_bench_support_early_filter_mixed_partition_and_stats_combines_both_paths() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("fixtures should be generated");

    let ctx = merge_bench_support::benchmark_early_filter_context(
        temp.path(),
        "sf1",
        MergeFilterEarlyVariant::MixedPartitionAndStats,
        &storage,
    )
    .await
    .expect("mixed merge filter context");
    let outcome = merge_bench_support::benchmark_try_construct_early_filter(&ctx)
        .await
        .expect("mixed early filter");

    let rendered = outcome.filter.to_string();
    assert!(
        rendered.contains("target.id BETWEEN"),
        "mixed filter should retain the target range predicate"
    );
    assert!(
        rendered.contains("target.region"),
        "mixed filter should retain the partition predicate"
    );
    assert!(
        !rendered.contains("source."),
        "mixed filter should be generalized away from source references"
    );
}

#[tokio::test]
async fn merge_bench_support_streaming_source_skips_dynamic_expansion() {
    let temp = tempfile::tempdir().expect("tempdir should be created");
    let storage = StorageConfig::local();

    generate_fixtures(temp.path(), "sf1", 42, true, &storage)
        .await
        .expect("fixtures should be generated");

    let ctx = merge_bench_support::benchmark_early_filter_context(
        temp.path(),
        "sf1",
        MergeFilterEarlyVariant::StreamingSource,
        &storage,
    )
    .await
    .expect("streaming merge filter context");
    let outcome = merge_bench_support::benchmark_try_construct_early_filter(&ctx)
        .await
        .expect("streaming early filter");

    let target = TableReference::parse_str("target");
    let expected = col(Column::new(Some(target), "region")).eq(lit("us"));

    assert_eq!(outcome.filter, expected);
    assert!(
        !outcome.filter.to_string().contains("BETWEEN"),
        "streaming source path should skip dynamic min/max expansion"
    );
}

#[test]
fn merge_bench_support_profile_and_bench_are_criterion_only() {
    let root = harness_root();
    let profile = root.join("bench/methodologies/merge-filter-criterion.env");
    let bench = root.join("crates/delta-bench/benches/merge_filter_bench.rs");

    let profile_text = fs::read_to_string(&profile).expect("read merge-filter criterion env");
    assert!(
        profile_text.contains("PROFILE_KIND=criterion"),
        "merge-filter profile must stay criterion-only"
    );
    assert!(
        profile_text.contains("CRITERION_BENCH=merge_filter_bench"),
        "merge-filter profile must target the dedicated criterion bench"
    );

    let bench_text = fs::read_to_string(&bench).expect("read merge filter bench source");
    for expected in [
        "merge_filter/generalize",
        "partition_eq_source_target",
        "non_partition_eq_minmax",
        "merge_filter/early_filter",
        "measurement_time(Duration::from_secs(8))",
        "localized_partition_expansion",
        "mixed_partition_and_stats",
        "streaming_source",
        "criterion_group!",
        "criterion_main!",
    ] {
        assert!(
            bench_text.contains(expected),
            "merge filter bench missing {expected}"
        );
    }
}

#[test]
fn merge_bench_support_copied_merge_filter_logic_stays_in_sync_with_upstream() {
    let root = harness_root();
    let local = root.join("crates/delta-bench/src/merge_bench_support.rs");
    let upstream = deltalake_core_merge_filter_path(&root);
    let local_text = fs::read_to_string(&local).expect("read local merge bench support source");
    let upstream_text =
        fs::read_to_string(&upstream).expect("read upstream delta-rs merge filter source");

    for item in [
        "enum ReferenceTableCheck",
        "impl ReferenceTableCheck",
        "struct PredicatePlaceholder",
        "fn references_table(",
        "fn construct_placeholder(",
        "fn replace_placeholders(",
        "fn generalize_filter(",
        "async fn try_construct_early_filter(",
        "async fn execute_plan_to_batch(",
    ] {
        let local_snippet = extract_rust_item(&local_text, item);
        let upstream_snippet = extract_rust_item(&upstream_text, item);

        assert_eq!(
            normalize_merge_filter_copy(&local_snippet),
            normalize_merge_filter_copy(&upstream_snippet),
            "{item} drifted from deltalake-core merge filter logic"
        );
    }
}

fn collect_partition_literals(expr: &Expr, values: &mut Vec<String>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            collect_partition_literals(&binary.left, values);
            collect_partition_literals(&binary.right, values);
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            if let Expr::Literal(value, _) = &*binary.left {
                values.push(value.to_string());
            }
        }
        _ => {}
    }
}

fn deltalake_core_merge_filter_path(repo_root: &Path) -> PathBuf {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .current_dir(repo_root)
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse cargo metadata json");
    let manifest_path = metadata["packages"]
        .as_array()
        .expect("cargo metadata packages array")
        .iter()
        .find(|package| package["name"].as_str() == Some("deltalake-core"))
        .and_then(|package| package["manifest_path"].as_str())
        .map(PathBuf::from)
        .expect("locate deltalake-core manifest path");

    manifest_path
        .parent()
        .expect("deltalake-core manifest parent")
        .join("src/operations/merge/filter.rs")
}

fn extract_rust_item(source: &str, marker: &str) -> String {
    let start = source
        .find(marker)
        .unwrap_or_else(|| panic!("missing marker {marker}"));
    let body_start = source[start..]
        .find('{')
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("missing opening brace for {marker}"));

    let mut depth = 0usize;
    let mut end = None;
    for (offset, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(body_start + offset + 1);
                    break;
                }
            }
            _ => {}
        }
    }

    source[start..end.expect("find closing brace")].to_string()
}

fn normalize_merge_filter_copy(snippet: &str) -> String {
    let normalized: String = snippet
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<String>()
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect();

    normalized
        .replace("BenchResult", "DeltaResult")
        .replace("BenchError", "DeltaTableError")
        .replace(
            "deltalake_core::arrow::record_batch::RecordBatch",
            "arrow::record_batch::RecordBatch",
        )
        .replace("=>{matchbinary.op{", "=>matchbinary.op{")
        .replace("}}(Some(l),Some(r))", "},(Some(l),Some(r))")
}
