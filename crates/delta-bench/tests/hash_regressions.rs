use std::fs;
use std::path::Path;

use delta_bench::manifests::{load_manifest, ManifestAssertion};

fn repo_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn tpcds_q07_sql_has_deterministic_tie_breaker() {
    let sql_path = repo_root().join("crates/delta-bench/src/suites/tpcds/sql/q07.sql");
    let sql = fs::read_to_string(&sql_path).expect("read q07.sql");
    let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized.contains("ORDER BY sale_count DESC") && normalized.contains("ss_item_sk ASC"),
        "q07 must include deterministic secondary ordering by ss_item_sk for ties; sql={normalized}"
    );
}

#[test]
fn core_rust_manifest_uses_refreshed_regression_hashes() {
    let path = repo_root().join("bench/manifests/core_rust.yaml");
    let manifest = load_manifest(&path).expect("load core rust manifest");

    fn exact_result_hash_for(
        manifest: &delta_bench::manifests::BenchmarkManifest,
        id: &str,
    ) -> String {
        let case = manifest
            .cases
            .iter()
            .find(|case| case.id == id)
            .unwrap_or_else(|| panic!("missing case in manifest: {id}"));
        let assertion = case
            .assertions
            .iter()
            .find_map(|assertion| {
                if let ManifestAssertion::ExactResultHash { value } = assertion {
                    Some(value.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("missing exact_result_hash assertion for {id}"));
        assertion
    }

    assert_eq!(
        exact_result_hash_for(&manifest, "scan_pruning_miss"),
        "sha256:4d1fd8f90ce4d7480edaf3af4d3716d84d10b0f77211789e47bbe1b06ad8e138"
    );
    assert_eq!(
        exact_result_hash_for(&manifest, "update_all_rows_expr"),
        "sha256:6246db1cb0db81f5da5430ec2e67f808a2aaba1514d69e558785ac3bdf9160e9"
    );
    assert_eq!(
        exact_result_hash_for(&manifest, "metadata_load"),
        "sha256:5ab9790136cccb6f14961db20191b148f33dfa2134072075022ac619c95a7195"
    );
    assert_eq!(
        exact_result_hash_for(&manifest, "tpcds_q07"),
        "sha256:d3cf7184a65e170c462bae58773eb0cc2d77abf7c7d260b515aaeb28ad4be70a"
    );
}
