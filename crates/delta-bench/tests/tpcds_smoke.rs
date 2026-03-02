#[path = "support/tpcds_fixture.rs"]
mod support;

use chrono::Utc;
use delta_bench::results::{BenchContext, BenchRunResult};
use delta_bench::storage::StorageConfig;
use delta_bench::suites::run_target;

#[tokio::test]
async fn tpcds_smoke_produces_deterministic_case_names_and_json_shape() {
    let temp = tempfile::tempdir().expect("tempdir");
    support::write_store_sales_fixture(temp.path(), "sf1").await;
    let storage = StorageConfig::local();

    let cases = run_target(temp.path(), "tpcds", "sf1", 0, 1, &storage)
        .await
        .expect("run tpcds target");

    let case_names = cases
        .iter()
        .map(|case| case.case.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        case_names,
        vec![
            "tpcds_q03".to_string(),
            "tpcds_q07".to_string(),
            "tpcds_q64".to_string(),
            "tpcds_q72".to_string(),
        ]
    );

    let output = BenchRunResult {
        schema_version: 2,
        context: BenchContext {
            schema_version: 2,
            label: "smoke".to_string(),
            git_sha: Some("deadbeef".to_string()),
            created_at: Utc::now(),
            host: "localhost".to_string(),
            suite: "tpcds".to_string(),
            scale: "sf1".to_string(),
            iterations: 1,
            warmup: 0,
            dataset_id: None,
            dataset_fingerprint: None,
            runner: None,
            backend_profile: None,
            image_version: None,
            hardening_profile_id: None,
            hardening_profile_sha256: None,
            cpu_model: None,
            cpu_microcode: None,
            kernel: None,
            boot_params: None,
            cpu_steal_pct: None,
            numa_topology: None,
            egress_policy_sha256: None,
            run_mode: None,
            maintenance_window_id: None,
        },
        cases,
    };

    let value = serde_json::to_value(output).expect("serialize smoke output");
    let serialized_cases = value["cases"].as_array().expect("cases array");
    assert_eq!(serialized_cases.len(), 4);
    assert_eq!(serialized_cases[0]["case"], "tpcds_q03");
    assert_eq!(serialized_cases[3]["case"], "tpcds_q72");
}
