use delta_bench::assertions::{apply_case_assertions, CaseAssertion};
use delta_bench::results::{CaseFailure, CaseResult, IterationSample, SampleMetrics};

fn sample_with_hash(hash: &str, table_version: Option<u64>) -> IterationSample {
    IterationSample {
        elapsed_ms: 1.0,
        rows: Some(1),
        bytes: None,
        metrics: Some(
            SampleMetrics::base(Some(1), None, Some(1), table_version).with_runtime_io_metrics(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(hash.to_string()),
            ),
        ),
    }
}

#[test]
fn expected_error_assertion_reclassifies_failure() {
    let mut case = CaseResult {
        case: "dv_lane".to_string(),
        success: false,
        classification: "supported".to_string(),
        samples: Vec::new(),
        failure: Some(CaseFailure {
            message: "deletion vectors are not supported".to_string(),
        }),
    };

    apply_case_assertions(
        &mut case,
        &[CaseAssertion::ExpectedErrorContains(
            "not supported".to_string(),
        )],
    );

    assert!(case.success);
    assert_eq!(case.classification, "expected_failure");
}

#[test]
fn expected_error_assertion_fails_when_case_unexpectedly_succeeds() {
    let mut case = CaseResult {
        case: "dv_lane".to_string(),
        success: true,
        classification: "supported".to_string(),
        samples: Vec::new(),
        failure: None,
    };

    apply_case_assertions(
        &mut case,
        &[CaseAssertion::ExpectedErrorContains(
            "not supported".to_string(),
        )],
    );

    assert!(!case.success);
    assert_eq!(case.classification, "supported");
    let message = case
        .failure
        .as_ref()
        .map(|f| f.message.as_str())
        .unwrap_or("");
    assert!(message.contains("expected error"));
}

#[test]
fn exact_result_hash_assertion_fails_mismatch() {
    let mut case = CaseResult {
        case: "read_case".to_string(),
        success: true,
        classification: "supported".to_string(),
        samples: vec![sample_with_hash("sha256:abc", Some(1))],
        failure: None,
    };

    apply_case_assertions(
        &mut case,
        &[CaseAssertion::ExactResultHash("sha256:def".to_string())],
    );

    assert!(!case.success);
    let message = case
        .failure
        .as_ref()
        .map(|f| f.message.as_str())
        .unwrap_or("");
    assert!(message.contains("result hash mismatch"));
}

#[test]
fn version_monotonicity_assertion_fails_on_decrease() {
    let mut case = CaseResult {
        case: "version_case".to_string(),
        success: true,
        classification: "supported".to_string(),
        samples: vec![
            sample_with_hash("sha256:abc", Some(2)),
            sample_with_hash("sha256:abc", Some(1)),
        ],
        failure: None,
    };

    apply_case_assertions(&mut case, &[CaseAssertion::VersionMonotonicity]);

    assert!(!case.success);
    let message = case
        .failure
        .as_ref()
        .map(|f| f.message.as_str())
        .unwrap_or("");
    assert!(message.contains("version monotonicity"));
}
