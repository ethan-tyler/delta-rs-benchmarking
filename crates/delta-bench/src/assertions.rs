use crate::results::{CaseFailure, CaseResult, FAILURE_KIND_ASSERTION_MISMATCH};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CaseAssertion {
    ExactResultHash(String),
    SchemaHash(String),
    ExpectedErrorContains(String),
    VersionMonotonicity,
}

pub fn apply_case_assertions(case: &mut CaseResult, assertions: &[CaseAssertion]) {
    for assertion in assertions {
        match assertion {
            CaseAssertion::ExactResultHash(expected) => assert_exact_result_hash(case, expected),
            CaseAssertion::SchemaHash(expected) => assert_schema_hash(case, expected),
            CaseAssertion::ExpectedErrorContains(needle) => {
                assert_expected_error_contains(case, needle)
            }
            CaseAssertion::VersionMonotonicity => assert_version_monotonicity(case),
        }
    }
}

fn assert_exact_result_hash(case: &mut CaseResult, expected: &str) {
    if !case.validation_passed {
        return;
    }
    for (idx, sample) in case.samples.iter().enumerate() {
        let found = sample.metrics.as_ref().and_then(|metrics| {
            metrics
                .semantic_state_digest
                .as_deref()
                .or(metrics.result_hash.as_deref())
        });
        if found != Some(expected) {
            fail_case(
                case,
                format!(
                    "result hash mismatch at sample {}: expected '{expected}', found '{}'",
                    idx + 1,
                    found.unwrap_or("none")
                ),
            );
            return;
        }
    }
}

fn assert_schema_hash(case: &mut CaseResult, expected: &str) {
    if !case.validation_passed {
        return;
    }
    for (idx, sample) in case.samples.iter().enumerate() {
        let found = sample
            .metrics
            .as_ref()
            .and_then(|metrics| metrics.schema_hash.as_deref());
        if found != Some(expected) {
            fail_case(
                case,
                format!(
                    "schema hash mismatch at sample {}: expected '{expected}', found '{}'",
                    idx + 1,
                    found.unwrap_or("none")
                ),
            );
            return;
        }
    }
}

fn assert_expected_error_contains(case: &mut CaseResult, needle: &str) {
    let Some(failure) = case.failure.as_ref() else {
        fail_case(
            case,
            format!(
                "expected error assertion failed: expected case to fail with message containing '{needle}', but case succeeded"
            ),
        );
        return;
    };
    if failure.message.contains(needle) {
        case.success = true;
        case.validation_passed = true;
        case.perf_valid = false;
        case.classification = "expected_failure".to_string();
    }
}

fn assert_version_monotonicity(case: &mut CaseResult) {
    if !case.validation_passed {
        return;
    }
    let mut previous: Option<u64> = None;
    for version in case
        .samples
        .iter()
        .filter_map(|sample| sample.metrics.as_ref())
        .filter_map(|metrics| metrics.table_version)
    {
        if let Some(prev) = previous {
            if version < prev {
                fail_case(
                    case,
                    format!(
                        "version monotonicity assertion failed: table version decreased from {prev} to {version}"
                    ),
                );
                return;
            }
        }
        previous = Some(version);
    }
}

fn fail_case(case: &mut CaseResult, message: String) {
    case.success = false;
    case.validation_passed = false;
    case.perf_valid = false;
    case.elapsed_stats = None;
    case.failure_kind = Some(FAILURE_KIND_ASSERTION_MISMATCH.to_string());
    case.failure = Some(CaseFailure { message });
}
