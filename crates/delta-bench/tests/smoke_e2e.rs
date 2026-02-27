use delta_bench::runner::{run_case, CaseExecutionResult};

#[test]
fn runner_records_failures_and_continues() {
    let ok = run_case("ok", 0, 2, || Ok::<u64, String>(10));
    let fail = run_case("fail", 0, 2, || Err::<u64, String>("boom".to_string()));

    assert!(matches!(ok, CaseExecutionResult::Success(_)));
    assert!(matches!(fail, CaseExecutionResult::Failure(_)));
}
