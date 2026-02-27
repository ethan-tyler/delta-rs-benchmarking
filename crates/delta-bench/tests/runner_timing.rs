use std::thread;
use std::time::Duration;

use delta_bench::runner::{
    run_case_async_with_async_setup, run_case_async_with_setup, CaseExecutionResult,
};

#[tokio::test]
async fn setup_delay_is_not_counted_in_iteration_elapsed_time() {
    let result = run_case_async_with_setup(
        "timing_case",
        0,
        1,
        || -> Result<(), String> {
            thread::sleep(Duration::from_millis(25));
            Ok(())
        },
        |_| async { Ok::<u64, String>(1) },
    )
    .await;

    let case = match result {
        CaseExecutionResult::Success(case) => case,
        CaseExecutionResult::Failure(case) => panic!("unexpected failure: {:?}", case.failure),
    };
    assert_eq!(case.samples.len(), 1);
    assert!(
        case.samples[0].elapsed_ms < 10.0,
        "setup delay leaked into measured time: {}",
        case.samples[0].elapsed_ms
    );
}

#[tokio::test]
async fn async_setup_delay_is_not_counted_in_iteration_elapsed_time() {
    let result = run_case_async_with_async_setup(
        "timing_case_async_setup",
        0,
        1,
        || async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok::<(), String>(())
        },
        |_| async { Ok::<u64, String>(1) },
    )
    .await;

    let case = match result {
        CaseExecutionResult::Success(case) => case,
        CaseExecutionResult::Failure(case) => panic!("unexpected failure: {:?}", case.failure),
    };
    assert_eq!(case.samples.len(), 1);
    assert!(
        case.samples[0].elapsed_ms < 10.0,
        "async setup delay leaked into measured time: {}",
        case.samples[0].elapsed_ms
    );
}
