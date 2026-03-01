use std::time::Instant;
use std::{future::Future, time::Duration};

use crate::results::{CaseFailure, CaseResult, IterationSample, SampleMetrics};

#[derive(Clone, Debug)]
#[must_use]
pub enum CaseExecutionResult {
    Success(CaseResult),
    Failure(CaseResult),
}

pub fn run_case<F, M, E>(name: &str, warmup: u32, iterations: u32, mut op: F) -> CaseExecutionResult
where
    F: FnMut() -> Result<M, E>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for _ in 0..warmup {
        let _ = op();
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        match op() {
            Ok(metrics) => {
                let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                let metrics = metrics.into();
                samples.push(IterationSample {
                    elapsed_ms,
                    rows: metrics.rows_processed,
                    bytes: metrics.bytes_processed,
                    metrics: Some(metrics),
                });
            }
            Err(e) => {
                let failure = CaseFailure {
                    message: e.to_string(),
                };
                let case = CaseResult {
                    case: name.to_string(),
                    success: false,
                    samples,
                    failure: Some(failure),
                };
                return CaseExecutionResult::Failure(case);
            }
        }
    }

    let case = CaseResult {
        case: name.to_string(),
        success: true,
        samples,
        failure: None,
    };
    CaseExecutionResult::Success(case)
}

pub async fn run_case_async<F, Fut, M, E>(
    name: &str,
    warmup: u32,
    iterations: u32,
    mut op: F,
) -> CaseExecutionResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<M, E>>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for _ in 0..warmup {
        let _ = op().await;
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        match op().await {
            Ok(metrics) => {
                let elapsed: Duration = start.elapsed();
                let metrics = metrics.into();
                samples.push(IterationSample {
                    elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                    rows: metrics.rows_processed,
                    bytes: metrics.bytes_processed,
                    metrics: Some(metrics),
                });
            }
            Err(e) => {
                let case = CaseResult {
                    case: name.to_string(),
                    success: false,
                    samples,
                    failure: Some(CaseFailure {
                        message: e.to_string(),
                    }),
                };
                return CaseExecutionResult::Failure(case);
            }
        }
    }

    CaseExecutionResult::Success(CaseResult {
        case: name.to_string(),
        success: true,
        samples,
        failure: None,
    })
}

pub async fn run_case_async_with_setup<S, SetupF, F, Fut, M, E>(
    name: &str,
    warmup: u32,
    iterations: u32,
    mut setup: SetupF,
    mut op: F,
) -> CaseExecutionResult
where
    SetupF: FnMut() -> Result<S, E>,
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Result<M, E>>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for _ in 0..warmup {
        if let Ok(input) = setup() {
            let _ = op(input).await;
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let input = match setup() {
            Ok(input) => input,
            Err(e) => {
                return CaseExecutionResult::Failure(CaseResult {
                    case: name.to_string(),
                    success: false,
                    samples,
                    failure: Some(CaseFailure {
                        message: e.to_string(),
                    }),
                })
            }
        };

        let start = Instant::now();
        match op(input).await {
            Ok(metrics) => {
                let elapsed: Duration = start.elapsed();
                let metrics = metrics.into();
                samples.push(IterationSample {
                    elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                    rows: metrics.rows_processed,
                    bytes: metrics.bytes_processed,
                    metrics: Some(metrics),
                });
            }
            Err(e) => {
                return CaseExecutionResult::Failure(CaseResult {
                    case: name.to_string(),
                    success: false,
                    samples,
                    failure: Some(CaseFailure {
                        message: e.to_string(),
                    }),
                })
            }
        }
    }

    CaseExecutionResult::Success(CaseResult {
        case: name.to_string(),
        success: true,
        samples,
        failure: None,
    })
}

pub async fn run_case_async_with_async_setup<S, SetupF, SetupFut, F, Fut, M, E>(
    name: &str,
    warmup: u32,
    iterations: u32,
    mut setup: SetupF,
    mut op: F,
) -> CaseExecutionResult
where
    SetupF: FnMut() -> SetupFut,
    SetupFut: Future<Output = Result<S, E>>,
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Result<M, E>>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for _ in 0..warmup {
        if let Ok(input) = setup().await {
            let _ = op(input).await;
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let input = match setup().await {
            Ok(input) => input,
            Err(e) => {
                return CaseExecutionResult::Failure(CaseResult {
                    case: name.to_string(),
                    success: false,
                    samples,
                    failure: Some(CaseFailure {
                        message: e.to_string(),
                    }),
                })
            }
        };

        let start = Instant::now();
        match op(input).await {
            Ok(metrics) => {
                let elapsed: Duration = start.elapsed();
                let metrics = metrics.into();
                samples.push(IterationSample {
                    elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                    rows: metrics.rows_processed,
                    bytes: metrics.bytes_processed,
                    metrics: Some(metrics),
                });
            }
            Err(e) => {
                return CaseExecutionResult::Failure(CaseResult {
                    case: name.to_string(),
                    success: false,
                    samples,
                    failure: Some(CaseFailure {
                        message: e.to_string(),
                    }),
                })
            }
        }
    }

    CaseExecutionResult::Success(CaseResult {
        case: name.to_string(),
        success: true,
        samples,
        failure: None,
    })
}
