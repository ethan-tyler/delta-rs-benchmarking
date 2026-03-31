use std::time::Instant;
use std::{future::Future, time::Duration};

pub use crate::cli::TimingPhase;
use crate::results::{
    build_run_summary, CaseFailure, CaseResult, ElapsedStats, IterationSample, PerfStatus,
    SampleMetrics, FAILURE_KIND_EXECUTION_ERROR, FAILURE_KIND_UNSUPPORTED,
};
use crate::stats::compute_stats;

#[derive(Clone, Debug)]
#[must_use]
pub enum CaseExecutionResult {
    Success(CaseResult),
    Failure(CaseResult),
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PhaseTiming {
    load_ms: Option<f64>,
    plan_ms: Option<f64>,
    execute_ms: Option<f64>,
    validate_ms: Option<f64>,
}

impl PhaseTiming {
    pub fn with_load_ms(mut self, load_ms: f64) -> Self {
        self.load_ms = Some(load_ms);
        self
    }

    pub fn with_plan_ms(mut self, plan_ms: f64) -> Self {
        self.plan_ms = Some(plan_ms);
        self
    }

    pub fn with_execute_ms(mut self, execute_ms: f64) -> Self {
        self.execute_ms = Some(execute_ms);
        self
    }

    pub fn with_validate_ms(mut self, validate_ms: f64) -> Self {
        self.validate_ms = Some(validate_ms);
        self
    }

    fn elapsed_ms_for(self, phase: TimingPhase) -> Option<f64> {
        match phase {
            TimingPhase::Load => self.load_ms,
            TimingPhase::Plan => self.plan_ms,
            TimingPhase::Execute => self.execute_ms,
            TimingPhase::Validate => self.validate_ms,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimedSample<M> {
    pub metrics: M,
    pub timing: PhaseTiming,
}

impl<M> TimedSample<M> {
    pub fn new(metrics: M, timing: PhaseTiming) -> Self {
        Self { metrics, timing }
    }
}

pub fn run_case<F, M, E>(name: &str, warmup: u32, iterations: u32, mut op: F) -> CaseExecutionResult
where
    F: FnMut() -> Result<M, E>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for warmup_idx in 0..warmup {
        if let Err(error) = op() {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
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
                let case = failure_case_result(name, samples, failure.message);
                return CaseExecutionResult::Failure(case);
            }
        }
    }

    let case = success_case_result(name, samples);
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
    for warmup_idx in 0..warmup {
        if let Err(error) = op().await {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        match op().await {
            Ok(metrics) => {
                append_sample(&mut samples, start.elapsed(), metrics, None);
            }
            Err(e) => {
                let case = failure_case_result(name, samples, e.to_string());
                return CaseExecutionResult::Failure(case);
            }
        }
    }

    CaseExecutionResult::Success(success_case_result(name, samples))
}

pub async fn run_case_async_with_timing_phase<F, Fut, M, E>(
    name: &str,
    warmup: u32,
    iterations: u32,
    timing_phase: TimingPhase,
    mut op: F,
) -> CaseExecutionResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<TimedSample<M>, E>>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for warmup_idx in 0..warmup {
        if let Err(error) = op().await {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        match op().await {
            Ok(sample) => {
                let Some(elapsed_ms) = sample.timing.elapsed_ms_for(timing_phase) else {
                    return CaseExecutionResult::Failure(unsupported_case_result(
                        name,
                        samples,
                        format!(
                            "requested timing phase '{}' is unavailable for this case",
                            timing_phase.as_str()
                        ),
                    ));
                };
                append_sample(
                    &mut samples,
                    Duration::from_secs(0),
                    sample.metrics,
                    Some(elapsed_ms),
                );
            }
            Err(e) => {
                let case = failure_case_result(name, samples, e.to_string());
                return CaseExecutionResult::Failure(case);
            }
        }
    }

    CaseExecutionResult::Success(success_case_result(name, samples))
}

pub async fn run_case_async_custom_timing<F, Fut, M, E>(
    name: &str,
    warmup: u32,
    iterations: u32,
    mut op: F,
) -> CaseExecutionResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(M, Option<f64>), E>>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for warmup_idx in 0..warmup {
        if let Err(error) = op().await {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let start = Instant::now();
        match op().await {
            Ok((metrics, elapsed_ms_override)) => {
                append_sample(&mut samples, start.elapsed(), metrics, elapsed_ms_override);
            }
            Err(e) => {
                let case = failure_case_result(name, samples, e.to_string());
                return CaseExecutionResult::Failure(case);
            }
        }
    }

    CaseExecutionResult::Success(success_case_result(name, samples))
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
    for warmup_idx in 0..warmup {
        let input = match setup() {
            Ok(input) => input,
            Err(error) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    Vec::new(),
                    format!(
                        "warmup setup iteration {} failed: {}",
                        warmup_idx + 1,
                        error.to_string()
                    ),
                ))
            }
        };
        if let Err(error) = op(input).await {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let input = match setup() {
            Ok(input) => input,
            Err(e) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    samples,
                    e.to_string(),
                ))
            }
        };

        let start = Instant::now();
        match op(input).await {
            Ok(metrics) => {
                append_sample(&mut samples, start.elapsed(), metrics, None);
            }
            Err(e) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    samples,
                    e.to_string(),
                ))
            }
        }
    }

    CaseExecutionResult::Success(success_case_result(name, samples))
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
    for warmup_idx in 0..warmup {
        let input = match setup().await {
            Ok(input) => input,
            Err(error) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    Vec::new(),
                    format!(
                        "warmup setup iteration {} failed: {}",
                        warmup_idx + 1,
                        error.to_string()
                    ),
                ))
            }
        };
        if let Err(error) = op(input).await {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let input = match setup().await {
            Ok(input) => input,
            Err(e) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    samples,
                    e.to_string(),
                ))
            }
        };

        let start = Instant::now();
        match op(input).await {
            Ok(metrics) => {
                append_sample(&mut samples, start.elapsed(), metrics, None);
            }
            Err(e) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    samples,
                    e.to_string(),
                ))
            }
        }
    }

    CaseExecutionResult::Success(success_case_result(name, samples))
}

pub async fn run_case_async_with_async_setup_custom_timing<S, SetupF, SetupFut, F, Fut, M, E>(
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
    Fut: Future<Output = Result<(M, Option<f64>), E>>,
    M: Into<SampleMetrics>,
    E: ToString,
{
    for warmup_idx in 0..warmup {
        let input = match setup().await {
            Ok(input) => input,
            Err(error) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    Vec::new(),
                    format!(
                        "warmup setup iteration {} failed: {}",
                        warmup_idx + 1,
                        error.to_string()
                    ),
                ))
            }
        };
        if let Err(error) = op(input).await {
            return CaseExecutionResult::Failure(failure_case_result(
                name,
                Vec::new(),
                format!(
                    "warmup iteration {} failed: {}",
                    warmup_idx + 1,
                    error.to_string()
                ),
            ));
        }
    }

    let mut samples = Vec::new();
    for _ in 0..iterations {
        let input = match setup().await {
            Ok(input) => input,
            Err(e) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    samples,
                    e.to_string(),
                ))
            }
        };

        let start = Instant::now();
        match op(input).await {
            Ok((metrics, elapsed_ms_override)) => {
                append_sample(&mut samples, start.elapsed(), metrics, elapsed_ms_override);
            }
            Err(e) => {
                return CaseExecutionResult::Failure(failure_case_result(
                    name,
                    samples,
                    e.to_string(),
                ))
            }
        }
    }

    CaseExecutionResult::Success(success_case_result(name, samples))
}

fn append_sample<M>(
    samples: &mut Vec<IterationSample>,
    elapsed: Duration,
    metrics: M,
    elapsed_ms_override: Option<f64>,
) where
    M: Into<SampleMetrics>,
{
    let metrics = metrics.into();
    samples.push(IterationSample {
        elapsed_ms: elapsed_ms_override.unwrap_or(elapsed.as_secs_f64() * 1000.0),
        rows: metrics.rows_processed,
        bytes: metrics.bytes_processed,
        metrics: Some(metrics),
    });
}

fn success_case_result(name: &str, samples: Vec<IterationSample>) -> CaseResult {
    let run_summary = build_run_summary(&samples, None, None);
    CaseResult {
        case: name.to_string(),
        success: true,
        validation_passed: true,
        perf_status: PerfStatus::Trusted,
        classification: "supported".to_string(),
        elapsed_stats: elapsed_stats_from_samples(&samples),
        run_summary: Some(run_summary),
        run_summaries: None,
        suite_manifest_hash: None,
        case_definition_hash: None,
        compatibility_key: None,
        supports_decision: None,
        required_runs: None,
        decision_threshold_pct: None,
        decision_metric: None,
        samples,
        failure_kind: None,
        failure: None,
    }
}

fn failure_case_result(name: &str, samples: Vec<IterationSample>, message: String) -> CaseResult {
    CaseResult {
        case: name.to_string(),
        success: false,
        validation_passed: false,
        perf_status: PerfStatus::Invalid,
        classification: "supported".to_string(),
        elapsed_stats: None,
        run_summary: Some(build_run_summary(&samples, None, None)),
        run_summaries: None,
        suite_manifest_hash: None,
        case_definition_hash: None,
        compatibility_key: None,
        supports_decision: None,
        required_runs: None,
        decision_threshold_pct: None,
        decision_metric: None,
        samples,
        failure_kind: Some(FAILURE_KIND_EXECUTION_ERROR.to_string()),
        failure: Some(CaseFailure { message }),
    }
}

fn unsupported_case_result(
    name: &str,
    samples: Vec<IterationSample>,
    message: String,
) -> CaseResult {
    CaseResult {
        case: name.to_string(),
        success: false,
        validation_passed: false,
        perf_status: PerfStatus::Invalid,
        classification: "supported".to_string(),
        elapsed_stats: None,
        run_summary: Some(build_run_summary(&samples, None, None)),
        run_summaries: None,
        suite_manifest_hash: None,
        case_definition_hash: None,
        compatibility_key: None,
        supports_decision: None,
        required_runs: None,
        decision_threshold_pct: None,
        decision_metric: None,
        samples,
        failure_kind: Some(FAILURE_KIND_UNSUPPORTED.to_string()),
        failure: Some(CaseFailure { message }),
    }
}

fn elapsed_stats_from_samples(samples: &[IterationSample]) -> Option<ElapsedStats> {
    let elapsed = samples
        .iter()
        .map(|sample| sample.elapsed_ms)
        .collect::<Vec<_>>();
    let stats = compute_stats(&elapsed)?;
    Some(ElapsedStats {
        min_ms: stats.min_ms,
        max_ms: stats.max_ms,
        mean_ms: stats.mean_ms,
        median_ms: stats.median_ms,
        stddev_ms: stats.stddev_ms,
        cv_pct: stats.cv_pct,
    })
}
