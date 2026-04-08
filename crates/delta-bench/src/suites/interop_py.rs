use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use serde_json::Value;

use crate::cli::BenchmarkLane;
use crate::error::{BenchError, BenchResult};
use crate::results::{
    validate_case_classification, CaseFailure, CaseResult, ElapsedStats, IterationSample,
    PerfStatus, RuntimeIOMetrics, SampleMetrics, FAILURE_KIND_EXECUTION_ERROR,
};
use crate::stats::compute_stats;
use crate::storage::StorageConfig;
use crate::system::PYTHON_INTEROP_REQUIRED_MODULES;
use crate::validation::lane_requires_semantic_validation;

const CASES: [&str; 3] = [
    "pandas_roundtrip_smoke",
    "polars_roundtrip_smoke",
    "pyarrow_dataset_scan_perf",
];
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_RETRIES: u32 = 1;
const INTEROP_AUDIT_REQUIREMENTS_RELATIVE_PATH: &str = "python/requirements-audit.txt";

#[derive(Debug, Deserialize)]
struct InteropCaseOutput {
    #[serde(default)]
    rows_processed: Option<u64>,
    #[serde(default)]
    bytes_processed: Option<u64>,
    #[serde(default)]
    operations: Option<u64>,
    #[serde(default)]
    table_version: Option<u64>,
    #[serde(default)]
    peak_rss_mb: Option<u64>,
    #[serde(default)]
    cpu_time_ms: Option<u64>,
    #[serde(default)]
    bytes_read: Option<u64>,
    #[serde(default)]
    bytes_written: Option<u64>,
    #[serde(default)]
    files_touched: Option<u64>,
    #[serde(default)]
    files_skipped: Option<u64>,
    #[serde(default)]
    spill_bytes: Option<u64>,
    #[serde(default)]
    result_hash: Option<String>,
    #[serde(default)]
    schema_hash: Option<String>,
    #[serde(default)]
    semantic_state_digest: Option<String>,
    #[serde(default)]
    validation_summary: Option<String>,
    #[serde(default)]
    elapsed_ms: Option<f64>,
    classification: String,
}

#[derive(Clone, Debug)]
struct InteropRuntimeConfig {
    timeout: Duration,
    retries: u32,
    python_executable: String,
}

impl InteropRuntimeConfig {
    fn from_env() -> BenchResult<Self> {
        let timeout_ms = parse_env_u64("DELTA_BENCH_INTEROP_TIMEOUT_MS", DEFAULT_TIMEOUT_MS)?;
        if timeout_ms == 0 {
            return Err(BenchError::InvalidArgument(
                "DELTA_BENCH_INTEROP_TIMEOUT_MS must be > 0".to_string(),
            ));
        }
        let retries = parse_env_u64("DELTA_BENCH_INTEROP_RETRIES", DEFAULT_RETRIES as u64)?;
        if retries > u32::MAX as u64 {
            return Err(BenchError::InvalidArgument(format!(
                "DELTA_BENCH_INTEROP_RETRIES is too large: {retries}"
            )));
        }

        let python_executable = std::env::var("DELTA_BENCH_INTEROP_PYTHON")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "python3".to_string());

        Ok(Self {
            timeout: Duration::from_millis(timeout_ms),
            retries: retries as u32,
            python_executable,
        })
    }
}

#[derive(Debug)]
struct PythonModuleVersionProbeResult {
    versions: BTreeMap<String, Option<String>>,
    probe_error: Option<String>,
}

fn parse_env_u64(name: &str, default: u64) -> BenchResult<u64> {
    let Some(raw) = std::env::var(name).ok() else {
        return Ok(default);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    trimmed.parse::<u64>().map_err(|error| {
        BenchError::InvalidArgument(format!("invalid value for {name}='{trimmed}': {error}"))
    })
}

pub fn case_names() -> Vec<String> {
    CASES.iter().map(|case| (*case).to_string()).collect()
}

pub async fn run(
    fixtures_dir: &Path,
    scale: &str,
    lane: BenchmarkLane,
    warmup: u32,
    iterations: u32,
    storage: &StorageConfig,
) -> BenchResult<Vec<CaseResult>> {
    if !storage.is_local() {
        return Ok(case_names()
            .into_iter()
            .map(|case| CaseResult {
                case,
                success: true,
                validation_passed: true,
                perf_status: PerfStatus::ValidationOnly,
                classification: "expected_failure".to_string(),
                samples: Vec::new(),
                elapsed_stats: None,
                run_summary: None,
                run_summaries: None,
                suite_manifest_hash: None,
                case_definition_hash: None,
                compatibility_key: None,
                supports_decision: None,
                required_runs: None,
                decision_threshold_pct: None,
                decision_metric: None,
                failure_kind: Some(FAILURE_KIND_EXECUTION_ERROR.to_string()),
                failure: Some(CaseFailure {
                    message: "interop_py currently supports local backend only in P0".to_string(),
                }),
            })
            .collect());
    }

    let runtime = InteropRuntimeConfig::from_env()?;
    if lane_requires_semantic_validation(lane) {
        if let Some(message) = interop_dependency_version_mismatch(&runtime)? {
            return Ok(interop_dependency_mismatch_results(&message));
        }
    }
    let mut out = Vec::new();
    for case in CASES {
        out.push(
            run_case(
                case,
                fixtures_dir,
                scale,
                lane,
                warmup,
                iterations,
                &runtime,
            )
            .await?,
        );
    }
    Ok(out)
}

fn interop_dependency_mismatch_results(message: &str) -> Vec<CaseResult> {
    case_names()
        .into_iter()
        .map(|case| CaseResult {
            case,
            success: false,
            validation_passed: false,
            perf_status: PerfStatus::Invalid,
            classification: "supported".to_string(),
            samples: Vec::new(),
            elapsed_stats: None,
            run_summary: None,
            run_summaries: None,
            suite_manifest_hash: None,
            case_definition_hash: None,
            compatibility_key: None,
            supports_decision: None,
            required_runs: None,
            decision_threshold_pct: None,
            decision_metric: None,
            failure_kind: Some(FAILURE_KIND_EXECUTION_ERROR.to_string()),
            failure: Some(CaseFailure {
                message: message.to_string(),
            }),
        })
        .collect()
}

fn interop_dependency_version_mismatch(
    runtime: &InteropRuntimeConfig,
) -> BenchResult<Option<String>> {
    let requirements_path = interop_audit_requirements_path();
    let expected_versions = load_expected_interop_versions(&requirements_path)?;
    let probe =
        probe_python_module_versions(&runtime.python_executable, &PYTHON_INTEROP_REQUIRED_MODULES);
    if let Some(error) = probe.probe_error {
        return Ok(Some(format!(
            "python interop dependency version mismatch: failed to probe '{}' ({error}); exact semantic assertions require versions from {}",
            runtime.python_executable,
            requirements_path.display()
        )));
    }

    let mismatches = PYTHON_INTEROP_REQUIRED_MODULES
        .iter()
        .filter_map(|module| {
            let expected = expected_versions.get(*module)?;
            let found = probe
                .versions
                .get(*module)
                .and_then(|value| value.as_deref());
            if found == Some(expected.as_str()) {
                None
            } else {
                Some(format!(
                    "{} expected {}, found {}",
                    module,
                    expected,
                    found.unwrap_or("missing")
                ))
            }
        })
        .collect::<Vec<_>>();
    if mismatches.is_empty() {
        Ok(None)
    } else {
        Ok(Some(format!(
            "python interop dependency version mismatch: {}; exact semantic assertions require versions from {}",
            mismatches.join("; "),
            requirements_path.display()
        )))
    }
}

fn interop_audit_requirements_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(INTEROP_AUDIT_REQUIREMENTS_RELATIVE_PATH)
}

fn load_expected_interop_versions(path: &Path) -> BenchResult<BTreeMap<String, String>> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        BenchError::InvalidArgument(format!(
            "failed to read python interop requirements at {}: {error}",
            path.display()
        ))
    })?;
    let mut versions = BTreeMap::new();
    for raw_line in content.lines() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let Some((name, version)) = line.split_once("==") else {
            continue;
        };
        let name = name.trim();
        if PYTHON_INTEROP_REQUIRED_MODULES.contains(&name) {
            versions.insert(name.to_string(), version.trim().to_string());
        }
    }
    for module in PYTHON_INTEROP_REQUIRED_MODULES {
        if !versions.contains_key(module) {
            return Err(BenchError::InvalidArgument(format!(
                "python interop requirements file {} is missing pinned version for {}",
                path.display(),
                module
            )));
        }
    }
    Ok(versions)
}

fn probe_python_module_versions(
    python_executable: &str,
    modules: &[&str],
) -> PythonModuleVersionProbeResult {
    if modules.is_empty() {
        return PythonModuleVersionProbeResult {
            versions: BTreeMap::new(),
            probe_error: None,
        };
    }

    const PROBE_SCRIPT: &str = r#"
import importlib
import importlib.util
import json
import sys

out = {}
for name in sys.argv[1:]:
    spec = importlib.util.find_spec(name)
    if spec is None:
        out[name] = None
        continue
    module = importlib.import_module(name)
    out[name] = getattr(module, "__version__", None)
print(json.dumps(out, sort_keys=True))
"#;

    let output = match std::process::Command::new(python_executable)
        .arg("-c")
        .arg(PROBE_SCRIPT)
        .args(modules)
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            return PythonModuleVersionProbeResult {
                versions: BTreeMap::new(),
                probe_error: Some(format!("failed to execute '{python_executable}': {error}")),
            };
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let message = if stderr.is_empty() {
            format!("'{python_executable}' exited with status {}", output.status)
        } else {
            format!(
                "'{python_executable}' exited with status {}: {stderr}",
                output.status
            )
        };
        return PythonModuleVersionProbeResult {
            versions: BTreeMap::new(),
            probe_error: Some(message),
        };
    }

    let parsed = match serde_json::from_slice::<Value>(&output.stdout) {
        Ok(value) => value,
        Err(error) => {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let snippet = if stdout.is_empty() {
                "empty stdout".to_string()
            } else {
                format!("stdout='{stdout}'")
            };
            return PythonModuleVersionProbeResult {
                versions: BTreeMap::new(),
                probe_error: Some(format!(
                    "failed to parse version probe output from '{python_executable}': {error} ({snippet})"
                )),
            };
        }
    };

    let Some(object) = parsed.as_object() else {
        return PythonModuleVersionProbeResult {
            versions: BTreeMap::new(),
            probe_error: Some(format!(
                "invalid version probe output from '{python_executable}': expected JSON object"
            )),
        };
    };

    let versions = modules
        .iter()
        .map(|module| {
            let value = object
                .get(*module)
                .and_then(|entry| entry.as_str())
                .map(|entry| entry.to_string());
            ((*module).to_string(), value)
        })
        .collect::<BTreeMap<_, _>>();

    PythonModuleVersionProbeResult {
        versions,
        probe_error: None,
    }
}

async fn run_case(
    case: &str,
    fixtures_dir: &Path,
    scale: &str,
    lane: BenchmarkLane,
    warmup: u32,
    iterations: u32,
    runtime: &InteropRuntimeConfig,
) -> BenchResult<CaseResult> {
    for _ in 0..warmup {
        let _ = run_python_case_with_runtime(case, fixtures_dir, scale, runtime, None).await;
    }

    let mut samples = Vec::new();
    let mut classification = "supported".to_string();

    for _ in 0..iterations {
        let started = Instant::now();
        match run_python_case_with_runtime(case, fixtures_dir, scale, runtime, None).await {
            Ok(output) => {
                classification = output.classification.clone();
                // Older runners may omit elapsed_ms; preserve the legacy wall-clock fallback.
                let elapsed_ms = output
                    .elapsed_ms
                    .unwrap_or_else(|| started.elapsed().as_secs_f64() * 1000.0);
                let semantic_state_digest = if lane_requires_semantic_validation(lane) {
                    output
                        .semantic_state_digest
                        .clone()
                        .or_else(|| output.result_hash.clone())
                } else {
                    None
                };
                let validation_summary = if lane_requires_semantic_validation(lane) {
                    output.validation_summary.clone().or_else(|| {
                        output
                            .rows_processed
                            .map(|rows| format!("rows_processed={rows}"))
                    })
                } else {
                    None
                };
                let metrics = SampleMetrics::base(
                    output.rows_processed,
                    output.bytes_processed,
                    output.operations,
                    output.table_version,
                )
                .with_runtime_io(RuntimeIOMetrics {
                    peak_rss_mb: output.peak_rss_mb,
                    cpu_time_ms: output.cpu_time_ms,
                    bytes_read: output.bytes_read,
                    bytes_written: output.bytes_written,
                    files_touched: output.files_touched,
                    files_skipped: output.files_skipped,
                    spill_bytes: output.spill_bytes,
                    result_hash: output.result_hash,
                    schema_hash: output.schema_hash,
                    semantic_state_digest,
                    validation_summary,
                });
                samples.push(IterationSample {
                    elapsed_ms,
                    rows: metrics.rows_processed,
                    bytes: metrics.bytes_processed,
                    metrics: Some(metrics),
                });
            }
            Err(error) => {
                return Ok(CaseResult {
                    case: case.to_string(),
                    success: false,
                    validation_passed: false,
                    perf_status: PerfStatus::Invalid,
                    classification,
                    elapsed_stats: None,
                    run_summary: None,
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
                    failure: Some(CaseFailure {
                        message: error.to_string(),
                    }),
                });
            }
        }
    }

    Ok(CaseResult {
        case: case.to_string(),
        success: true,
        validation_passed: true,
        perf_status: PerfStatus::Trusted,
        classification,
        elapsed_stats: elapsed_stats_from_samples(&samples),
        run_summary: None,
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
    })
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

async fn run_python_case_with_runtime(
    case: &str,
    fixtures_dir: &Path,
    scale: &str,
    runtime: &InteropRuntimeConfig,
    script_override: Option<&Path>,
) -> BenchResult<InteropCaseOutput> {
    let script = match script_override {
        Some(path) => path.to_path_buf(),
        None => {
            let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            repo_root
                .join("python")
                .join("delta_bench_interop")
                .join("run_case.py")
        }
    };

    let max_attempts = runtime.retries.saturating_add(1);
    for attempt in 1..=max_attempts {
        match run_python_case_once(case, fixtures_dir, scale, runtime, &script).await {
            Ok(output) => return Ok(output),
            Err(_error) if attempt < max_attempts => continue,
            Err(error) => {
                return Err(BenchError::InvalidArgument(format!(
                    "interop case '{case}' failed after {max_attempts} attempt(s): {error}"
                )));
            }
        }
    }

    Err(BenchError::InvalidArgument(format!(
        "interop case '{case}' did not execute any attempts"
    )))
}

async fn run_python_case_once(
    case: &str,
    fixtures_dir: &Path,
    scale: &str,
    runtime: &InteropRuntimeConfig,
    script: &Path,
) -> BenchResult<InteropCaseOutput> {
    let mut command = tokio::process::Command::new(&runtime.python_executable);
    command.kill_on_drop(true);
    command
        .arg(script)
        .arg("--case")
        .arg(case)
        .arg("--fixtures-dir")
        .arg(fixtures_dir)
        .arg("--scale")
        .arg(scale);
    let output = match tokio::time::timeout(runtime.timeout, command.output()).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(BenchError::InvalidArgument(format!(
                "interop case '{case}' timed out after {} ms",
                runtime.timeout.as_millis()
            )));
        }
    };

    if !output.status.success() {
        return Err(BenchError::InvalidArgument(format!(
            "interop case '{case}' failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    let parsed = serde_json::from_slice::<InteropCaseOutput>(&output.stdout).map_err(|error| {
        BenchError::InvalidArgument(format!(
            "failed to parse interop output for case '{case}': {error}"
        ))
    })?;
    validate_case_classification(parsed.classification.as_str()).map_err(|error| {
        BenchError::InvalidArgument(format!(
            "failed to parse interop output for case '{case}': {error}"
        ))
    })?;
    if let Some(elapsed_ms) = parsed.elapsed_ms {
        if !elapsed_ms.is_finite() || elapsed_ms < 0.0 {
            return Err(BenchError::InvalidArgument(format!(
                "failed to parse interop output for case '{case}': elapsed_ms must be finite and >= 0 (found {elapsed_ms})"
            )));
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use crate::cli::BenchmarkLane;

    use super::{run_case, run_python_case_with_runtime, InteropRuntimeConfig};

    #[cfg(unix)]
    fn make_executable(path: &std::path::Path) {
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn case_elapsed_uses_python_reported_timing_not_process_startup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let fake_python = temp.path().join("fake-python");
        fs::write(
            &fake_python,
            r#"#!/usr/bin/env sh
sleep 0.25
printf '%s' '{"rows_processed":1,"bytes_processed":1,"operations":1,"classification":"supported","elapsed_ms":1.5}'
"#,
        )
        .expect("write fake python executable");
        make_executable(&fake_python);

        let runtime = InteropRuntimeConfig {
            // Leave room for busy CI/workstations; the assertion is about reported
            // elapsed time, not exercising the timeout path.
            timeout: Duration::from_secs(5),
            retries: 0,
            python_executable: fake_python.to_string_lossy().into_owned(),
        };

        let case = run_case(
            "pandas_roundtrip_smoke",
            temp.path(),
            "sf1",
            BenchmarkLane::Macro,
            0,
            1,
            &runtime,
        )
        .await
        .expect("run case");

        assert!(
            case.success,
            "run case returned failure: {:?}",
            case.failure.as_ref().map(|failure| &failure.message)
        );
        assert_eq!(case.samples.len(), 1);
        assert!(
            (case.samples[0].elapsed_ms - 1.5).abs() < 0.001,
            "process startup leaked into measured time: {} ms",
            case.samples[0].elapsed_ms
        );
    }

    #[tokio::test]
    async fn python_runtime_enforces_timeout() {
        let temp = tempfile::tempdir().expect("tempdir");
        let script = temp.path().join("sleep_case.py");
        fs::write(
            &script,
            r#"#!/usr/bin/env python3
import time
time.sleep(0.25)
print('{"rows_processed":1,"bytes_processed":1,"operations":1,"classification":"supported"}')
"#,
        )
        .expect("write script");

        let runtime = InteropRuntimeConfig {
            timeout: Duration::from_millis(10),
            retries: 0,
            python_executable: "python3".to_string(),
        };
        let err = run_python_case_with_runtime(
            "timeout_case",
            temp.path(),
            "sf1",
            &runtime,
            Some(script.as_path()),
        )
        .await
        .expect_err("slow script should time out");
        assert!(
            err.to_string().contains("timed out"),
            "unexpected timeout error: {err}"
        );
    }

    #[tokio::test]
    async fn python_runtime_retries_transient_failure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let state_file = temp.path().join("retry_state.txt");
        let script = temp.path().join("retry_case.py");
        fs::write(
            &script,
            format!(
                r#"#!/usr/bin/env python3
from pathlib import Path
state = Path(r"{state}")
if not state.exists():
    state.write_text("1", encoding="utf-8")
    raise SystemExit("first attempt fails")
print('{{"rows_processed":1,"bytes_processed":1,"operations":1,"classification":"supported"}}')
"#,
                state = state_file.display()
            ),
        )
        .expect("write script");

        let runtime = InteropRuntimeConfig {
            timeout: Duration::from_secs(1),
            retries: 1,
            python_executable: "python3".to_string(),
        };
        let out = run_python_case_with_runtime(
            "retry_case",
            temp.path(),
            "sf1",
            &runtime,
            Some(script.as_path()),
        )
        .await
        .expect("one retry should recover");
        assert_eq!(out.classification, "supported");
    }

    #[tokio::test]
    async fn python_runtime_rejects_negative_elapsed_override() {
        let temp = tempfile::tempdir().expect("tempdir");
        let script = temp.path().join("negative_elapsed.py");
        fs::write(
            &script,
            r#"#!/usr/bin/env python3
print('{"rows_processed":1,"bytes_processed":1,"operations":1,"classification":"supported","elapsed_ms":-1.0}')
"#,
        )
        .expect("write script");

        let runtime = InteropRuntimeConfig {
            timeout: Duration::from_secs(1),
            retries: 0,
            python_executable: "python3".to_string(),
        };
        let err = run_python_case_with_runtime(
            "negative_elapsed",
            temp.path(),
            "sf1",
            &runtime,
            Some(script.as_path()),
        )
        .await
        .expect_err("negative elapsed should be rejected");
        assert!(
            err.to_string().contains("elapsed_ms"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn python_runtime_rejects_invalid_classification() {
        let temp = tempfile::tempdir().expect("tempdir");
        let script = temp.path().join("bad_classification.py");
        fs::write(
            &script,
            r#"#!/usr/bin/env python3
print('{"rows_processed":1,"bytes_processed":1,"operations":1,"classification":"experimental"}')
"#,
        )
        .expect("write script");

        let runtime = InteropRuntimeConfig {
            timeout: Duration::from_secs(1),
            retries: 0,
            python_executable: "python3".to_string(),
        };
        let err = run_python_case_with_runtime(
            "bad_classification",
            temp.path(),
            "sf1",
            &runtime,
            Some(script.as_path()),
        )
        .await
        .expect_err("invalid classification should fail parsing");
        assert!(
            err.to_string().contains("classification"),
            "unexpected error: {err}"
        );
    }
}
