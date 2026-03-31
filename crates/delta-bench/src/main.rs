use std::fs;

use chrono::Utc;
use clap::Parser;
use serde::Serialize;

use delta_bench::cli::{
    parse_storage_options, validate_label, Args, BenchmarkLane, BenchmarkMode, Command, RunnerMode,
};
use delta_bench::data::fixtures::{generate_fixtures_with_profile, load_manifest, FixtureProfile};
use delta_bench::error::{BenchError, BenchResult};
use delta_bench::fingerprint::hash_json;
use delta_bench::manifests::{ensure_required_manifests_exist, DatasetId};
use delta_bench::results::{
    build_run_summary, render_run_summary_table, BenchContext, BenchRunResult,
    RESULT_SCHEMA_VERSION,
};
use delta_bench::storage::{load_backend_profile_options, StorageConfig};
use delta_bench::suites::{
    apply_dataset_assertion_policy, list_targets, plan_run_cases, run_planned_cases,
};
use delta_bench::system::{
    benchmark_fidelity_info, delta_rs_checkout_info, host_name, probe_python_modules,
    FidelityEnvOverrides, PYTHON_INTEROP_REQUIRED_MODULES,
};

#[tokio::main]
async fn main() -> BenchResult<()> {
    let args = Args::parse();
    if command_requires_manifest_preflight(&args.command) {
        ensure_required_manifests_exist()?;
    }
    let mut storage_options = load_backend_profile_options(args.backend_profile.as_deref())?;
    let cli_storage_options = parse_storage_options(&args.storage_options)?;
    storage_options.extend(cli_storage_options);
    let storage = StorageConfig::new(args.storage_backend, storage_options)?;

    match args.command {
        Command::List { target } => {
            if target == "all" {
                println!("targets:");
                for t in list_targets() {
                    println!("- {t}");
                }
            }
            for case in plan_run_cases(&target, RunnerMode::All, None)? {
                println!("{}", case.id);
            }
        }
        Command::Data {
            scale,
            dataset_id,
            seed,
            force,
        } => {
            let dataset = parse_dataset(dataset_id.as_deref())?;
            let effective_scale = resolve_scale(&scale, dataset)?;
            let profile = resolve_fixture_profile(dataset)?;
            generate_fixtures_with_profile(
                &args.fixtures_dir,
                effective_scale.as_str(),
                seed,
                force,
                profile,
                &storage,
            )
            .await?;
            println!(
                "fixtures ready at {} (scale={}, seed={seed})",
                args.fixtures_dir.display(),
                effective_scale
            );
        }
        Command::Run {
            scale,
            dataset_id,
            target,
            case_filter,
            runner,
            benchmark_mode,
            lane,
            timing_phase,
            warmup,
            iterations,
            no_summary_table,
        } => {
            let dataset = parse_dataset(dataset_id.as_deref())?;
            let effective_scale = resolve_scale(&scale, dataset)?;
            validate_label(&args.label)?;
            validate_execution_contract(benchmark_mode, lane)?;
            fs::create_dir_all(&args.results_dir)?;
            let mut run_plan = plan_run_cases(&target, runner, case_filter.as_deref())?;
            apply_dataset_assertion_policy(&mut run_plan, dataset);
            let effective_warmup = if benchmark_mode == BenchmarkMode::Assert
                || lane == BenchmarkLane::Correctness
                || lane == BenchmarkLane::Smoke
            {
                0
            } else {
                warmup
            };
            let effective_iterations = if benchmark_mode == BenchmarkMode::Assert
                || lane == BenchmarkLane::Correctness
                || lane == BenchmarkLane::Smoke
            {
                1
            } else {
                iterations
            };
            let cases = run_planned_cases(
                &args.fixtures_dir,
                &run_plan,
                effective_scale.as_str(),
                lane,
                timing_phase,
                effective_warmup,
                effective_iterations,
                &storage,
            )
            .await?;
            let fixture_manifest = load_manifest(&args.fixtures_dir, effective_scale.as_str())?;
            let fidelity = benchmark_fidelity_info(&FidelityEnvOverrides::from_env());
            let measurement_kind = measurement_kind_for_target(&target);
            let validation_level = validation_level_for_run_plan(&run_plan, lane);
            let fidelity_fingerprint = compute_fidelity_fingerprint(&fidelity)?;
            let run_id = compute_run_id(
                &args.label,
                args.git_sha.as_deref(),
                &target,
                &effective_scale,
                lane.as_str(),
                timing_phase.as_str(),
            )?;
            let context = BenchContext {
                schema_version: RESULT_SCHEMA_VERSION,
                label: args.label.clone(),
                git_sha: args.git_sha.clone(),
                created_at: Utc::now(),
                host: host_name(),
                suite: target.clone(),
                scale: effective_scale.clone(),
                iterations: effective_iterations,
                warmup: effective_warmup,
                timing_phase: Some(timing_phase.as_str().to_string()),
                dataset_id: dataset_id.clone(),
                dataset_fingerprint: Some(fixture_manifest.dataset_fingerprint.clone()),
                runner: Some(runner.as_str().to_string()),
                storage_backend: Some(args.storage_backend.as_str().to_string()),
                benchmark_mode: Some(benchmark_mode.as_str().to_string()),
                lane: Some(lane.as_str().to_string()),
                measurement_kind: Some(measurement_kind.to_string()),
                validation_level: Some(validation_level.to_string()),
                run_id: Some(run_id),
                harness_revision: args.harness_revision.clone(),
                fixture_recipe_hash: Some(fixture_manifest.fixture_recipe_hash.clone()),
                fidelity_fingerprint: Some(fidelity_fingerprint.clone()),
                backend_profile: args.backend_profile.clone(),
                image_version: fidelity.image_version,
                hardening_profile_id: fidelity.hardening_profile_id,
                hardening_profile_sha256: fidelity.hardening_profile_sha256,
                cpu_model: fidelity.cpu_model,
                cpu_microcode: fidelity.cpu_microcode,
                kernel: fidelity.kernel,
                boot_params: fidelity.boot_params,
                cpu_steal_pct: fidelity.cpu_steal_pct,
                numa_topology: fidelity.numa_topology,
                egress_policy_sha256: fidelity.egress_policy_sha256,
                run_mode: fidelity.run_mode,
                maintenance_window_id: fidelity.maintenance_window_id,
            };
            let cases = finalize_cases(cases, &run_plan, benchmark_mode, lane, &context)?;

            let output = BenchRunResult {
                schema_version: RESULT_SCHEMA_VERSION,
                context,
                cases,
            };

            let out_dir = args.results_dir.join(&args.label);
            fs::create_dir_all(&out_dir)?;
            let out_file = out_dir.join(format!("{target}.json"));
            fs::write(out_file.clone(), serde_json::to_vec_pretty(&output)?)?;
            let ok_count = output.cases.iter().filter(|case| case.success).count();
            let failed_count = output.cases.len().saturating_sub(ok_count);
            println!(
                "run summary: {} case(s), {} ok, {} failed",
                output.cases.len(),
                ok_count,
                failed_count
            );
            if !no_summary_table {
                println!("{}", render_run_summary_table(&output.cases));
            }
            println!("wrote result: {}", out_file.display());
        }
        Command::Doctor => {
            println!("delta-bench doctor");
            println!("fixtures_dir={}", args.fixtures_dir.display());
            println!("results_dir={}", args.results_dir.display());
            println!("storage_backend={:?}", storage.backend());
            println!(
                "backend_profile={}",
                args.backend_profile.as_deref().unwrap_or("none")
            );

            let checkout = delta_rs_checkout_info(None);
            println!("delta_rs_dir={}", checkout.checkout_dir.display());
            println!("delta_rs_checkout_present={}", checkout.checkout_present);
            println!("delta_rs_core_present={}", checkout.core_present);

            let fidelity = benchmark_fidelity_info(&FidelityEnvOverrides::from_env());
            println!(
                "image_version={}",
                fidelity.image_version.as_deref().unwrap_or("unknown")
            );
            println!(
                "hardening_profile_id={}",
                fidelity
                    .hardening_profile_id
                    .as_deref()
                    .unwrap_or("unknown")
            );
            println!(
                "hardening_profile_sha256={}",
                fidelity
                    .hardening_profile_sha256
                    .as_deref()
                    .unwrap_or("unknown")
            );
            println!(
                "egress_policy_sha256={}",
                fidelity
                    .egress_policy_sha256
                    .as_deref()
                    .unwrap_or("unknown")
            );
            println!(
                "run_mode={}",
                fidelity.run_mode.as_deref().unwrap_or("unknown")
            );
            println!(
                "maintenance_window_id={}",
                fidelity.maintenance_window_id.as_deref().unwrap_or("none")
            );
            println!(
                "cpu_model={}",
                fidelity.cpu_model.as_deref().unwrap_or("unknown")
            );
            println!(
                "cpu_microcode={}",
                fidelity.cpu_microcode.as_deref().unwrap_or("unknown")
            );
            println!("kernel={}", fidelity.kernel.as_deref().unwrap_or("unknown"));
            println!(
                "boot_params={}",
                fidelity.boot_params.as_deref().unwrap_or("unknown")
            );
            println!(
                "cpu_steal_pct={}",
                fidelity
                    .cpu_steal_pct
                    .map(|v| format!("{v:.6}"))
                    .unwrap_or_else(|| "unknown".to_string())
            );
            println!(
                "numa_topology={}",
                fidelity.numa_topology.as_deref().unwrap_or("unknown")
            );
            let hardening_state = match (
                fidelity.hardening_profile_id.as_deref(),
                fidelity.hardening_profile_sha256.as_deref(),
                fidelity.run_mode.as_deref(),
            ) {
                (Some(id), Some(sha), Some(mode)) => {
                    format!("profile={id};sha256={sha};run_mode={mode}")
                }
                (Some(id), Some(sha), None) => {
                    format!("profile={id};sha256={sha};run_mode=unknown")
                }
                (Some(id), None, Some(mode)) => {
                    format!("profile={id};sha256=unknown;run_mode={mode}")
                }
                (None, Some(sha), Some(mode)) => {
                    format!("profile=unknown;sha256={sha};run_mode={mode}")
                }
                _ => "unknown".to_string(),
            };
            println!("hardening_state={hardening_state}");

            let interop_python = std::env::var("DELTA_BENCH_INTEROP_PYTHON")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "python3".to_string());
            println!("interop_python_executable={interop_python}");

            let interop_probe =
                probe_python_modules(&interop_python, &PYTHON_INTEROP_REQUIRED_MODULES);
            if let Some(error) = interop_probe.probe_error.as_deref() {
                println!("interop_python_dependency_probe=error");
                println!("interop_python_dependency_probe_error={error}");
                println!(
                    "doctor_warning=python interop dependency probe failed for {interop_python}"
                );
                println!(
                    "doctor_hint=install/check modules: {}",
                    PYTHON_INTEROP_REQUIRED_MODULES.join(",")
                );
            } else if interop_probe.missing_modules.is_empty() {
                println!("interop_python_dependency_probe=ok");
                println!("interop_python_missing_dependencies=none");
            } else {
                println!("interop_python_dependency_probe=ok");
                println!(
                    "interop_python_missing_dependencies={}",
                    interop_probe.missing_modules.join(",")
                );
                println!(
                    "doctor_warning=python interop cases may report expected_failure until dependencies are installed"
                );
                println!(
                    "doctor_hint=install with: {interop_python} -m pip install {}",
                    PYTHON_INTEROP_REQUIRED_MODULES.join(" ")
                );
            }
        }
    }

    Ok(())
}

fn resolve_scale(scale: &str, dataset: Option<DatasetId>) -> BenchResult<String> {
    let Some(dataset) = dataset else {
        return Ok(scale.to_string());
    };
    Ok(dataset.scale().to_string())
}

fn resolve_fixture_profile(dataset: Option<DatasetId>) -> BenchResult<FixtureProfile> {
    let Some(dataset) = dataset else {
        return Ok(FixtureProfile::Standard);
    };
    Ok(match dataset.fixture_profile() {
        "many_versions" => FixtureProfile::ManyVersions,
        "tpcds_duckdb" => FixtureProfile::TpcdsDuckdb,
        _ => FixtureProfile::Standard,
    })
}

fn parse_dataset(dataset_id: Option<&str>) -> BenchResult<Option<DatasetId>> {
    dataset_id.map(DatasetId::parse).transpose()
}

fn validate_execution_contract(
    benchmark_mode: BenchmarkMode,
    lane: BenchmarkLane,
) -> BenchResult<()> {
    if benchmark_mode == BenchmarkMode::Assert && lane != BenchmarkLane::Correctness {
        return Err(BenchError::InvalidArgument(
            "--mode assert requires --lane correctness".to_string(),
        ));
    }
    Ok(())
}

fn finalize_cases(
    mut cases: Vec<delta_bench::results::CaseResult>,
    plan: &[delta_bench::suites::PlannedCase],
    benchmark_mode: BenchmarkMode,
    lane: BenchmarkLane,
    context: &BenchContext,
) -> BenchResult<Vec<delta_bench::results::CaseResult>> {
    for (case, planned) in cases.iter_mut().zip(plan.iter()) {
        case.run_summary = Some(build_run_summary(
            &case.samples,
            Some(context.host.as_str()),
            context.fidelity_fingerprint.as_deref(),
        ));
        case.suite_manifest_hash = Some(planned.suite_manifest_hash.clone());
        case.case_definition_hash = Some(planned.case_definition_hash.clone());
        case.supports_decision = Some(planned.supports_decision);
        case.required_runs = planned.required_runs;
        case.decision_threshold_pct = planned.decision_threshold_pct;
        case.decision_metric = planned.decision_metric.clone();
        case.compatibility_key =
            compute_case_compatibility_key(planned, lane, context).map(Some)?;
        if benchmark_mode == BenchmarkMode::Assert
            || lane == BenchmarkLane::Correctness
            || lane == BenchmarkLane::Smoke
            || (lane == BenchmarkLane::Macro && planned.lane == BenchmarkLane::Correctness.as_str())
        {
            case.perf_status = if case.validation_passed {
                delta_bench::results::PerfStatus::ValidationOnly
            } else {
                delta_bench::results::PerfStatus::Invalid
            };
            case.elapsed_stats = None;
        }
    }
    Ok(cases)
}

fn measurement_kind_for_target(target: &str) -> &'static str {
    if matches!(target, "scan" | "tpcds") {
        "phase_breakdown"
    } else {
        "end_to_end"
    }
}

fn validation_level_for_run_plan(
    plan: &[delta_bench::suites::PlannedCase],
    lane: BenchmarkLane,
) -> &'static str {
    if lane != BenchmarkLane::Correctness {
        return "operational";
    }
    if plan.iter().all(case_supports_semantic_validation) {
        "semantic"
    } else {
        "operational"
    }
}

fn case_supports_semantic_validation(case: &delta_bench::suites::PlannedCase) -> bool {
    matches!(
        case.target.as_str(),
        "write" | "delete_update" | "merge" | "metadata" | "optimize_vacuum" | "interop_py"
    )
}

fn compute_fidelity_fingerprint(
    fidelity: &delta_bench::system::BenchmarkFidelityInfo,
) -> BenchResult<String> {
    hash_json(&serde_json::json!({
        "image_version": fidelity.image_version,
        "hardening_profile_id": fidelity.hardening_profile_id,
        "hardening_profile_sha256": fidelity.hardening_profile_sha256,
        "cpu_model": fidelity.cpu_model,
        "cpu_microcode": fidelity.cpu_microcode,
        "kernel": fidelity.kernel,
        "boot_params": fidelity.boot_params,
        "cpu_steal_pct": fidelity.cpu_steal_pct,
        "numa_topology": fidelity.numa_topology,
        "egress_policy_sha256": fidelity.egress_policy_sha256,
        "run_mode": fidelity.run_mode,
        "maintenance_window_id": fidelity.maintenance_window_id,
    }))
}

fn compute_run_id(
    label: &str,
    git_sha: Option<&str>,
    suite: &str,
    scale: &str,
    lane: &str,
    timing_phase: &str,
) -> BenchResult<String> {
    hash_json(&serde_json::json!({
        "label": label,
        "git_sha": git_sha,
        "suite": suite,
        "scale": scale,
        "lane": lane,
        "timing_phase": timing_phase,
        "created_at": Utc::now().to_rfc3339(),
    }))
}

fn compute_case_compatibility_key(
    planned: &delta_bench::suites::PlannedCase,
    lane: BenchmarkLane,
    context: &BenchContext,
) -> BenchResult<String> {
    if planned
        .decision_threshold_pct
        .is_some_and(|threshold| !threshold.is_finite())
    {
        return Err(BenchError::InvalidArgument(format!(
            "decision_threshold_pct must be finite for compatibility_key generation (case='{}')",
            planned.id
        )));
    }

    #[derive(Serialize)]
    struct CompatibilityKeyInput<'a> {
        suite_scope: &'a str,
        target: &'a str,
        runner: Option<&'a str>,
        timing_phase: Option<&'a str>,
        dataset_id: Option<&'a str>,
        dataset_fingerprint: Option<&'a str>,
        scale: &'a str,
        storage_backend: Option<&'a str>,
        backend_profile: Option<&'a str>,
        lane: &'a str,
        measurement_kind: Option<&'a str>,
        validation_level: Option<&'a str>,
        harness_revision: Option<&'a str>,
        fixture_recipe_hash: Option<&'a str>,
        fidelity_fingerprint: Option<&'a str>,
        planned_lane: &'a str,
        suite_manifest_hash: &'a str,
        case_definition_hash: &'a str,
        supports_decision: bool,
        required_runs: Option<u32>,
        decision_threshold_pct: Option<f64>,
        decision_metric: Option<&'a str>,
    }

    hash_json(&CompatibilityKeyInput {
        suite_scope: context.suite.as_str(),
        target: planned.target.as_str(),
        runner: context.runner.as_deref(),
        timing_phase: context.timing_phase.as_deref(),
        dataset_id: context.dataset_id.as_deref(),
        dataset_fingerprint: context.dataset_fingerprint.as_deref(),
        scale: context.scale.as_str(),
        storage_backend: context.storage_backend.as_deref(),
        backend_profile: context.backend_profile.as_deref(),
        lane: lane.as_str(),
        measurement_kind: context.measurement_kind.as_deref(),
        validation_level: context.validation_level.as_deref(),
        harness_revision: context.harness_revision.as_deref(),
        fixture_recipe_hash: context.fixture_recipe_hash.as_deref(),
        fidelity_fingerprint: context.fidelity_fingerprint.as_deref(),
        planned_lane: planned.lane.as_str(),
        suite_manifest_hash: planned.suite_manifest_hash.as_str(),
        case_definition_hash: planned.case_definition_hash.as_str(),
        supports_decision: planned.supports_decision,
        required_runs: planned.required_runs,
        decision_threshold_pct: planned.decision_threshold_pct,
        decision_metric: planned.decision_metric.as_deref(),
    })
}

fn command_requires_manifest_preflight(command: &Command) -> bool {
    matches!(command, Command::List { .. } | Command::Run { .. })
}

#[cfg(test)]
mod tests {
    use super::{compute_case_compatibility_key, finalize_cases, validate_execution_contract};
    use chrono::Utc;
    use delta_bench::cli::{BenchmarkLane, BenchmarkMode};
    use delta_bench::error::BenchError;
    use delta_bench::results::{
        BenchContext, CaseResult, ElapsedStats, IterationSample, PerfStatus,
    };
    use delta_bench::suites::PlannedCase;

    fn planned_case(decision_threshold_pct: Option<f64>) -> PlannedCase {
        PlannedCase {
            id: "case-a".to_string(),
            target: "scan".to_string(),
            lane: "macro".to_string(),
            assertions: Vec::new(),
            suite_manifest_hash: "sha256:manifest".to_string(),
            case_definition_hash: "sha256:case-def".to_string(),
            supports_decision: true,
            required_runs: Some(5),
            decision_threshold_pct,
            decision_metric: Some("median".to_string()),
        }
    }

    fn case_result() -> CaseResult {
        CaseResult {
            case: "case-a".to_string(),
            success: true,
            validation_passed: true,
            perf_status: PerfStatus::Trusted,
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
            failure_kind: None,
            failure: None,
        }
    }

    fn timed_case_result() -> CaseResult {
        let mut case = case_result();
        case.samples = vec![IterationSample {
            elapsed_ms: 123.0,
            rows: None,
            bytes: None,
            metrics: None,
        }];
        case.elapsed_stats = Some(ElapsedStats {
            min_ms: 123.0,
            max_ms: 123.0,
            mean_ms: 123.0,
            median_ms: 123.0,
            stddev_ms: 0.0,
            cv_pct: Some(0.0),
        });
        case
    }

    fn bench_context() -> BenchContext {
        BenchContext {
            schema_version: 5,
            label: "test".to_string(),
            git_sha: Some("abc123".to_string()),
            created_at: Utc::now(),
            host: "host-a".to_string(),
            suite: "scan".to_string(),
            scale: "sf1".to_string(),
            iterations: 5,
            warmup: 1,
            timing_phase: Some("execute".to_string()),
            dataset_id: Some("tiny_smoke".to_string()),
            dataset_fingerprint: Some("sha256:dataset".to_string()),
            runner: Some("rust".to_string()),
            storage_backend: Some("local".to_string()),
            benchmark_mode: Some("perf".to_string()),
            lane: Some("macro".to_string()),
            measurement_kind: Some("phase_breakdown".to_string()),
            validation_level: Some("operational".to_string()),
            run_id: Some("sha256:run".to_string()),
            harness_revision: Some("harness-1".to_string()),
            fixture_recipe_hash: Some("sha256:recipe-a".to_string()),
            fidelity_fingerprint: Some("sha256:fidelity".to_string()),
            backend_profile: Some("local".to_string()),
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
        }
    }

    #[test]
    fn compatibility_key_rejects_non_finite_decision_thresholds() {
        let err = compute_case_compatibility_key(
            &planned_case(Some(f64::NAN)),
            BenchmarkLane::Macro,
            &bench_context(),
        )
        .expect_err("non-finite thresholds must fail compatibility hashing");

        assert!(
            matches!(err, BenchError::InvalidArgument(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn finalize_cases_propagates_compatibility_key_errors() {
        let err = finalize_cases(
            vec![case_result()],
            &[planned_case(Some(f64::NAN))],
            BenchmarkMode::Perf,
            BenchmarkLane::Macro,
            &bench_context(),
        )
        .expect_err("finalization must not silently drop compatibility-key failures");

        assert!(
            matches!(err, BenchError::InvalidArgument(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn compatibility_key_changes_when_context_identity_changes() {
        let planned = planned_case(Some(5.0));
        let baseline_context = bench_context();
        let baseline =
            compute_case_compatibility_key(&planned, BenchmarkLane::Macro, &baseline_context)
                .expect("baseline compatibility key");
        let mut fixture_context = bench_context();
        fixture_context.fixture_recipe_hash = Some("sha256:recipe-b".to_string());
        let fixture_changed =
            compute_case_compatibility_key(&planned, BenchmarkLane::Macro, &fixture_context)
                .expect("fixture change must hash");
        let mut runner_context = bench_context();
        runner_context.runner = Some("python".to_string());
        let runner_changed =
            compute_case_compatibility_key(&planned, BenchmarkLane::Macro, &runner_context)
                .expect("runner change must hash");

        assert_ne!(baseline, fixture_changed);
        assert_ne!(baseline, runner_changed);
    }

    #[test]
    fn finalize_cases_marks_correctness_tagged_macro_runs_validation_only() {
        let mut planned = planned_case(Some(5.0));
        planned.target = "write".to_string();
        planned.lane = "correctness".to_string();

        let cases = finalize_cases(
            vec![timed_case_result()],
            &[planned],
            BenchmarkMode::Perf,
            BenchmarkLane::Macro,
            &bench_context(),
        )
        .expect("finalization succeeds");

        let case = &cases[0];
        assert!(case.success);
        assert_eq!(case.perf_status, PerfStatus::ValidationOnly);
        assert!(case.elapsed_stats.is_none());
        assert_eq!(case.samples.len(), 1);
    }

    #[test]
    fn finalize_cases_preserves_invalid_status_for_failed_correctness_runs() {
        let mut case = timed_case_result();
        case.success = false;
        case.validation_passed = false;
        case.perf_status = PerfStatus::Invalid;
        case.elapsed_stats = None;

        let cases = finalize_cases(
            vec![case],
            &[planned_case(Some(5.0))],
            BenchmarkMode::Assert,
            BenchmarkLane::Correctness,
            &bench_context(),
        )
        .expect("finalization succeeds");

        let case = &cases[0];
        assert!(!case.success);
        assert_eq!(case.perf_status, PerfStatus::Invalid);
        assert!(case.elapsed_stats.is_none());
    }

    #[test]
    fn assert_mode_requires_correctness_lane() {
        let err = validate_execution_contract(BenchmarkMode::Assert, BenchmarkLane::Smoke)
            .expect_err("assert mode outside correctness must fail");
        assert!(
            matches!(err, BenchError::InvalidArgument(_)),
            "unexpected error: {err}"
        );

        validate_execution_contract(BenchmarkMode::Assert, BenchmarkLane::Correctness)
            .expect("correctness lane should be allowed");
    }
}
