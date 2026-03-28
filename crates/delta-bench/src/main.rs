use std::fs;

use chrono::Utc;
use clap::Parser;

use delta_bench::cli::{
    parse_storage_options, validate_label, Args, BenchmarkMode, Command, RunnerMode,
};
use delta_bench::data::fixtures::{generate_fixtures_with_profile, load_manifest, FixtureProfile};
use delta_bench::error::BenchResult;
use delta_bench::manifests::{ensure_required_manifests_exist, DatasetId};
use delta_bench::results::{
    render_run_summary_table, BenchContext, BenchRunResult, RESULT_SCHEMA_VERSION,
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
            timing_phase,
            warmup,
            iterations,
            no_summary_table,
        } => {
            let dataset = parse_dataset(dataset_id.as_deref())?;
            let effective_scale = resolve_scale(&scale, dataset)?;
            validate_label(&args.label)?;
            fs::create_dir_all(&args.results_dir)?;
            let mut run_plan = plan_run_cases(&target, runner, case_filter.as_deref())?;
            apply_dataset_assertion_policy(&mut run_plan, dataset);
            let effective_warmup = if benchmark_mode == BenchmarkMode::Assert {
                0
            } else {
                warmup
            };
            let effective_iterations = if benchmark_mode == BenchmarkMode::Assert {
                1
            } else {
                iterations
            };
            let cases = run_planned_cases(
                &args.fixtures_dir,
                &run_plan,
                effective_scale.as_str(),
                timing_phase,
                effective_warmup,
                effective_iterations,
                &storage,
            )
            .await?;
            let fixture_manifest = load_manifest(&args.fixtures_dir, effective_scale.as_str())?;
            let fidelity = benchmark_fidelity_info(&FidelityEnvOverrides::from_env());
            let cases = finalize_cases_for_benchmark_mode(cases, benchmark_mode);

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
                dataset_fingerprint: Some(fixture_manifest.dataset_fingerprint),
                runner: Some(runner.as_str().to_string()),
                storage_backend: Some(args.storage_backend.as_str().to_string()),
                benchmark_mode: Some(benchmark_mode.as_str().to_string()),
                lane: None,
                measurement_kind: None,
                validation_level: None,
                run_id: None,
                harness_revision: None,
                fixture_recipe_hash: fixture_manifest.fixture_recipe_hash.clone().into(),
                fidelity_fingerprint: None,
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

fn finalize_cases_for_benchmark_mode(
    mut cases: Vec<delta_bench::results::CaseResult>,
    benchmark_mode: BenchmarkMode,
) -> Vec<delta_bench::results::CaseResult> {
    if benchmark_mode != BenchmarkMode::Assert {
        return cases;
    }

    for case in &mut cases {
        case.perf_valid = false;
        case.elapsed_stats = None;
    }
    cases
}

fn command_requires_manifest_preflight(command: &Command) -> bool {
    matches!(command, Command::List { .. } | Command::Run { .. })
}
