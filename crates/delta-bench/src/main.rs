use std::fs;

use chrono::Utc;
use clap::Parser;

use delta_bench::cli::{parse_storage_options, validate_label, Args, Command};
use delta_bench::data::fixtures::generate_fixtures;
use delta_bench::error::BenchResult;
use delta_bench::results::{BenchContext, BenchRunResult};
use delta_bench::storage::StorageConfig;
use delta_bench::suites::{list_cases_for_target, list_targets, run_target};
use delta_bench::system::{
    benchmark_fidelity_info, delta_rs_checkout_info, host_name, FidelityEnvOverrides,
};

#[tokio::main]
async fn main() -> BenchResult<()> {
    let args = Args::parse();
    let storage_options = parse_storage_options(&args.storage_options)?;
    let storage = StorageConfig::new(args.storage_backend, storage_options)?;

    match args.command {
        Command::List { target } => {
            if target == "all" {
                println!("targets:");
                for t in list_targets() {
                    println!("- {t}");
                }
            }

            for c in list_cases_for_target(&target)? {
                println!("{c}");
            }
        }
        Command::Data { scale, seed, force } => {
            generate_fixtures(&args.fixtures_dir, &scale, seed, force, &storage).await?;
            println!(
                "fixtures ready at {} (scale={scale}, seed={seed})",
                args.fixtures_dir.display()
            );
        }
        Command::Run {
            scale,
            target,
            warmup,
            iterations,
        } => {
            validate_label(&args.label)?;
            fs::create_dir_all(&args.results_dir)?;
            let cases = run_target(
                &args.fixtures_dir,
                &target,
                &scale,
                warmup,
                iterations,
                &storage,
            )
            .await?;
            let fidelity = benchmark_fidelity_info(&FidelityEnvOverrides::from_env());

            let context = BenchContext {
                schema_version: 1,
                label: args.label.clone(),
                git_sha: args.git_sha.clone(),
                created_at: Utc::now(),
                host: host_name(),
                suite: target.clone(),
                scale: scale.clone(),
                iterations,
                warmup,
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
                schema_version: 1,
                context,
                cases,
            };

            let out_dir = args.results_dir.join(&args.label);
            fs::create_dir_all(&out_dir)?;
            let out_file = out_dir.join(format!("{target}.json"));
            fs::write(out_file.clone(), serde_json::to_vec_pretty(&output)?)?;
            println!("wrote result: {}", out_file.display());
        }
        Command::Doctor => {
            println!("delta-bench doctor");
            println!("fixtures_dir={}", args.fixtures_dir.display());
            println!("results_dir={}", args.results_dir.display());
            println!("storage_backend={:?}", storage.backend());

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
        }
    }

    Ok(())
}
