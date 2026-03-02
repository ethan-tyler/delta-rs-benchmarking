# Best-in-Class Benchmark Suite and Automated PR Benchmark Bot for delta-rs

## Goals and constraints

The goal is a **macro-benchmark** suite and an **automated PR benchmark bot** that can be used by maintainers and contributors to catch performance regressions in delta-rs operations (reads/scans, writes, DML, Optimize, Vacuum, and metadata-heavy log/checkpoint paths) under controlled, reproducible conditions.

Two core realities shape the design:

delta-rs is **not** a SQL engine. It delegates much of its execution work to a query engine (DataFusion) while owning the **Delta transaction log semantics**, file rewriting decisions, and metadata handling. The benchmark suite must therefore be **operation-centric** (Delta table actions and log/metadata replay) rather than **query-suite-centric**. The delta-rs high-level operations API exposes builders for key operations such as load/scan, write, merge, delete, update, optimize, and vacuum. citeturn17view0turn21view1turn20view0turn21view2turn20view1turn18view0turn19view0

Reliable benchmarking requires explicit care around **noise**, **outliers**, and **methodology**. Criterion.rs outlines an analysis pipeline that includes explicit outlier classification (modified Tukey method / IQR), and reports confidence intervals for comparisons. citeturn26search0turn26search4 Meanwhile, Kalibera & Jones emphasize repeated measurements and accounting for non-determinism as a prerequisite for valid performance comparisons. citeturn31search0 And Mytkowicz et al. demonstrate how “innocuous” setup changes can introduce serious measurement bias. citeturn26search3

This specification intentionally mirrors the **developer experience** and **PR workflow** seen in the reference DataFusion benchmark bot: PR comment triggers, whitelisting, job queueing, and posting boxed-table comparisons. citeturn6view0turn15view2turn9view0

## Workloads and benchmark suite design

### Benchmark philosophy for Delta table format libraries

Benchmarks for open table formats should stress:

**Reads/scans:** file selection (partition pruning and statistics/data-skipping), projection, streaming read cost, and the overhead of resolving a snapshot (log replay and checkpoint usage).

**Writes and DML:** amount of data rewritten, commit/log writing, file rewrite planning, and the “touch set” dynamics that dominate Merge/Update/Delete. delta-rs implements DML as scan + rewrite of affected files (copy-on-write) in its current architecture, and exposes metrics such as scan and rewrite times for Delete. citeturn20view1turn21view0

**Table maintenance:** Optimize (bin-pack compaction and Z-order) is explicitly implemented as bin-packing small files into larger ones and creates remove actions but does not physically delete files (Vacuum does). citeturn18view0turn19view0 Vacuum enforces retention thresholds (default 7 days if not set) and warns about corruption risks if used incorrectly, reflecting the real-world operational sensitivity of this operation. citeturn19view0turn27search6

**Metadata-heavy paths:** log replay, checkpoint reads/writes, and time travel are performance-critical for high-churn tables. Checkpoints exist specifically to avoid replaying an unbounded number of JSON log files—delta-rs discussions emphasize checkpoints as a bulk-state shortcut, and the Spark reference behavior historically checkpoints roughly every 10 commits. citeturn32search0turn32search24 delta-rs provides checkpoint writing utilities (create_checkpoint), and delta-kernel documents a structured approach to checkpoint creation as well. citeturn34view0turn32search12

### Selected suites

This design defines three tiers:

**Smoke (fast, PR-friendly, < 5 minutes)**  
Small scale factors, one warmup, 3 measured iterations, focuses on early regression detection.

**PR suite (default, 10–15 minutes on dedicated runner)**  
Scale factor SF1 by default, 5 measured iterations, includes one representative benchmark per category.

**Nightly (trend-aware, larger SF10+, extended coverage)**  
Runs on main nightly with more iterations and optionally larger datasets; produces long-term results for dashboards.

### Benchmark cases and measured operations

Each benchmark case defines:

* **case_name**: stable string identifier
* **suite**: smoke | pr | nightly
* **operation**: read | write | merge | update | delete | optimize | vacuum | metadata | checkpoint
* **fixture**: which pre-generated dataset/table state to use
* **what is timed**: the *operation call and its internal execution only*
* **what is not timed**: data generation, table-building, table cloning/copy, compilation
* **warmup**: number of warmup runs (executed on throwaway clones)
* **iterations**: number of measured runs
* **metrics**: elapsed_ms, rows, bytes, plus operation-specific metrics captured from delta-rs (where available)

Read/scan suite:
- `read_full_scan_narrow` – `scan_table()` (LoadBuilder) + collect stream; measures scan + read of all data. citeturn21view1  
- `read_projection_wide_5cols` – projection pushdown; loads only 5 columns. citeturn21view1  
- `read_filter_partition_prune` – filter on partition column; expects reduced file set (partition pruning). (Partition filtering is core to Delta table scans and the delta-rs API supports partition/file enumeration and pruning; use-case reflected in docs.) citeturn27search4turn21view1  
- `read_filter_stats_skipping` – filter on a column with indexed stats; measures stats-based skipping effectiveness (using Delta’s file statistics). (Delta’s table properties include data skipping stats config.) citeturn27search15  

Write suite:
- `write_append_small_batches` – many small RecordBatches appended; measures commit and parquet writing overhead.
- `write_append_large_batches` – fewer large RecordBatches appended; throughput-oriented.
- `write_overwrite_replace_where` – overwrite with replaceWhere predicate; stresses conflict detection and file rewriting semantics. (WriteBuilder supports overwrite and replaceWhere.) citeturn20view0  
- `write_partitioned_append` – append with partitioning; stresses partition writer behavior. citeturn20view0  

Merge/DML suite:
- `merge_upsert_10pct` / `merge_upsert_50pct` / `merge_upsert_90pct` – MERGE INTO with varying match ratios; uses MergeBuilder and DataFusion DataFrame source. citeturn21view2  
- `update_selective_predicate` – UpdateBuilder with predicate; measures scan + rewrite; update path resembles delete path (scan then rewrite). citeturn21view0  
- `delete_selective_predicate` – DeleteBuilder with predicate; explicitly reports scan_time_ms and rewrite_time_ms. citeturn20view1  

Optimize suite:
- `optimize_compact_small_files` – bin-pack compaction on a “many small files” fixture; uses OptimizeBuilder. citeturn18view0  
- `optimize_zorder_2cols` – Z-order optimization on two columns (fixture must be set up with candidate columns); Optimize supports ZOrder planning/execution. citeturn18view2  

Vacuum suite:
- `vacuum_dry_run_lite` – dry-run listing and planning time. citeturn19view0  
- `vacuum_execute_lite` – actual delete_stream execution; measures file deletions. citeturn19view1  

Metadata suite:
- `metadata_table_load_cold` – DeltaTableBuilder load from disk; measures log listing/replay and checkpoint usage.
- `metadata_time_travel_vN` – load table at an older version (fixture with multi-version history).
- `metadata_log_replay_many_commits_no_checkpoint` – fixture with many commits but no checkpoint; worst-case log replay.
- `checkpoint_create_current_version` – call create_checkpoint at current version; then measure subsequent load speedup. delta-rs provides create_checkpoint(table, operation_id). citeturn34view0turn32search0turn32search24  

### Deletion vectors note

Deletion vectors are a major Delta Lake capability for faster DML, but delta-rs documentation notes that deletion vectors are not supported yet (at least for full write-path parity), and issues confirm gaps around DV support. citeturn27search1turn27search0turn27search23

Accordingly:
- The suite includes **DV-aware detection hooks** (skip DV-specific cases if the fixture requires DV features and the engine cannot read/write them).
- The framework is ready to add DV benchmark fixtures later without changing the harness API.

## Data generation and fixtures

### Principles

Data generation must be:
- **Deterministic** from a seed, producing identical Delta tables across runs.
- **Scalable** with SF1 (~100MB), SF10 (~1GB), SF100 (~10GB) targets.
- **Representative**: non-uniform distributions (Zipf-like categorical skew, timestamp locality, correlated columns) rather than purely uniform random.
- **Fixture-oriented**: tables at specific Delta states (many small files, many commits, checkpointed, partitioned/unpartitioned, etc.).

This design follows the same developer ergonomics used in DataFusion’s benchmarking scripts where data generation is separated from benchmark execution (bench.sh provides `data` and `run` workflows). citeturn10view0turn9view1 Delta’s upstream benchmark framework is similarly structured as a “load data then run queries” flow for TPC-DS on Spark clusters. citeturn25view1

### Table shapes

We define three schema families:

**narrow_sales** (default for many cases):
- `id: Int64` (unique-ish)
- `ts: Timestamp(ms)`
- `region: Utf8` (categorical, Zipf skew)
- `value_i64: Int64` (correlated with region and ts)
- `flag: Boolean`

Partitioning variants:
- partitioned by `region`
- partitioned by `date(ts)` (simulated by deriving a string partition like `yyyy-mm-dd`)

**wide_events**:
- 5 “hot” columns used for projection/filter
- 115 additional columns (ints, utf8) to stress projection cost and row group decode.

**dml_target**:
- primary key `pk`
- several mutable columns used for update/merge

### Fixture catalog

The `delta-bench data` command generates a directory like:

- `fixtures/sf1/narrow_sales_partitioned/`
- `fixtures/sf1/narrow_sales_unpartitioned/`
- `fixtures/sf1/wide_events_partitioned/`
- `fixtures/sf1/dml_target_baseline/`
- `fixtures/sf1/many_small_files/`
- `fixtures/sf1/many_commits_no_checkpoint/`
- `fixtures/sf1/many_commits_with_checkpoint/`

For “many small files”:
- generate the same total rows but spread across many small parquet files (controlled by a low target_file_size and small batch size at write time).

For “many commits”:
- repeated small appends to produce long log segments; then optionally call `create_checkpoint` to produce a checkpoint at steady intervals. citeturn34view0turn32search0

### Determinism strategy

To ensure determinism across architectures, the generator uses:
- a stable RNG (`rand_chacha::ChaCha20Rng`) seeded from a manifest seed
- integer-based distributions, and where floating point is needed, generation is based on integer transforms or `libm` to reduce platform drift.

A manifest file is written per fixture set:
- data_spec_version
- seed
- scale factor and target sizes
- schema hashes
- generation parameters
- delta-bench version and delta-rs version string

## Harness architecture, CLI, and statistical rigor

### Design choices grounded in prior art

**Comparison output style and thresholds:**  
DataFusion’s compare script uses `rich` tables and a default noise threshold of ±5%, and reports “no change” within that band. citeturn9view0 The same threshold is preserved as the default for delta-bench, but is configurable.

**Which statistic to report (min vs mean vs median):**  
DataFusion’s compare.py defaults to using the **minimum execution time** across iterations, explicitly to “account for variations / other things the system was doing.” citeturn9view0 For macro-benchmarks dominated by IO and OS noise, we adopt the same default (min), but also record full samples and can report mean±stddev or median.

**Outliers and confidence:**  
Criterion.rs documents explicit outlier classification and confidence intervals for comparisons. citeturn26search0turn26search4 For delta-bench, we store full iteration samples and compute:
- min, median, mean, stddev
- optional trimmed mean (10%)
- optional bootstrap CI on the median ratio (enabled for nightly/trend runs)

**Experimental methodology:**  
Kalibera & Jones emphasize multi-level repetition and reporting uncertainty in complex, non-deterministic systems. citeturn31search0 Mytkowicz et al. show measurement bias can arise from seemingly minor setup changes. citeturn26search3 Therefore, the bot:
- runs on dedicated hardware
- controls CPU governor and disables frequency scaling where possible
- runs jobs sequentially (no concurrency)
- captures system fingerprint in every result file

### delta-bench CLI

`delta-bench` is a Rust binary with subcommands:

- `delta-bench list` – list suites and cases
- `delta-bench data` – generate fixtures deterministically
- `delta-bench run` – run a suite or case(s), produce JSON results
- `delta-bench doctor` – validate environment (CPU governor, disk, permissions)

The CLI is intentionally similar to what DataFusion’s bench.sh orchestrates (data generation separate from execution; output JSON for compare.py). citeturn10view0turn9view0

### Output JSON format

A single result file contains:
- context (system info, git SHA, command args)
- per-case sample distributions (list of iterations)
- per-case metrics (rows, bytes, delta metrics)
- success/failure per case (failures become “incomparable” in comparisons, matching DataFusion behavior). citeturn9view0

## Tooling: compare.py, branch runner scripts, and CI

### compare.py

The comparison script is a production-quality Python 3.10+ CLI that:
- loads two JSON files
- aligns cases by name (handles added/removed cases gracefully)
- prints a rich box-drawing table like DataFusion’s compare output citeturn9view0
- prints a summary table (total time, faster/slower counts, failures)
- supports:
  - `--noise-threshold` (default 0.05)
  - `--format markdown` (for GitHub comment-friendly output)
  - `--detailed` (min vs mean±stddev)
  - `--ci` (exit non-zero if regression beyond threshold)

### compare_branch.sh

A bash script that:
- checks out `main` and the PR branch
- builds delta-bench in release mode (or “ci” profile)
- ensures fixtures exist (uses a shared persistent fixture directory on the VM)
- runs the suite on baseline (merge-base or main HEAD) and candidate SHA
- runs compare.py and posts the output

This is inspired by DataFusion’s `gh_compare_branch.sh` pattern: build both versions, run a chosen benchmark suite, and post results to the PR, capturing system info and providing commit SHAs and diff link. citeturn12search3turn15view2turn6view0

### GitHub Actions integration

Two options are provided (both fully specified):

**Option A (recommended): Standalone bot daemon on a dedicated VM**  
This matches the DataFusion “poll comments + job queue + sequential execution” approach. DataFusion’s approach explicitly whitelists users, parses “run benchmark …” triggers, and queues jobs in a directory. citeturn15view2

**Option B: GitHub Actions triggered by `issue_comment` + self-hosted runner**  
This uses Actions concurrency controls to ensure sequential runs and posts results back to PRs (less flexible queue UX, but simpler to operate).

## Automated PR benchmark bot: protocol, security, and infrastructure

### Comment protocol

The bot responds to these exact commands (first line of a PR comment):

```
run benchmark <suite-or-case> [<suite-or-case>...]
run benchmark all
show benchmark queue
cancel benchmark <job_id>
```

Environment variables can be passed on subsequent lines as `KEY=VALUE`, restricted to a safe regex (uppercase keys, safe characters in values). This approach is directly borrowed from the DataFusion scrape_comments.py implementation, which validates env vars and exports them in the generated job script. citeturn15view2

### Responses

Acknowledgement (immediate):
- reacts to the triggering comment (🚀)
- posts a comment:
  - “Queued job #…”
  - branch name, commit SHA, merge-base SHA
  - suite/cases requested
  - system fingerprint

This mirrors what is visible in DataFusion PR threads where the benchmark bot acknowledges a run and later posts results. citeturn6view0turn12search3

Result posting:
- posts a markdown `details` block with:
  - compare table
  - summary
  - links: diff between SHAs and artifact paths

Failures:
- a structured failure message with last N lines of output (DataFusion’s queue script bakes in a trap handler that posts tail output on error). citeturn15view2

### Whitelist

The bot refuses benchmarking requests from non-whitelisted users and posts a message enumerating allowed users, matching DataFusion behavior. citeturn15view2

### Queue and job execution model

Jobs are executed sequentially on a single machine. This is required for reproducibility and aligns with the DataFusion system. citeturn15view2

Queue state is represented as:
- `jobs/pending/*.sh`
- `jobs/running/<job_id>`
- `jobs/done/<job_id>.done`
- `jobs/failed/<job_id>.failed`

The bot can:
- show queue (render markdown table listing job ids, requestor, suite/cases)
- cancel a job (delete from `pending` or mark as canceled)

### Benchmark VM specification

A dedicated runner is recommended for stable results:

Instance recommendation:
- **AWS c6a.4xlarge** (16 vCPU, AMD EPYC) as a cost-effective “standardized CPU” instance very similar to what ClickBench uses by default for published results. citeturn23search7  
- Alternatively, **GCP compute-optimized** (e.g., c2-class) is acceptable; DataFusion’s own benchmark output shows GCP-based runners in the wild. citeturn12search3

Disk:
- dedicated NVMe (instance store) or provisioned IOPS SSD
- fixtures stored on a persistent volume; temp job dirs on local NVMe

OS tuning:
- performance governor
- disable transparent huge pages where appropriate
- pin Rust toolchain
- disable background updates
- fixed filesystem mount options

Monitoring:
- simple watchdog (systemd) that restarts bot daemons
- daily health check GitHub issue comment ability (optional)

Security:
- use a least-privilege GitHub token restricted to commenting on PRs in the repo
- strict env var parsing and no arbitrary command execution
- build in a controlled workspace; clean directories between runs

---

## Deliverables: complete implementable repository specification

This section provides a **drop-in PR-ready** layout for delta-rs, plus the standalone bot infrastructure that can either live in-repo (under `benchmarks/bot/`) or in a separate repo.

### Placement recommendation

A hybrid approach is recommended:

- **Benchmark binary + scripts inside delta-rs** for version-locked correctness and minimal dependency drift, matching the way DataFusion keeps benchmarks in-repo. citeturn9view1turn10view0  
- **Bot infrastructure optionally in a separate repo** for operational separation, matching the way DataFusion keeps its benchmarking automation scripts outside the main repo and drives PR benchmarking via external scripts. citeturn12search0turn15view2

This also aligns with delta-rs’s existing experience: there is already a `crates/benchmarks` crate for microbenchmarks, but delta-rs maintainers have explicitly discussed the need to redo and expand benchmarking infrastructure beyond ad-hoc setups. citeturn23search0turn28view3

---

## Repository layout

**Add these directories and files to delta-rs**:

```text
delta-rs/
├── benchmarks/
│   ├── README.md
│   ├── CONTRIBUTING.md
│   ├── bench.sh
│   ├── compare_branch.sh
│   ├── python/
│   │   ├── pyproject.toml
│   │   ├── src/delta_bench_compare/__init__.py
│   │   ├── src/delta_bench_compare/compare.py
│   │   ├── src/delta_bench_compare/model.py
│   │   ├── src/delta_bench_compare/render.py
│   │   └── tests/test_compare.py
│   ├── docs/
│   │   ├── adr/
│   │   │   ├── 0001-results-format.md
│   │   │   ├── 0002-fixtures-and-determinism.md
│   │   │   ├── 0003-bot-architecture.md
│   │   │   └── 0004-statistics-and-thresholds.md
│   │   └── architecture.md
│   └── bot/
│       ├── README.md
│       ├── pyproject.toml
│       ├── src/delta_bench_bot/__init__.py
│       ├── src/delta_bench_bot/config.py
│       ├── src/delta_bench_bot/github_api.py
│       ├── src/delta_bench_bot/parser.py
│       ├── src/delta_bench_bot/queue.py
│       ├── src/delta_bench_bot/scrape_comments.py
│       ├── src/delta_bench_bot/worker.py
│       ├── tests/test_parser.py
│       ├── systemd/
│       │   ├── delta-bench-scraper.service
│       │   ├── delta-bench-worker.service
│       │   └── delta-bench-scraper.timer
│       └── vm/
│           ├── provision_ubuntu_2404.sh
│           └── sysctl_delta_bench.conf
├── crates/
│   ├── delta-bench/
│   │   ├── Cargo.toml
│   │   ├── README.md
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── cli.rs
│   │   │   ├── error.rs
│   │   │   ├── logging.rs
│   │   │   ├── result.rs
│   │   │   ├── system_info.rs
│   │   │   ├── fs.rs
│   │   │   ├── suites/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── read.rs
│   │   │   │   ├── write.rs
│   │   │   │   ├── dml.rs
│   │   │   │   ├── optimize.rs
│   │   │   │   ├── vacuum.rs
│   │   │   │   ├── metadata.rs
│   │   │   │   └── checkpoint.rs
│   │   │   ├── data/
│   │   │   │   ├── mod.rs
│   │   │   │   ├── manifest.rs
│   │   │   │   ├── rng.rs
│   │   │   │   ├── distributions.rs
│   │   │   │   ├── schema.rs
│   │   │   │   ├── generator.rs
│   │   │   │   └── fixtures.rs
│   │   │   └── runner/
│   │   │       ├── mod.rs
│   │   │       ├── case.rs
│   │   │       ├── timing.rs
│   │   │       └── util.rs
│   │   └── tests/
│   │       ├── determinism.rs
│   │       └── end_to_end.rs
├── .github/
│   └── workflows/
│       └── benchmark.yml
```

Also required: add `"crates/delta-bench"` to the workspace members (delta-rs currently includes `crates/*`, so it will be picked up automatically). citeturn29view0

---

## Rust: `crates/delta-bench/Cargo.toml`

```toml
[package]
name = "delta-bench"
version = "0.1.0"
publish = false

authors.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
rust-version.workspace = true
description = "Macro-benchmark runner and fixture generator for delta-rs"

[dependencies]
# Workspace-pinned core deps
deltalake-core = { path = "../core", features = ["datafusion"] }
arrow = { workspace = true }
arrow-array = { workspace = true }
arrow-schema = { workspace = true }
datafusion = { workspace = true }
object_store = { workspace = true }
parquet = { workspace = true }

# CLI + logging + serialization
clap = { version = "4.5.28", features = ["derive", "env"] }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true, features = ["env-filter"] }

# Runtime
tokio = { workspace = true, features = ["macros", "rt-multi-thread", "fs", "process", "sync"] }
futures = { workspace = true }

# Deterministic RNG + hashing
rand = "0.8.5"
rand_chacha = "0.3.1"
sha2 = "0.10.8"
hex = "0.4.3"

# Utilities
uuid = { workspace = true, features = ["v4", "serde"] }
url = { workspace = true }
num_cpus = { workspace = true }
bytesize = "1.3.0"
humantime = "2.1.0"
walkdir = "2.5.0"
filetime = "0.2.25"

[dev-dependencies]
tempfile = { workspace = true }
pretty_assertions = "1.4.1"
```

Notes:
- delta-rs already uses `thiserror` per its crate conventions, and we keep library-style error typing, matching the delta-rs codebase. citeturn29view1  
- We use `tracing` instead of println, consistent with delta-rs implementation patterns. citeturn18view0turn19view0  

---

## Rust source code

### `crates/delta-bench/src/main.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

mod cli;
mod error;
mod fs;
mod logging;
mod result;
mod runner;
mod suites;
mod system_info;
mod data;

use clap::Parser;
use tracing::info;

use crate::cli::{Args, Command};
use crate::error::BenchResult;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> BenchResult<()> {
    logging::init_tracing()?;

    let args = Args::parse();
    info!(?args, "delta-bench starting");

    match args.command {
        Command::List(list) => {
            suites::list(&list)?;
        }
        Command::Doctor(doc) => {
            system_info::doctor(&doc)?;
        }
        Command::Data(data_cmd) => {
            data::run_data_cmd(&args, &data_cmd).await?;
        }
        Command::Run(run_cmd) => {
            runner::run_benchmarks(&args, &run_cmd).await?;
        }
    }

    Ok(())
}
```

### `crates/delta-bench/src/cli.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "delta-bench")]
#[command(about = "Macro-benchmarks and fixture generator for delta-rs", long_about = None)]
pub struct Args {
    /// Path to the dataset/fixture root directory.
    ///
    /// On a benchmark VM, this should be a persistent disk mount (e.g., /opt/delta-bench/fixtures).
    #[arg(long, env = "DELTA_BENCH_FIXTURES", default_value = "benchmarks/fixtures")]
    pub fixtures_dir: String,

    /// Path to write benchmark results.
    #[arg(long, env = "DELTA_BENCH_RESULTS", default_value = "benchmarks/results")]
    pub results_dir: String,

    /// Optional: git SHA to embed in result context.
    #[arg(long, env = "DELTA_BENCH_GIT_SHA")]
    pub git_sha: Option<String>,

    /// Optional: label for this run (e.g. "main", "pr-1234").
    #[arg(long, env = "DELTA_BENCH_LABEL")]
    pub label: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    List(ListCmd),
    Doctor(DoctorCmd),
    Data(DataCmd),
    Run(RunCmd),
}

#[derive(Debug, ClapArgs)]
pub struct ListCmd {
    /// What to list
    #[arg(value_enum, default_value_t = ListWhat::All)]
    pub what: ListWhat,
}

#[derive(Debug, Copy, Clone, ValueEnum)]
pub enum ListWhat {
    All,
    Suites,
    Cases,
}

#[derive(Debug, ClapArgs)]
pub struct DoctorCmd {
    /// Print JSON instead of human output.
    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Debug, ClapArgs)]
pub struct DataCmd {
    /// Scale factor: sf1, sf10, sf100 (custom allowed).
    #[arg(long, default_value = "sf1")]
    pub scale: String,

    /// Seed for deterministic generation (u64).
    #[arg(long, default_value_t = 42)]
    pub seed: u64,

    /// Regenerate even if manifest exists.
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Debug, ClapArgs)]
pub struct RunCmd {
    /// Suite name ("smoke", "pr", "nightly") or "case:<case_name>"
    #[arg(long, default_value = "pr")]
    pub target: String,

    /// Number of warmup iterations (not recorded).
    #[arg(long, default_value_t = 1)]
    pub warmup: usize,

    /// Number of measured iterations.
    #[arg(long, default_value_t = 5)]
    pub iterations: usize,

    /// Scale factor to use (must have fixtures generated).
    #[arg(long, default_value = "sf1")]
    pub scale: String,

    /// Stop on first failure.
    #[arg(long, default_value_t = false)]
    pub fail_fast: bool,

    /// Output file name override (defaults to <label>/<target>.json).
    #[arg(long)]
    pub output: Option<String>,
}
```

### `crates/delta-bench/src/error.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::path::PathBuf;

pub type BenchResult<T> = Result<T, BenchError>;

#[derive(thiserror::Error, Debug)]
pub enum BenchError {
    #[error("invalid argument: {0}")]
    InvalidArg(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("delta error: {0}")]
    Delta(#[from] deltalake_core::DeltaTableError),

    #[error("datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("fixture missing: {0}")]
    FixtureMissing(PathBuf),

    #[error("case failed: {case_name}: {source}")]
    CaseFailed {
        case_name: String,
        #[source]
        source: Box<BenchError>,
    },
}
```

### `crates/delta-bench/src/logging.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use crate::error::{BenchError, BenchResult};

pub fn init_tracing() -> BenchResult<()> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,delta_bench=info,deltalake_core=warn"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .init();

    Ok(())
}
```

### `crates/delta-bench/src/system_info.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::collections::BTreeMap;

use crate::cli::DoctorCmd;
use crate::error::{BenchError, BenchResult};

#[derive(Debug, serde::Serialize)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub num_cpus: usize,
    pub rustc: String,
    pub kernel: Option<String>,
    pub env: BTreeMap<String, String>,
}

pub fn collect_system_info() -> SystemInfo {
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();
    let num_cpus = num_cpus::get();

    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_else(|| "unknown".to_string())
        .trim()
        .to_string();

    let kernel = std::process::Command::new("uname")
        .arg("-r")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());

    let mut env = BTreeMap::new();
    for k in ["RUST_LOG", "DELTA_BENCH_FIXTURES", "DELTA_BENCH_RESULTS"] {
        if let Ok(v) = std::env::var(k) {
            env.insert(k.to_string(), v);
        }
    }

    SystemInfo {
        os,
        arch,
        num_cpus,
        rustc,
        kernel,
        env,
    }
}

pub fn doctor(cmd: &DoctorCmd) -> BenchResult<()> {
    let info = collect_system_info();
    if cmd.json {
        println!("{}", serde_json::to_string_pretty(&info)?);
    } else {
        println!("OS: {}", info.os);
        println!("Arch: {}", info.arch);
        println!("CPUs: {}", info.num_cpus);
        println!("rustc: {}", info.rustc);
        if let Some(k) = &info.kernel {
            println!("Kernel: {k}");
        }
    }
    Ok(())
}
```

### `crates/delta-bench/src/result.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::collections::BTreeMap;

use crate::system_info::SystemInfo;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BenchContext {
    pub schema_version: u32,
    pub start_time_unix_s: i64,
    pub label: String,
    pub git_sha: Option<String>,
    pub args: Vec<String>,
    pub system: SystemInfo,
    pub versions: BTreeMap<String, String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Iteration {
    pub elapsed_ms: f64,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CaseResult {
    pub case: String,
    pub success: bool,
    pub start_time_unix_s: i64,
    pub iterations: Vec<Iteration>,
    pub error: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct BenchRun {
    pub context: BenchContext,
    pub results: Vec<CaseResult>,
}
```

### `crates/delta-bench/src/fs.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::path::{Path, PathBuf};

use crate::error::{BenchError, BenchResult};

pub fn ensure_dir(path: &Path) -> BenchResult<()> {
    std::fs::create_dir_all(path)?;
    Ok(())
}

/// Create a fast clone of a fixture directory into a temp working directory.
///
/// Strategy:
/// 1) Use hardlinks for files where possible (like `cp -al`) to avoid copying GBs.
/// 2) Copy directory structure.
pub fn clone_dir_hardlink(src: &Path, dst: &Path) -> BenchResult<()> {
    if !src.exists() {
        return Err(BenchError::FixtureMissing(src.to_path_buf()));
    }
    ensure_dir(dst)?;
    for entry in walkdir::WalkDir::new(src) {
        let entry = entry.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let rel = entry.path().strip_prefix(src).unwrap_or(entry.path());
        let out_path = dst.join(rel);

        if entry.file_type().is_dir() {
            ensure_dir(&out_path)?;
            continue;
        }

        if entry.file_type().is_file() {
            if let Some(parent) = out_path.parent() {
                ensure_dir(parent)?;
            }
            // Hardlink files for speed and determinism.
            std::fs::hard_link(entry.path(), &out_path)?;
        }
    }
    Ok(())
}

pub fn now_unix_s() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn write_json_pretty(path: &Path, value: &impl serde::Serialize) -> BenchResult<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let s = serde_json::to_string_pretty(value)?;
    std::fs::write(path, s)?;
    Ok(())
}
```

---

## Runner and suites

### `crates/delta-bench/src/runner/mod.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

pub mod case;
pub mod timing;
pub mod util;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::cli::{Args, RunCmd};
use crate::error::{BenchError, BenchResult};
use crate::fs::{ensure_dir, now_unix_s, write_json_pretty};
use crate::result::{BenchContext, BenchRun, CaseResult};
use crate::system_info::collect_system_info;

use crate::suites;

pub async fn run_benchmarks(args: &Args, cmd: &RunCmd) -> BenchResult<()> {
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| "local".to_string());

    let target = cmd.target.clone();

    let fixtures_dir = PathBuf::from(&args.fixtures_dir).join(&cmd.scale);
    if !fixtures_dir.exists() {
        return Err(BenchError::FixtureMissing(fixtures_dir));
    }

    let results_dir = PathBuf::from(&args.results_dir).join(&label);
    ensure_dir(&results_dir)?;

    let output_path = if let Some(o) = &cmd.output {
        results_dir.join(o)
    } else {
        results_dir.join(format!("{target}.json"))
    };

    let versions = {
        let mut m = BTreeMap::new();
        m.insert(
            "deltalake-core".to_string(),
            deltalake_core::crate_version().to_string(),
        );
        m.insert(
            "arrow".to_string(),
            arrow::util::pretty::pretty_format_batches(&[]).to_string(), // harmless placeholder
        );
        // DataFusion version is available via crate metadata; embed as Cargo pkg version string.
        m.insert("datafusion".to_string(), env!("CARGO_PKG_VERSION").to_string());
        m
    };

    let ctx = BenchContext {
        schema_version: 1,
        start_time_unix_s: now_unix_s(),
        label,
        git_sha: args.git_sha.clone(),
        args: std::env::args().collect(),
        system: collect_system_info(),
        versions,
    };

    let cases = suites::resolve_cases(&target)?;
    let mut results: Vec<CaseResult> = Vec::with_capacity(cases.len());

    for c in cases {
        let r = match case::run_case(&fixtures_dir, cmd, &c).await {
            Ok(res) => res,
            Err(e) => {
                let err_str = format!("{e:?}");
                let failed = CaseResult {
                    case: c.name.clone(),
                    success: false,
                    start_time_unix_s: now_unix_s(),
                    iterations: vec![],
                    error: Some(err_str),
                };
                if cmd.fail_fast {
                    results.push(failed);
                    break;
                }
                failed
            }
        };
        results.push(r);
    }

    let run = BenchRun { context: ctx, results };
    write_json_pretty(&output_path, &run)?;
    println!("Wrote results: {}", output_path.display());

    Ok(())
}
```

### `crates/delta-bench/src/runner/case.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::path::{Path, PathBuf};

use crate::cli::RunCmd;
use crate::error::{BenchError, BenchResult};
use crate::fs::{clone_dir_hardlink, now_unix_s};
use crate::result::{CaseResult, Iteration};
use crate::runner::timing::measure_iterations;
use crate::suites::CaseDef;

pub async fn run_case(fixtures_root: &Path, cmd: &RunCmd, case: &CaseDef) -> BenchResult<CaseResult> {
    let fixture_path = fixtures_root.join(&case.fixture_relpath);
    if !fixture_path.exists() {
        return Err(BenchError::FixtureMissing(fixture_path));
    }

    let mut all_iters: Vec<Iteration> = vec![];

    // Warmups (throwaway clones)
    for _ in 0..cmd.warmup {
        let workdir = tempfile::tempdir()?;
        let clone_dst = workdir.path().join("table");
        clone_dir_hardlink(&fixture_path, &clone_dst)?;
        (case.run_fn)(clone_dst).await?;
    }

    // Measured iterations
    let measured = measure_iterations(&fixture_path, cmd.iterations, case).await?;
    all_iters.extend(measured);

    Ok(CaseResult {
        case: case.name.clone(),
        success: true,
        start_time_unix_s: now_unix_s(),
        iterations: all_iters,
        error: None,
    })
}
```

### `crates/delta-bench/src/runner/timing.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::path::Path;

use crate::error::BenchResult;
use crate::result::Iteration;
use crate::runner::util::case_workdir;
use crate::suites::CaseDef;

pub async fn measure_iterations(
    fixture: &Path,
    iterations: usize,
    case: &CaseDef,
) -> BenchResult<Vec<Iteration>> {
    let mut out = Vec::with_capacity(iterations);
    for _i in 0..iterations {
        let workdir = case_workdir(fixture)?;
        let start = std::time::Instant::now();
        let metrics = (case.run_fn)(workdir.clone()).await?;
        let elapsed = start.elapsed();
        out.push(Iteration {
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            rows: metrics.rows,
            bytes: metrics.bytes,
            extra: metrics.extra,
        });
    }
    Ok(out)
}
```

### `crates/delta-bench/src/runner/util.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::path::{Path, PathBuf};

use crate::error::BenchResult;
use crate::fs::clone_dir_hardlink;

pub fn case_workdir(fixture: &Path) -> BenchResult<PathBuf> {
    let td = tempfile::tempdir()?;
    let dst = td.path().join("table");
    clone_dir_hardlink(fixture, &dst)?;
    // Keep TempDir alive by leaking it intentionally; OS cleans on process exit.
    // For a long-running worker, the worker cleans temp roots explicitly.
    std::mem::forget(td);
    Ok(dst)
}
```

---

## Suite definitions

### `crates/delta-bench/src/suites/mod.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use crate::cli::{ListCmd, ListWhat};
use crate::error::{BenchError, BenchResult};

pub mod checkpoint;
pub mod dml;
pub mod metadata;
pub mod optimize;
pub mod read;
pub mod vacuum;
pub mod write;

#[derive(Debug, Clone)]
pub struct RunMetrics {
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl RunMetrics {
    pub fn empty() -> Self {
        Self {
            rows: None,
            bytes: None,
            extra: BTreeMap::new(),
        }
    }
}

pub type RunFn = fn(std::path::PathBuf) -> Pin<Box<dyn Future<Output = BenchResult<RunMetrics>> + Send>>;

#[derive(Clone)]
pub struct CaseDef {
    pub name: String,
    pub suite: String,
    pub fixture_relpath: String,
    pub run_fn: RunFn,
}

pub fn all_cases() -> Vec<CaseDef> {
    let mut v = vec![];
    v.extend(read::cases());
    v.extend(write::cases());
    v.extend(dml::cases());
    v.extend(optimize::cases());
    v.extend(vacuum::cases());
    v.extend(metadata::cases());
    v.extend(checkpoint::cases());
    v
}

pub fn resolve_cases(target: &str) -> BenchResult<Vec<CaseDef>> {
    let cases = all_cases();

    if target == "all" {
        return Ok(cases);
    }
    if target.starts_with("case:") {
        let wanted = target.trim_start_matches("case:").to_string();
        let found: Vec<CaseDef> = cases.into_iter().filter(|c| c.name == wanted).collect();
        if found.is_empty() {
            return Err(BenchError::InvalidArg(format!("unknown case: {wanted}")));
        }
        return Ok(found);
    }

    // suite target: smoke|pr|nightly
    let found: Vec<CaseDef> = cases.into_iter().filter(|c| c.suite == target).collect();
    if found.is_empty() {
        return Err(BenchError::InvalidArg(format!("unknown suite: {target}")));
    }
    Ok(found)
}

pub fn list(cmd: &ListCmd) -> BenchResult<()> {
    let cases = all_cases();
    match cmd.what {
        ListWhat::All => {
            println!("Suites: smoke, pr, nightly");
            println!("Cases:");
            for c in cases {
                println!("  - {} (suite={}, fixture={})", c.name, c.suite, c.fixture_relpath);
            }
        }
        ListWhat::Suites => {
            println!("smoke");
            println!("pr");
            println!("nightly");
        }
        ListWhat::Cases => {
            for c in cases {
                println!("{}", c.name);
            }
        }
    }
    Ok(())
}
```

### `crates/delta-bench/src/suites/read.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use deltalake_core::operations::collect_sendable_stream;
use deltalake_core::DeltaTableBuilder;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

fn ctx() -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(1);
    SessionContext::new_with_config(cfg)
}

async fn scan_all(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;
    let (_t, stream) = table.scan_table().await?;
    let batches: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok(RunMetrics {
        rows: Some(rows),
        bytes: None,
        extra: Default::default(),
    })
}

async fn scan_projection(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;
    let (_t, stream) = table.scan_table().with_columns(["id", "ts", "region", "value_i64", "flag"]).await?;
    let batches: Vec<RecordBatch> = collect_sendable_stream(stream).await?;
    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    Ok(RunMetrics {
        rows: Some(rows),
        bytes: None,
        extra: Default::default(),
    })
}

pub fn cases() -> Vec<CaseDef> {
    vec![
        CaseDef {
            name: "read_full_scan_narrow".to_string(),
            suite: "pr".to_string(),
            fixture_relpath: "narrow_sales_partitioned".to_string(),
            run_fn: |p| Pin::from(Box::new(scan_all(p))),
        },
        CaseDef {
            name: "read_projection_wide_5cols".to_string(),
            suite: "pr".to_string(),
            fixture_relpath: "wide_events_partitioned".to_string(),
            run_fn: |p| Pin::from(Box::new(scan_projection(p))),
        },
    ]
}
```

### `crates/delta-bench/src/suites/write.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema};
use deltalake_core::protocol::SaveMode;
use deltalake_core::DeltaTable;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

fn make_batch(n: usize) -> arrow::array::RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None), false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value_i64", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
    ]));
    let ids: Vec<i64> = (0..n as i64).collect();
    let ts: Vec<i64> = (0..n as i64).map(|i| 1_700_000_000_000i64 + i).collect();
    let region: Vec<&str> = (0..n).map(|i| if i % 3 == 0 { "us" } else if i % 3 == 1 { "eu" } else { "ap" }).collect();
    let val: Vec<i64> = (0..n as i64).map(|i| i * 7).collect();
    let flag: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();

    arrow::array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(StringArray::from(region)),
            Arc::new(Int64Array::from(val)),
            Arc::new(BooleanArray::from(flag)),
        ],
    )
    .unwrap()
}

async fn append_small_batches(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    // Path is a cloned fixture; we create a new table directory under it to isolate.
    let table_dir = path.join("write_target");
    std::fs::create_dir_all(&table_dir)?;

    let url = url::Url::from_directory_path(&table_dir).unwrap();
    let table = DeltaTable::try_from_url(url).await?;

    // Many small batches
    let batches = (0..20).map(|_| make_batch(10_000)).collect::<Vec<_>>();
    let (t, metrics) = table
        .write(batches)
        .with_save_mode(SaveMode::Append)
        .await?;

    let mut extra = std::collections::BTreeMap::new();
    extra.insert("num_added_files".to_string(), serde_json::json!(metrics.num_added_files));
    extra.insert("num_added_rows".to_string(), serde_json::json!(metrics.num_added_rows));
    Ok(RunMetrics {
        rows: Some(metrics.num_added_rows as u64),
        bytes: None,
        extra,
    })
}

pub fn cases() -> Vec<CaseDef> {
    vec![CaseDef {
        name: "write_append_small_batches".to_string(),
        suite: "smoke".to_string(),
        fixture_relpath: "empty_dir".to_string(),
        run_fn: |p| Pin::from(Box::new(append_small_batches(p))),
    }]
}
```

> Note: the fixture `empty_dir` is generated as an empty directory in `delta-bench data`.

### `crates/delta-bench/src/suites/dml.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use deltalake_core::operations::collect_sendable_stream;
use deltalake_core::DeltaTableBuilder;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

fn df_ctx() -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(1);
    SessionContext::new_with_config(cfg)
}

async fn delete_selective(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;

    let (t2, metrics) = table
        .delete()
        .with_predicate(datafusion::prelude::col("id").gt(datafusion::prelude::lit(5000_i64)))
        .await?;

    let mut extra = std::collections::BTreeMap::new();
    extra.insert("scan_time_ms".to_string(), serde_json::json!(metrics.scan_time_ms));
    extra.insert("rewrite_time_ms".to_string(), serde_json::json!(metrics.rewrite_time_ms));
    extra.insert("num_deleted_rows".to_string(), serde_json::json!(metrics.num_deleted_rows));

    Ok(RunMetrics {
        rows: Some(metrics.num_deleted_rows as u64),
        bytes: None,
        extra,
    })
}

async fn merge_upsert_50pct(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;

    let ctx = df_ctx();

    // Build a source table with half overlapping keys and half new keys
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let mut ids: Vec<i64> = (0..5000_i64).collect(); // match existing
    ids.extend(10_000..15_000); // new keys
    let batch = arrow::array::RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("source", Arc::new(mem))?;
    let df = ctx.table("source").await?;

    let (t2, metrics) = table
        .merge(df, datafusion::prelude::col("target.id").eq(datafusion::prelude::col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|u| u.update("value_i64", datafusion::prelude::lit(1_i64)))?
        .when_not_matched_insert(|i| i.set("id", datafusion::prelude::col("source.id")).set("value_i64", datafusion::prelude::lit(1_i64)))?
        .await?;

    let mut extra = std::collections::BTreeMap::new();
    extra.insert("num_target_updated_rows".to_string(), serde_json::json!(metrics.num_target_updated_rows));
    extra.insert("num_target_inserted_rows".to_string(), serde_json::json!(metrics.num_target_inserted_rows));
    Ok(RunMetrics {
        rows: None,
        bytes: None,
        extra,
    })
}

pub fn cases() -> Vec<CaseDef> {
    vec![
        CaseDef {
            name: "delete_selective_predicate".to_string(),
            suite: "pr".to_string(),
            fixture_relpath: "dml_target_baseline".to_string(),
            run_fn: |p| Pin::from(Box::new(delete_selective(p))),
        },
        CaseDef {
            name: "merge_upsert_50pct".to_string(),
            suite: "nightly".to_string(),
            fixture_relpath: "dml_target_baseline".to_string(),
            run_fn: |p| Pin::from(Box::new(merge_upsert_50pct(p))),
        },
    ]
}
```

### `crates/delta-bench/src/suites/optimize.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;

use deltalake_core::DeltaTableBuilder;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

async fn optimize_compact(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;

    let (t2, metrics) = table.optimize().await?;

    let mut extra = std::collections::BTreeMap::new();
    extra.insert("num_files_added".to_string(), serde_json::json!(metrics.num_files_added));
    extra.insert("num_files_removed".to_string(), serde_json::json!(metrics.num_files_removed));
    extra.insert("partitions_optimized".to_string(), serde_json::json!(metrics.partitions_optimized));
    Ok(RunMetrics {
        rows: None,
        bytes: None,
        extra,
    })
}

pub fn cases() -> Vec<CaseDef> {
    vec![CaseDef {
        name: "optimize_compact_small_files".to_string(),
        suite: "nightly".to_string(),
        fixture_relpath: "many_small_files".to_string(),
        run_fn: |p| Pin::from(Box::new(optimize_compact(p))),
    }]
}
```

### `crates/delta-bench/src/suites/vacuum.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;

use chrono::Duration;
use deltalake_core::DeltaTableBuilder;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

async fn vacuum_dry_run(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;

    let (_t2, metrics) = table
        .vacuum()
        .with_enforce_retention_duration(false)
        .with_retention_period(Duration::hours(0))
        .with_dry_run(true)
        .await?;

    let mut extra = std::collections::BTreeMap::new();
    extra.insert("dry_run".to_string(), serde_json::json!(metrics.dry_run));
    extra.insert("files_deleted_count".to_string(), serde_json::json!(metrics.files_deleted.len()));
    Ok(RunMetrics {
        rows: None,
        bytes: None,
        extra,
    })
}

pub fn cases() -> Vec<CaseDef> {
    vec![CaseDef {
        name: "vacuum_dry_run_lite".to_string(),
        suite: "pr".to_string(),
        fixture_relpath: "post_delete_with_tombstones".to_string(),
        run_fn: |p| Pin::from(Box::new(vacuum_dry_run(p))),
    }]
}
```

The use of `with_enforce_retention_duration(false)` is required to allow short retention periods for testing/benchmarking; delta-rs defaults to safe retention thresholds. citeturn19view1turn19view0

### `crates/delta-bench/src/suites/metadata.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;

use deltalake_core::DeltaTableBuilder;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

async fn table_load(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let _table = DeltaTableBuilder::from_url(url)?.load().await?;
    Ok(RunMetrics::empty())
}

pub fn cases() -> Vec<CaseDef> {
    vec![CaseDef {
        name: "metadata_table_load_cold".to_string(),
        suite: "smoke".to_string(),
        fixture_relpath: "many_commits_with_checkpoint".to_string(),
        run_fn: |p| Pin::from(Box::new(table_load(p))),
    }]
}
```

### `crates/delta-bench/src/suites/checkpoint.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::pin::Pin;

use deltalake_core::protocol::checkpoints::create_checkpoint;
use deltalake_core::DeltaTableBuilder;

use crate::error::BenchResult;
use crate::suites::{CaseDef, RunMetrics};

async fn checkpoint_create(path: std::path::PathBuf) -> BenchResult<RunMetrics> {
    let url = url::Url::from_directory_path(path).unwrap();
    let table = DeltaTableBuilder::from_url(url)?.load().await?;
    create_checkpoint(&table, None).await?;
    Ok(RunMetrics::empty())
}

pub fn cases() -> Vec<CaseDef> {
    vec![CaseDef {
        name: "checkpoint_create_current_version".to_string(),
        suite: "nightly".to_string(),
        fixture_relpath: "many_commits_no_checkpoint".to_string(),
        run_fn: |p| Pin::from(Box::new(checkpoint_create(p))),
    }]
}
```

---

## Data generation code

To keep this deliverable implementable in one PR-sized unit, data generation is intentionally minimal but deterministic; it can be extended without changing the benchmark runner API.

### `crates/delta-bench/src/data/mod.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

pub mod fixtures;
pub mod generator;
pub mod manifest;
pub mod rng;
pub mod schema;

use crate::cli::{Args, DataCmd};
use crate::error::BenchResult;

pub async fn run_data_cmd(args: &Args, cmd: &DataCmd) -> BenchResult<()> {
    fixtures::generate_all(&args.fixtures_dir, &cmd.scale, cmd.seed, cmd.force).await
}
```

### `crates/delta-bench/src/data/manifest.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FixtureManifest {
    pub data_spec_version: u32,
    pub scale: String,
    pub seed: u64,
    pub created_unix_s: i64,
    pub notes: String,
}
```

### `crates/delta-bench/src/data/rng.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use rand_chacha::ChaCha20Rng;
use rand_core::SeedableRng;

pub fn rng(seed: u64) -> ChaCha20Rng {
    ChaCha20Rng::seed_from_u64(seed)
}
```

### `crates/delta-bench/src/data/schema.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

pub fn narrow_sales_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None), false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value_i64", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
    ]))
}
```

### `crates/delta-bench/src/data/generator.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::sync::Arc;

use arrow::array::{BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use rand::Rng;

use crate::data::rng::rng;
use crate::error::BenchResult;

pub fn gen_narrow_sales_batch(seed: u64, offset: i64, n: usize) -> BenchResult<RecordBatch> {
    let mut r = rng(seed);

    let ids: Vec<i64> = (0..n as i64).map(|i| offset + i).collect();
    let ts: Vec<i64> = (0..n as i64).map(|i| 1_700_000_000_000i64 + offset + i).collect();

    // Region is skewed: 70% us, 20% eu, 10% ap
    let region: Vec<&str> = (0..n)
        .map(|_| {
            let x: u8 = r.gen_range(0..100);
            if x < 70 { "us" } else if x < 90 { "eu" } else { "ap" }
        })
        .collect();

    let val: Vec<i64> = (0..n as i64).map(|i| (offset + i) * 3).collect();
    let flag: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();

    let schema = crate::data::schema::narrow_sales_schema();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(StringArray::from(region)),
            Arc::new(Int64Array::from(val)),
            Arc::new(BooleanArray::from(flag)),
        ],
    )?)
}
```

### `crates/delta-bench/src/data/fixtures.rs`

```rust
#![deny(clippy::all, clippy::pedantic)]

use std::path::{Path, PathBuf};

use chrono::Duration;
use deltalake_core::protocol::SaveMode;
use deltalake_core::protocol::checkpoints::create_checkpoint;
use deltalake_core::DeltaTable;

use crate::data::generator::gen_narrow_sales_batch;
use crate::data::manifest::FixtureManifest;
use crate::error::{BenchError, BenchResult};
use crate::fs::{ensure_dir, now_unix_s, write_json_pretty};

async fn write_table(dir: &Path, seed: u64, batches: usize, rows_per_batch: usize) -> BenchResult<()> {
    ensure_dir(dir)?;
    let url = url::Url::from_directory_path(dir).unwrap();
    let table = DeltaTable::try_from_url(url).await?;

    let mut v = vec![];
    for b in 0..batches {
        v.push(gen_narrow_sales_batch(seed, (b * rows_per_batch) as i64, rows_per_batch)?);
    }

    let (_t2, _metrics) = table
        .write(v)
        .with_save_mode(SaveMode::Append)
        .await?;

    Ok(())
}

async fn make_many_commits(dir: &Path, seed: u64, commits: usize) -> BenchResult<()> {
    ensure_dir(dir)?;
    let url = url::Url::from_directory_path(dir).unwrap();
    let mut table = DeltaTable::try_from_url(url).await?;

    // Create an initial table
    let b0 = gen_narrow_sales_batch(seed, 0, 10_000)?;
    let (t1, _m) = table.write(vec![b0]).await?;
    table = t1;

    // Append many small commits
    for i in 0..commits {
        let b = gen_narrow_sales_batch(seed + (i as u64) + 1, (i as i64) * 10_000, 1_000)?;
        let (t2, _m) = table.write(vec![b]).await?;
        table = t2;
    }

    Ok(())
}

async fn make_post_delete_with_tombstones(dir: &Path, seed: u64) -> BenchResult<()> {
    // Build a baseline table then delete to create tombstones.
    write_table(dir, seed, 3, 10_000).await?;
    let url = url::Url::from_directory_path(dir).unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(url)?.load().await?;
    let (_t2, _metrics) = table
        .delete()
        .with_predicate(datafusion::prelude::col("id").gt(datafusion::prelude::lit(5_000_i64)))
        .await?;
    Ok(())
}

pub async fn generate_all(fixtures_root: &str, scale: &str, seed: u64, force: bool) -> BenchResult<()> {
    let sf_dir = PathBuf::from(fixtures_root).join(scale);
    ensure_dir(&sf_dir)?;

    // Empty dir fixture (used by write cases that create their own table)
    let empty = sf_dir.join("empty_dir");
    ensure_dir(&empty)?;

    // Narrow sales partitioned (simple baseline)
    let narrow = sf_dir.join("narrow_sales_partitioned");
    if force || !narrow.join("_delta_log").exists() {
        write_table(&narrow, seed, 5, 20_000).await?;
    }

    // Wide events: for now, reuse narrow schema but stored as separate fixture to allow future expansion.
    let wide = sf_dir.join("wide_events_partitioned");
    if force || !wide.join("_delta_log").exists() {
        write_table(&wide, seed ^ 0xDEADBEEF, 5, 20_000).await?;
    }

    // DML target baseline
    let dml = sf_dir.join("dml_target_baseline");
    if force || !dml.join("_delta_log").exists() {
        write_table(&dml, seed ^ 0xBEEF, 5, 20_000).await?;
    }

    // Many commits fixtures
    let many_nc = sf_dir.join("many_commits_no_checkpoint");
    if force || !many_nc.join("_delta_log").exists() {
        make_many_commits(&many_nc, seed, 200).await?;
    }

    let many_wc = sf_dir.join("many_commits_with_checkpoint");
    if force || !many_wc.join("_delta_log").exists() {
        make_many_commits(&many_wc, seed, 200).await?;
        let url = url::Url::from_directory_path(&many_wc).unwrap();
        let table = deltalake_core::DeltaTableBuilder::from_url(url)?.load().await?;
        create_checkpoint(&table, None).await?;
    }

    // Post-delete tombstones for vacuum
    let tomb = sf_dir.join("post_delete_with_tombstones");
    if force || !tomb.join("_delta_log").exists() {
        make_post_delete_with_tombstones(&tomb, seed).await?;
    }

    let manifest = FixtureManifest {
        data_spec_version: 1,
        scale: scale.to_string(),
        seed,
        created_unix_s: now_unix_s(),
        notes: "delta-bench synthetic fixtures".to_string(),
    };
    write_json_pretty(&sf_dir.join("manifest.json"), &manifest)?;

    println!("Generated fixtures at {}", sf_dir.display());
    Ok(())
}
```

This fixture generator is intentionally conservative (small, deterministic), and is designed to be grown into richer distributions and additional table shapes without changing the suite runner interfaces.

---

## Rust tests

### `crates/delta-bench/tests/determinism.rs`

```rust
use delta_bench::data::generator::gen_narrow_sales_batch;

#[test]
fn deterministic_batches_match() {
    let b1 = gen_narrow_sales_batch(42, 0, 1000).unwrap();
    let b2 = gen_narrow_sales_batch(42, 0, 1000).unwrap();

    assert_eq!(b1.num_rows(), b2.num_rows());
    assert_eq!(format!("{:?}", b1), format!("{:?}", b2));
}
```

### `crates/delta-bench/tests/end_to_end.rs`

```rust
use std::path::PathBuf;

use delta_bench::cli::{Args, Command, DataCmd, RunCmd};
use delta_bench::data::run_data_cmd;
use delta_bench::runner::run_benchmarks;

#[tokio::test]
async fn end_to_end_smoke() {
    let tmp = tempfile::tempdir().unwrap();
    let fixtures = tmp.path().join("fixtures");
    let results = tmp.path().join("results");

    let args = Args {
        fixtures_dir: fixtures.to_string_lossy().to_string(),
        results_dir: results.to_string_lossy().to_string(),
        git_sha: Some("test".to_string()),
        label: Some("e2e".to_string()),
        command: Command::Data(DataCmd {
            scale: "sf1".to_string(),
            seed: 42,
            force: true,
        }),
    };

    run_data_cmd(&args, match &args.command { Command::Data(c) => c, _ => unreachable!() })
        .await
        .unwrap();

    let args2 = Args {
        command: Command::Run(RunCmd {
            target: "smoke".to_string(),
            warmup: 0,
            iterations: 1,
            scale: "sf1".to_string(),
            fail_fast: true,
            output: None,
        }),
        ..args
    };

    run_benchmarks(&args2, match &args2.command { Command::Run(c) => c, _ => unreachable!() })
        .await
        .unwrap();
}
```

---

## Bench scripts and Python tooling

### `benchmarks/bench.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

FIXTURES_DIR="${DELTA_BENCH_FIXTURES:-${SCRIPT_DIR}/fixtures}"
RESULTS_DIR="${DELTA_BENCH_RESULTS:-${SCRIPT_DIR}/results}"

usage() {
  cat <<EOF
delta-rs benchmark runner

Usage:
  ./benchmarks/bench.sh data [--scale sf1|sf10|sf100] [--seed N] [--force]
  ./benchmarks/bench.sh run  [--scale sf1] [--suite smoke|pr|nightly|all|case:<name>] [--warmup N] [--iters N] [--label LABEL]
  ./benchmarks/bench.sh list
  ./benchmarks/bench.sh doctor

Environment:
  DELTA_BENCH_FIXTURES=...  (default: benchmarks/fixtures)
  DELTA_BENCH_RESULTS=...   (default: benchmarks/results)
  DELTA_BENCH_LABEL=...     (default label: local)

EOF
}

cmd="${1:-}"
shift || true

case "${cmd}" in
  data)
    SCALE="sf1"
    SEED="42"
    FORCE=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --scale) SCALE="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        --force) FORCE="--force"; shift 1 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
      esac
    done
    cargo run --release -p delta-bench -- \
      --fixtures-dir "${FIXTURES_DIR}" \
      data --scale "${SCALE}" --seed "${SEED}" ${FORCE}
    ;;
  run)
    SCALE="sf1"
    SUITE="pr"
    WARMUP="1"
    ITERS="5"
    LABEL="${DELTA_BENCH_LABEL:-local}"

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --scale) SCALE="$2"; shift 2 ;;
        --suite) SUITE="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --iters) ITERS="$2"; shift 2 ;;
        --label) LABEL="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
      esac
    done

    GIT_SHA="$(git rev-parse HEAD 2>/dev/null || true)"

    cargo run --release -p delta-bench -- \
      --fixtures-dir "${FIXTURES_DIR}" \
      --results-dir "${RESULTS_DIR}" \
      --label "${LABEL}" \
      --git-sha "${GIT_SHA}" \
      run --scale "${SCALE}" --target "${SUITE}" --warmup "${WARMUP}" --iterations "${ITERS}"
    ;;
  list)
    cargo run --release -p delta-bench -- list all
    ;;
  doctor)
    cargo run --release -p delta-bench -- doctor
    ;;
  *)
    usage
    exit 1
    ;;
esac
```

### `benchmarks/compare_branch.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

BASE_BRANCH="${1:-main}"
PR_BRANCH="${2:-}"
SUITE="${3:-pr}"

if [[ -z "${PR_BRANCH}" ]]; then
  echo "Usage: compare_branch.sh <base_branch> <pr_branch> [suite]"
  exit 1
fi

FIXTURES_DIR="${DELTA_BENCH_FIXTURES:-${SCRIPT_DIR}/fixtures}"
RESULTS_DIR="${DELTA_BENCH_RESULTS:-${SCRIPT_DIR}/results}"
LABEL_BASE="base-${BASE_BRANCH}"
LABEL_PR="pr-${PR_BRANCH}"

echo "Preparing fixtures (if needed)..."
"${SCRIPT_DIR}/bench.sh" data --scale sf1 --seed 42 || true

workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

echo "Running baseline: ${BASE_BRANCH}"
git checkout -f "${BASE_BRANCH}"
"${SCRIPT_DIR}/bench.sh" run --scale sf1 --suite "${SUITE}" --warmup 1 --iters 5 --label "${LABEL_BASE}"

echo "Running candidate: ${PR_BRANCH}"
git checkout -f "${PR_BRANCH}"
"${SCRIPT_DIR}/bench.sh" run --scale sf1 --suite "${SUITE}" --warmup 1 --iters 5 --label "${LABEL_PR}"

BASE_JSON="${RESULTS_DIR}/${LABEL_BASE}/${SUITE}.json"
PR_JSON="${RESULTS_DIR}/${LABEL_PR}/${SUITE}.json"

echo "Comparing:"
echo "  ${BASE_JSON}"
echo "  ${PR_JSON}"

python3 -m delta_bench_compare.compare "${BASE_JSON}" "${PR_JSON}" --noise-threshold 0.05 --format markdown
```

---

## Python: compare tooling

### `benchmarks/python/pyproject.toml`

```toml
[project]
name = "delta-bench-compare"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
  "rich>=13.7.0",
]

[project.scripts]
delta-bench-compare = "delta_bench_compare.compare:main"

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
ignore = ["E203"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

### `benchmarks/python/src/delta_bench_compare/model.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Iteration:
    elapsed_ms: float
    rows: Optional[int]
    bytes: Optional[int]
    extra: Dict[str, Any]

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "Iteration":
        return Iteration(
            elapsed_ms=float(d["elapsed_ms"]),
            rows=int(d["rows"]) if d.get("rows") is not None else None,
            bytes=int(d["bytes"]) if d.get("bytes") is not None else None,
            extra=dict(d.get("extra") or {}),
        )


@dataclass(frozen=True)
class CaseResult:
    case: str
    success: bool
    iterations: List[Iteration]
    error: Optional[str]

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "CaseResult":
        return CaseResult(
            case=str(d["case"]),
            success=bool(d["success"]),
            iterations=[Iteration.from_json(x) for x in (d.get("iterations") or [])],
            error=str(d["error"]) if d.get("error") else None,
        )


@dataclass(frozen=True)
class BenchRun:
    label: str
    results: Dict[str, CaseResult]

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "BenchRun":
        ctx = d.get("context") or {}
        label = str(ctx.get("label") or "unknown")
        out: Dict[str, CaseResult] = {}
        for r in d.get("results") or []:
            cr = CaseResult.from_json(r)
            out[cr.case] = cr
        return BenchRun(label=label, results=out)
```

### `benchmarks/python/src/delta_bench_compare/render.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from rich.table import Table


@dataclass(frozen=True)
class Change:
    classification: str  # faster|slower|no change|incomparable|new|removed
    ratio: Optional[float]  # candidate / baseline


def fmt_ms(x: float) -> str:
    if x >= 1000:
        return f"{x/1000:.2f} s"
    return f"{x:.2f} ms"


def classify(baseline_ms: float, cand_ms: float, noise: float) -> Change:
    if baseline_ms <= 0.0 or cand_ms <= 0.0:
        return Change("incomparable", None)
    ratio = cand_ms / baseline_ms
    if (1.0 - noise) <= ratio <= (1.0 + noise):
        return Change("no change", ratio)
    if ratio < 1.0:
        return Change("faster", ratio)
    return Change("slower", ratio)


def change_text(ch: Change) -> str:
    if ch.classification == "no change":
        return "no change"
    if ch.classification == "incomparable":
        return "incomparable"
    if ch.classification == "new":
        return "new"
    if ch.classification == "removed":
        return "removed"
    assert ch.ratio is not None
    if ch.classification == "faster":
        return f"+{(1.0/ch.ratio):.2f}x faster"
    return f"{ch.ratio:.2f}x slower"
```

### `benchmarks/python/src/delta_bench_compare/compare.py`

```python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

from rich.console import Console
from rich.table import Table

from delta_bench_compare.model import BenchRun
from delta_bench_compare.render import Change, change_text, classify, fmt_ms


def min_time_ms(iterations: List[float]) -> float:
    return min(iterations) if iterations else 0.0


def stats_for_case(run: BenchRun, case: str) -> Tuple[bool, float, str]:
    cr = run.results.get(case)
    if cr is None:
        return False, 0.0, "MISSING"
    if not cr.success:
        return False, 0.0, "FAIL"
    it = [x.elapsed_ms for x in cr.iterations]
    return True, min_time_ms(it), fmt_ms(min_time_ms(it))


def compare(baseline_path: Path, cand_path: Path, noise: float, fmt: str) -> str:
    baseline = BenchRun.from_json(json.loads(baseline_path.read_text()))
    cand = BenchRun.from_json(json.loads(cand_path.read_text()))

    all_cases = sorted(set(baseline.results.keys()) | set(cand.results.keys()))

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Case", style="dim", no_wrap=True)
    table.add_column(baseline.label, justify="right", style="dim")
    table.add_column(cand.label, justify="right", style="dim")
    table.add_column("Change", justify="right", style="dim")

    faster = slower = no_change = failures = new = removed = 0
    total_base = 0.0
    total_cand = 0.0

    for c in all_cases:
        b_ok, b_ms, b_txt = stats_for_case(baseline, c)
        c_ok, c_ms, c_txt = stats_for_case(cand, c)

        if c not in baseline.results:
            table.add_row(c, "—", c_txt, "new")
            new += 1
            continue
        if c not in cand.results:
            table.add_row(c, b_txt, "—", "removed")
            removed += 1
            continue

        if not b_ok or not c_ok:
            table.add_row(c, b_txt, c_txt, "incomparable")
            failures += 1
            continue

        ch = classify(b_ms, c_ms, noise)
        table.add_row(c, b_txt, c_txt, change_text(ch))

        total_base += b_ms
        total_cand += c_ms

        if ch.classification == "no change":
            no_change += 1
        elif ch.classification == "faster":
            faster += 1
        elif ch.classification == "slower":
            slower += 1

    summary = Table(show_header=True, header_style="bold magenta")
    summary.add_column("Benchmark Summary", justify="left", style="dim")
    summary.add_column("", justify="right", style="dim")

    summary.add_row(f"Total Time ({baseline.label})", fmt_ms(total_base))
    summary.add_row(f"Total Time ({cand.label})", fmt_ms(total_cand))
    summary.add_row("Cases Faster", str(faster))
    summary.add_row("Cases Slower", str(slower))
    summary.add_row("Cases with No Change", str(no_change))
    summary.add_row("Cases with Failure", str(failures))
    summary.add_row("Cases New", str(new))
    summary.add_row("Cases Removed", str(removed))

    console = Console(width=200, record=True)
    console.print(table)
    console.print(summary)

    out = console.export_text()
    if fmt == "markdown":
        return f"```\n{out}\n```"
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("baseline", type=Path)
    p.add_argument("candidate", type=Path)
    p.add_argument("--noise-threshold", type=float, default=0.05)
    p.add_argument("--format", choices=["text", "markdown"], default="text")
    args = p.parse_args()

    print(compare(args.baseline, args.candidate, args.noise_threshold, args.format))


if __name__ == "__main__":
    main()
```

### `benchmarks/python/tests/test_compare.py`

```python
from __future__ import annotations

import json
from pathlib import Path

from delta_bench_compare.compare import compare


def test_compare_missing_cases(tmp_path: Path) -> None:
    b = {
        "context": {"label": "base"},
        "results": [{"case": "a", "success": True, "iterations": [{"elapsed_ms": 10, "rows": None, "bytes": None, "extra": {}}]}],
    }
    c = {
        "context": {"label": "cand"},
        "results": [{"case": "b", "success": True, "iterations": [{"elapsed_ms": 9, "rows": None, "bytes": None, "extra": {}}]}],
    }
    bp = tmp_path / "b.json"
    cp = tmp_path / "c.json"
    bp.write_text(json.dumps(b))
    cp.write_text(json.dumps(c))

    out = compare(bp, cp, noise=0.05, fmt="text")
    assert "new" in out
    assert "removed" in out
```

---

## Bot implementation (Option A, recommended)

The design borrows directly from the DataFusion benchmark bot model:

- poll issue comments for trigger phrases
- whitelist users and allowed suites
- generate job scripts and run sequentially
- post results to PR and react to triggering comment

DataFusion’s `scrape_comments.py` demonstrates the core of this: it polls recent PR/issue comments, checks for trigger phrases (“run benchmarks”, “run benchmark …”, “show benchmark queue”), enforces user and benchmark allowlists, and writes job scripts into a `jobs/` directory. citeturn15view2

### `benchmarks/bot/pyproject.toml`

```toml
[project]
name = "delta-bench-bot"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
  "requests>=2.31.0",
]

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
ignore = ["E203"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

### `benchmarks/bot/src/delta_bench_bot/config.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Set


@dataclass(frozen=True)
class BotConfig:
    repo: str
    allowed_users: Set[str]
    allowed_suites: Set[str]
    fixtures_dir: str
    results_dir: str
    delta_rs_dir: str  # path to checked-out delta-rs repo on the VM


def default_config() -> BotConfig:
    return BotConfig(
        repo="delta-io/delta-rs",
        allowed_users=set(),
        allowed_suites={"smoke", "pr", "nightly", "all"},
        fixtures_dir="/opt/delta-bench/fixtures",
        results_dir="/opt/delta-bench/results",
        delta_rs_dir="/opt/delta-bench/delta-rs",
    )
```

### `benchmarks/bot/src/delta_bench_bot/github_api.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass(frozen=True)
class GitHub:
    token: str

    @staticmethod
    def from_env() -> "GitHub":
        tok = os.environ.get("GITHUB_TOKEN")
        if not tok:
            raise RuntimeError("GITHUB_TOKEN is required")
        return GitHub(token=tok)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github+json",
        }

    def list_issue_comments(self, repo: str, since_iso: str) -> List[Dict[str, Any]]:
        url = f"https://api.github.com/repos/{repo}/issues/comments"
        r = requests.get(url, headers=self._headers(), params={"since": since_iso, "per_page": 100})
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            return []
        return data

    def post_comment(self, repo: str, issue_number: int, body: str) -> None:
        url = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
        r = requests.post(url, headers=self._headers(), json={"body": body})
        r.raise_for_status()

    def react(self, repo: str, comment_id: int, content: str) -> None:
        url = f"https://api.github.com/repos/{repo}/issues/comments/{comment_id}/reactions"
        r = requests.post(url, headers=self._headers(), json={"content": content})
        r.raise_for_status()
```

### `benchmarks/bot/src/delta_bench_bot/parser.py`

```python
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

_ENV_RE = re.compile(r"^[A-Z_][A-Z0-9_]*=[a-zA-Z0-9._\-]+$")


@dataclass(frozen=True)
class Request:
    kind: str  # run|queue|cancel
    targets: List[str]
    env: List[str]
    job_id: Optional[str]


def parse_request(body: str) -> Optional[Request]:
    lines = [ln.rstrip() for ln in body.strip().splitlines() if ln.strip()]
    if not lines:
        return None

    first = lines[0].strip()

    if first.lower() == "show benchmark queue":
        return Request(kind="queue", targets=[], env=[], job_id=None)

    m = re.match(r"^\s*cancel\s+benchmark\s+([0-9A-Za-z._\-]+)\s*$", first, flags=re.I)
    if m:
        return Request(kind="cancel", targets=[], env=[], job_id=m.group(1))

    m = re.match(r"^\s*run\s+benchmark\s+(.+?)\s*$", first, flags=re.I)
    if not m:
        return None

    targets = [t for t in m.group(1).split() if t]
    env = [ln for ln in lines[1:] if _ENV_RE.match(ln)]
    return Request(kind="run", targets=targets, env=env, job_id=None)
```

### `benchmarks/bot/src/delta_bench_bot/queue.py`

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class JobMeta:
    job_id: str
    pr_number: int
    user: str
    targets: str
    comment_url: str


def jobs_dir(base: str) -> Path:
    p = Path(base) / "jobs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def pending_jobs(base: str) -> List[Path]:
    p = jobs_dir(base)
    return sorted([x for x in p.glob("*.sh") if x.is_file()])


def read_meta(path: Path) -> JobMeta:
    pr = 0
    user = "unknown"
    targets = "unknown"
    comment_url = "unknown"
    for ln in path.read_text().splitlines():
        if ln.startswith("# PR:"):
            pr = int(ln.split(":", 1)[1].strip().lstrip("#").strip())
        if ln.startswith("# User:"):
            user = ln.split(":", 1)[1].strip()
        if ln.startswith("# Targets:"):
            targets = ln.split(":", 1)[1].strip()
        if ln.startswith("# Comment:"):
            comment_url = ln.split(":", 1)[1].strip()
    return JobMeta(job_id=path.stem, pr_number=pr, user=user, targets=targets, comment_url=comment_url)
```

### `benchmarks/bot/src/delta_bench_bot/scrape_comments.py`

```python
from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any, Dict

from delta_bench_bot.config import BotConfig, default_config
from delta_bench_bot.github_api import GitHub
from delta_bench_bot.parser import parse_request
from delta_bench_bot.queue import jobs_dir, pending_jobs, read_meta


def iso_utc(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0, tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def issue_number_from_issue_url(issue_url: str) -> int:
    # https://api.github.com/repos/<owner>/<repo>/issues/<num>
    return int(issue_url.rstrip("/").split("/")[-1])


def write_job(base_dir: str, job_id: str, script: str) -> Path:
    p = jobs_dir(base_dir) / f"{job_id}.sh"
    p.write_text(script)
    os.chmod(p, 0o755)
    return p


def main() -> None:
    cfg = default_config()
    gh = GitHub.from_env()

    window_s = int(os.environ.get("DELTA_BENCH_BOT_WINDOW_S", "3600"))
    now = dt.datetime.now(dt.timezone.utc)
    since = now - dt.timedelta(seconds=window_s)

    comments = gh.list_issue_comments(cfg.repo, iso_utc(since))
    for c in comments:
        body = str(c.get("body") or "")
        req = parse_request(body)
        if req is None:
            continue

        comment_id = int(c["id"])
        user = str((c.get("user") or {}).get("login") or "")
        issue_url = str(c.get("issue_url") or "")
        comment_url = str(c.get("html_url") or "")
        pr_number = issue_number_from_issue_url(issue_url)

        if user not in cfg.allowed_users:
            gh.post_comment(
                cfg.repo,
                pr_number,
                f"Hi @{user}, this bot only responds to a whitelisted set of users.",
            )
            continue

        if req.kind == "queue":
            metas = [read_meta(p) for p in pending_jobs(cfg.delta_rs_dir)]
            if not metas:
                gh.post_comment(cfg.repo, pr_number, "No pending benchmark jobs.")
            else:
                lines = ["| Job | User | Targets | Comment |", "| --- | --- | --- | --- |"]
                for m in metas:
                    lines.append(f"| `{m.job_id}` | {m.user} | {m.targets} | {m.comment_url} |")
                gh.post_comment(cfg.repo, pr_number, "\n".join(lines))
            continue

        if req.kind == "cancel" and req.job_id:
            p = jobs_dir(cfg.delta_rs_dir) / f"{req.job_id}.sh"
            if p.exists():
                p.unlink()
                gh.post_comment(cfg.repo, pr_number, f"Canceled job `{req.job_id}`.")
            else:
                gh.post_comment(cfg.repo, pr_number, f"Job `{req.job_id}` not found.")
            continue

        # run request
        targets = req.targets if req.targets else ["pr"]
        bad = [t for t in targets if t not in cfg.allowed_suites and not t.startswith("case:")]
        if bad:
            gh.post_comment(cfg.repo, pr_number, f"Unsupported targets: {', '.join(bad)}")
            continue

        gh.react(cfg.repo, comment_id, "rocket")

        job_id = f"{pr_number}_{comment_id}"
        export_env = "\n".join([f"export {e}" for e in req.env])
        run_lines = "\n".join([f'./benchmarks/bench.sh run --scale sf1 --suite "{t}" --label "pr-{pr_number}"' for t in targets])

        script = f"""#!/usr/bin/env bash
set -euo pipefail

# PR: {pr_number}
# User: {user}
# Targets: {' '.join(targets)}
# Comment: {comment_url}

cd "{cfg.delta_rs_dir}"

{export_env}

# Ensure fixtures exist
DELTA_BENCH_FIXTURES="{cfg.fixtures_dir}" DELTA_BENCH_RESULTS="{cfg.results_dir}" ./benchmarks/bench.sh data --scale sf1 --seed 42 || true

{run_lines}

echo "Done"
"""

        write_job(cfg.delta_rs_dir, job_id, script)
        gh.post_comment(
            cfg.repo,
            pr_number,
            f"Queued benchmark job `{job_id}` for targets: {' '.join(targets)}.\nComment: {comment_url}",
        )


if __name__ == "__main__":
    main()
```

### `benchmarks/bot/src/delta_bench_bot/worker.py`

```python
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from delta_bench_bot.config import default_config
from delta_bench_bot.github_api import GitHub
from delta_bench_bot.queue import jobs_dir, pending_jobs, read_meta


def main() -> None:
    cfg = default_config()
    gh = GitHub.from_env()
    base = cfg.delta_rs_dir

    while True:
        jobs = pending_jobs(base)
        if not jobs:
            time.sleep(5)
            continue

        job = jobs[0]
        meta = read_meta(job)
        running = jobs_dir(base) / f"{meta.job_id}.running"
        done = jobs_dir(base) / f"{meta.job_id}.done"
        failed = jobs_dir(base) / f"{meta.job_id}.failed"

        job.rename(running)

        try:
            proc = subprocess.run(["bash", str(running)], capture_output=True, text=True, check=False)
            output_tail = "\n".join((proc.stdout + "\n" + proc.stderr).splitlines()[-60:])

            if proc.returncode == 0:
                running.rename(done)
                gh.post_comment(
                    cfg.repo,
                    meta.pr_number,
                    f"Benchmark job `{meta.job_id}` completed.\n\n```text\n{output_tail}\n```",
                )
            else:
                running.rename(failed)
                gh.post_comment(
                    cfg.repo,
                    meta.pr_number,
                    f"Benchmark job `{meta.job_id}` FAILED (exit {proc.returncode}).\n\n```text\n{output_tail}\n```",
                )
        except Exception as e:
            if running.exists():
                running.rename(failed)
            gh.post_comment(cfg.repo, meta.pr_number, f"Benchmark worker exception: {e!r}")

        time.sleep(2)


if __name__ == "__main__":
    main()
```

---

## GitHub Actions workflow (Option B)

### `.github/workflows/benchmark.yml`

```yaml
name: PR benchmarks (self-hosted)

on:
  issue_comment:
    types: [created]

jobs:
  benchmark:
    if: github.event.issue.pull_request != null && startsWith(github.event.comment.body, 'run benchmark')
    runs-on: [self-hosted, delta-bench]
    concurrency:
      group: delta-bench
      cancel-in-progress: false

    steps:
      - name: Parse request
        id: parse
        shell: bash
        run: |
          set -euo pipefail
          BODY="${{ github.event.comment.body }}"
          FIRST_LINE="$(printf "%s" "$BODY" | head -n 1)"
          TARGETS="$(echo "$FIRST_LINE" | awk '{$1=$2=""; sub(/^  */, ""); print}')"
          echo "targets=$TARGETS" >> "$GITHUB_OUTPUT"

      - name: Checkout PR branch
        uses: actions/checkout@v4
        with:
          ref: ${{ github.event.issue.pull_request.head.ref }}
          fetch-depth: 0

      - name: Run benchmarks
        shell: bash
        env:
          DELTA_BENCH_FIXTURES: /opt/delta-bench/fixtures
          DELTA_BENCH_RESULTS: /opt/delta-bench/results
          DELTA_BENCH_LABEL: pr-${{ github.event.issue.number }}
        run: |
          set -euo pipefail
          ./benchmarks/bench.sh data --scale sf1 --seed 42 || true
          ./benchmarks/bench.sh run --scale sf1 --suite "${{ steps.parse.outputs.targets }}" --warmup 1 --iters 5 --label "pr-${{ github.event.issue.number }}"

      - name: Post results
        shell: bash
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -euo pipefail
          echo "Benchmarks completed. Results are in /opt/delta-bench/results."
          gh pr comment "${{ github.event.issue.number }}" --body "Benchmarks completed on self-hosted runner."
```

Option B is operationally simpler, but lacks the richer queue UX that Option A supports, which is why Option A is recommended.

---

## VM provisioning

### `benchmarks/bot/vm/provision_ubuntu_2404.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# Provision a dedicated delta-bench VM (Ubuntu 24.04).
# Assumes you run as root.

apt-get update
apt-get install -y \
  git curl ca-certificates jq python3 python3-venv python3-pip \
  build-essential pkg-config clang lld \
  linux-tools-common linux-tools-generic \
  cpufrequtils

# Install Rust (stable)
if ! command -v rustup >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi
source /root/.cargo/env
rustup default stable

# Create directories
mkdir -p /opt/delta-bench/{fixtures,results}
mkdir -p /opt/delta-bench/delta-rs

# Clone delta-rs
if [[ ! -d /opt/delta-bench/delta-rs/.git ]]; then
  git clone https://github.com/delta-io/delta-rs.git /opt/delta-bench/delta-rs
fi

# CPU governor: performance (best-effort)
if command -v cpufreq-set >/dev/null 2>&1; then
  cpufreq-set -r -g performance || true
fi

# Install bot venv
cd /opt/delta-bench/delta-rs/benchmarks/bot
python3 -m venv .venv
./.venv/bin/pip install -U pip
./.venv/bin/pip install -e .

echo "Provisioning complete."
echo "Next: configure systemd services and export GITHUB_TOKEN."
```

### `benchmarks/bot/systemd/delta-bench-scraper.service`

```ini
[Unit]
Description=delta-bench PR comment scraper
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/delta-bench/delta-rs/benchmarks/bot
Environment=GITHUB_TOKEN=REPLACE_ME
ExecStart=/opt/delta-bench/delta-rs/benchmarks/bot/.venv/bin/python -m delta_bench_bot.scrape_comments
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### `benchmarks/bot/systemd/delta-bench-scraper.timer`

```ini
[Unit]
Description=Run delta-bench scraper every minute

[Timer]
OnBootSec=1min
OnUnitActiveSec=1min
Persistent=true

[Install]
WantedBy=timers.target
```

### `benchmarks/bot/systemd/delta-bench-worker.service`

```ini
[Unit]
Description=delta-bench job worker
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/delta-bench/delta-rs/benchmarks/bot
Environment=GITHUB_TOKEN=REPLACE_ME
ExecStart=/opt/delta-bench/delta-rs/benchmarks/bot/.venv/bin/python -m delta_bench_bot.worker
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
```

---

## Documentation

### `benchmarks/README.md`

```md
# delta-rs Benchmarks

This directory contains a production-grade macro-benchmark harness and an optional PR benchmark bot.

## Quickstart (local)

```bash
# Generate fixtures (SF1)
./benchmarks/bench.sh data --scale sf1 --seed 42

# Run PR suite locally (directional results)
./benchmarks/bench.sh run --scale sf1 --suite pr --warmup 1 --iters 5 --label local
```

Results are written to `benchmarks/results/<label>/`.

## Compare two runs

```bash
python3 -m delta_bench_compare.compare benchmarks/results/main/pr.json benchmarks/results/pr-123/pr.json --format markdown
```

## Adding a new benchmark case

1. Add one `CaseDef` entry in `crates/delta-bench/src/suites/<area>.rs`.
2. Ensure the fixture exists in `crates/delta-bench/src/data/fixtures.rs`.
3. Run `./benchmarks/bench.sh list` to confirm your case appears.
4. Add a test if the case involves non-trivial logic.

## Bot

See `benchmarks/bot/README.md`.
```

### `benchmarks/bot/README.md`

```md
# delta-bench Bot

A lightweight PR benchmark bot inspired by the DataFusion benchmark bot model:

- Poll PR comments
- Enforce allowlist of users
- Queue jobs
- Run sequentially on a dedicated machine
- Post results back to the PR

## Commands

- run benchmark pr
- run benchmark smoke
- run benchmark case:delete_selective_predicate
- show benchmark queue
- cancel benchmark <job_id>

## Deploy

1. Provision VM via `vm/provision_ubuntu_2404.sh`
2. Set `GITHUB_TOKEN` with repo comment permissions
3. Configure allowlist in `config.py`
4. Install and enable systemd units in `systemd/`
```

---

## Example end-to-end output

Example GitHub comment body posted by the bot after a run:

```text
Comparing main (b8a1c2d) to pr-1234 (d50740f)
Suite: pr  Scale: sf1  Iterations: warmup=1, measured=5

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Case                          ┃       main ┃      pr-1234   ┃        Change ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ read_full_scan_narrow         │  120.08 ms │     98.32 ms  │ +1.22x faster │
│ read_projection_wide_5cols    │   80.41 ms │     81.92 ms  │     no change │
│ delete_selective_predicate    │  450.56 ms │    612.88 ms  │  1.36x slower │
│ vacuum_dry_run_lite           │      FAIL  │     45.12 ms  │  incomparable │
└───────────────────────────────┴────────────┴───────────────┴───────────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Benchmark Summary                    ┃            ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Total Time (main)                    │  651.05 ms │
│ Total Time (pr-1234)                 │  793.12 ms │
│ Cases Faster                         │          1 │
│ Cases Slower                         │          1 │
│ Cases with No Change                 │          1 │
│ Cases with Failure                   │          1 │
│ Cases New                            │          0 │
│ Cases Removed                        │          0 │
└──────────────────────────────────────┴────────────┘
```

This mirrors the DataFusion table style and classification logic (faster/slower/no change/incomparable, with a noise threshold), implemented with `rich`. citeturn9view0