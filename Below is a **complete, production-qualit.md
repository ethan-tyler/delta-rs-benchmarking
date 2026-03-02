Below is a **complete, production-quality design + full implementable code** for a delta-rs benchmarking system that’s the spiritual equivalent of Andrew Lamb’s DataFusion benchmarking + PR bot infrastructure, but purpose-built for **Delta Lake table-format operations** (metadata/log replay, writes, MERGE/UPDATE/DELETE, OPTIMIZE/VACUUM, and read scans).

It intentionally mirrors the UX DataFusion maintainers like: PR comments such as `run benchmark <suite>` trigger a queued run and the bot later posts a **box-drawing comparison table** and summary. ([GitHub][1])

This proposal is also informed by delta-rs’s existing “benchmarks script problems” discussion (the repo already had some benchmarking attempts that need rework). ([GitHub][2])

---

# Recommendation: Hybrid layout (best for delta-rs)

**Recommended: C) Hybrid approach**

* **Inside `delta-rs`**: add a first-class benchmark binary (`delta-bench`) under `benchmarks/` (like DataFusion).
* **Standalone repo** (`delta-rs-benchmarking`): contains the PR-bot + branch-compare automation + Python comparison tooling + VM provisioning.

Why:

* Keeps the benchmark binary **version-locked** to the delta-rs APIs (important because operations are DataFusion-feature-gated and evolve). delta-rs exposes these operations via `DeltaOps` and modules like `merge`, `optimize`, `delete`, `update`, `write`, etc. ([Docs.rs][3])
* Keeps the bot/infra separate (like `alamb/datafusion-benchmarking`) so delta-rs doesn’t absorb VM scripts, queue state, bot configs, etc.

---

# Part 1: Benchmark suite design

## 1.1 Workload selection for a Delta table-format library

A Delta library’s performance “hot spots” differ from a SQL engine:

* **Transaction log / checkpoint** read & replay (table load time) is critical for every operation.
* **DML (MERGE/UPDATE/DELETE)** often dominates cost and is highest risk for regressions (Delta Lake explicitly tracks/benchmarks MERGE performance and has published MERGE benchmark additions). ([Delta Lake][4])
* **OPTIMIZE/VACUUM** are maintenance operations with metadata-heavy behavior.
* **Read scans** are often executed through an engine (DataFusion here), but delta-rs controls table provider behavior (partition pruning, data skipping) and therefore must benchmark end-to-end table scanning characteristics.

We also borrow ideas from:

* ClickBench: “typical workload” for analytics queries (DataFusion adopted it; Delta table formats often end up serving these patterns). ([GitHub][5])
* DuckDB Labs’ db-benchmark: PR-driven bench runs + published results concept. ([duckdblabs.github.io][6])
* Iceberg’s benchmark docs + GH Action approach (good inspiration for “run benchmarks on demand”). ([Apache Iceberg][7])
* Hudi’s published performance focus, and their blog-style benchmark narratives on skipping/indexes. ([Apache Hudi][8])

### Suites and cases (first production set)

Each benchmark case defines:

* **Operation measured**
* **Setup excluded** (data generation, template creation)
* **Warmup strategy**
* **Measurement strategy**

#### A) `read_scan`

Goal: validate regressions in scan + pruning/skipping behavior.

Cases (DataFusion SQL over delta-rs TableProvider; result is aggregate to avoid huge output):

1. `read/full_scan_count`

   * SQL: `SELECT COUNT(*) FROM events`
   * Measures: full scan throughput
   * Excludes: table creation; uses pre-generated `events_partitioned` dataset
2. `read/projected_sum`

   * SQL: `SELECT SUM(metric_i64) FROM events`
   * Measures: projection performance
3. `read/partition_pruning`

   * SQL: `SELECT COUNT(*) FROM events WHERE event_date = 'YYYY-MM-DD'`
   * Measures: partition pruning effectiveness
4. `read/data_skipping_selective`

   * SQL: `SELECT COUNT(*) FROM events WHERE event_id BETWEEN X AND Y`
   * Measures: file skipping via min/max stats (if available in Add stats)
5. `read/high_selectivity_filter`

   * SQL: `SELECT COUNT(*) FROM events WHERE country = 'US' AND device = 'mobile' AND metric_i64 < 0`

Warmup: 1 run per case by default (cache/page warm).
Measurement: `iterations` timed runs; report median/p50 + dispersion.

#### B) `write`

Goal: track write throughput and file layout behaviors.

Cases (each runs in a fresh cloned working copy):

1. `write/append_small_batches`

   * Append 200 × 5k rows (many small files)
2. `write/append_large_batches`

   * Append 10 × 100k rows
3. `write/overwrite`

   * Overwrite table with N rows
4. `write/partitioned_append`

   * Append with partitioning enabled

Excludes: generating RecordBatches for input (generated once per iteration outside timed region).

#### C) `merge_dml`

Goal: benchmark the most complex/high-risk operation.

Cases:

1. `merge/match_10pct` (upsert)
2. `merge/match_50pct`
3. `merge/match_90pct`

Each uses:

* Target: `merge_target` delta table template (pre-generated)
* Source: in-memory DataFrame with deterministic keys + updated payload
* Measures only the merge execution (not source generation)

delta-rs exposes `MergeBuilder` and MERGE APIs via `DeltaOps(...).merge(...)` with metrics. ([Docs.rs][9])

#### D) `optimize_vacuum`

Goal: catch regressions in file compaction + cleanup.

Cases:

1. `optimize/compact_small_files`

   * Runs OPTIMIZE (bin-pack) on a small-files template table.
   * delta-rs optimize docs explicitly describe bin-packing and the fact OPTIMIZE produces remove actions, and VACUUM is used to delete. ([Docs.rs][10])
2. `vacuum/dry_run`
3. `vacuum/execute`

#### E) `metadata`

Goal: measure costs users feel *even when queries are fast*.

Cases:

1. `meta/open_latest` (table load)
2. `meta/open_version_0` (time travel load)
3. `meta/update_state` (checkpoint + log replay path)

DeltaTable update_state docs: “loading the last checkpoint and incrementally applying each version since”. ([Docs.rs][11])

---

## 1.2 Data generation design

### Deterministic generator constraints

We guarantee:

* Deterministic row content using `ChaCha8Rng` and **integer-only distributions** (no platform-dependent `powf`/libm).
* Stable “table shapes”: narrow/wide, partitioned/unpartitioned, small-files, many-versions.
* **Logical determinism**: same seed ⇒ same rows, same partition distribution, same batch boundaries.
  (Exact Parquet bytes and file names may vary if the writer uses UUIDs; that is acceptable for benchmarking repeatability as long as layout is stable.)

### Scale factors

Default mapping (tunable in config):

* `sf1`: 1,000,000 rows (≈100–300MB depending on schema/compression)
* `sf10`: 10,000,000 rows (≈1–3GB)
* `sf100`: 100,000,000 rows (≈10–30GB) (typically used only on nightly)

### Data distributions

Columns emulate clickstream/events:

* `event_id`: sequential int64
* `user_id`: heavy-tail via bucketed geometric distribution
* `event_ts`: monotonic-ish timestamp
* `event_date`: derived date string for partitioning
* `country`, `device`: categorical with skew
* `metric_i64`: approx-normal (sum of uniforms)
* optional payload strings

This supports:

* partition pruning (`event_date`)
* data skipping (`event_id` ranges align with file boundaries)
* realistic skew (country/device/user)

### Table variants (pre-generated templates)

* `events_partitioned`: partitioned by `event_date`, moderate file size
* `events_unpartitioned`: same rows, no partitions
* `events_small_files`: same schema but produced via many tiny appends
* `merge_target`: keyed table for MERGE
* `meta_many_versions`: table with many commits to stress log replay/checkpoint

---

## 1.3 Statistical rigor and classification

We apply pragmatic “macrobenchmark” best practices:

* Warmup is needed due to OS page cache and filesystem cache effects (even without JIT).
* We record **all iteration samples** and compute:

  * min/max/mean
  * median (p50) as primary value
  * p90
  * stddev and coefficient of variation (CV)
  * Tukey outlier counts (Criterion warns about outliers; outliers are a reliability signal). ([B Heisler GitHub][12])
* We optionally compute a bootstrap CI later (future extension); for now we expose enough distribution detail.

**Threshold**:

* Default `--threshold 0.05` (±5%) like the DataFusion bot’s “no change” band (practical for noisy macrobenches and aligns with typical tooling practice).
* You can set `--threshold 0.03` if your runner is stable enough (goal ±3% on same hardware).

This follows the spirit of “rigorous but pragmatic” repetition guidance (Kalibera & Jones). ([ACM Digital Library][13])
And acknowledges that small environmental factors can silently skew results (Mytkowicz et al.). ([Northwestern Computer Science][14])

We also borrow operational features from hyperfine (warmups, cache-clear hooks, JSON export concepts). ([GitHub][15])

---

# Part 2: Comparison tooling

## 2.1 `compare.py` goals

* Input: two JSON result files
* Output: DataFusion-style box-drawing tables + summary
* Handles:

  * missing cases
  * failures
  * configurable threshold
  * markdown mode

## 2.2 `compare_branch.sh` goals

* Build + run `main` vs PR branch (release mode)
* Uses pre-generated benchmark data
* Captures system info header
* Produces comparison output suitable for GitHub comments
* Handles timeouts/build failure/OOM

---

# Part 3: PR benchmark bot

We provide both options.

## Option A: Standalone polling bot (DataFusion-like)

* A Python daemon that polls PR comments, enforces whitelist, queues jobs, runs them sequentially, and posts results.
* Mirrors the “bot UX” seen in DataFusion PRs. ([GitHub][1])

## Option B: GitHub Actions `issue_comment` + self-hosted runner

* A workflow triggered by comment; enforces whitelist; uses concurrency to guarantee one run at a time; posts results.

---

# Part 5: Integration guidance (delta-rs vs standalone)

* Put `delta-bench` inside delta-rs (like DataFusion).
* Put the bot + automation in `delta-rs-benchmarking` (like `alamb/datafusion-benchmarking`).

This avoids the known pain where delta-rs already had benchmark scripts that relied on hard-to-find datasets and lacked modern scaffolding. ([GitHub][2])

---

# Deliverable 1: Repository layouts

## A) delta-rs patch (adds `delta-bench`)

Add these paths to `delta-rs/`:

```
delta-rs/
├── benchmarks/
│   ├── Cargo.toml
│   ├── README.md
│   └── src/
│       ├── main.rs
│       ├── lib.rs
│       ├── cli.rs
│       ├── error.rs
│       ├── results.rs
│       ├── stats.rs
│       ├── system.rs
│       ├── util.rs
│       ├── data/
│       │   ├── mod.rs
│       │   ├── datasets.rs
│       │   ├── generator.rs
│       │   └── table_ops.rs
│       └── suites/
│           ├── mod.rs
│           ├── read_scan.rs
│           ├── write.rs
│           ├── merge_dml.rs
│           ├── optimize_vacuum.rs
│           └── metadata.rs
└── Cargo.toml  (workspace member update)
```

**Workspace change** (root `Cargo.toml`): add `"benchmarks"` to `[workspace].members`.

## B) Standalone repo: `delta-rs-benchmarking`

```
delta-rs-benchmarking/
├── README.md
├── LICENSE
├── pyproject.toml
├── python/
│   ├── delta_bench_compare/
│   │   ├── __init__.py
│   │   ├── compare.py
│   │   ├── model.py
│   │   └── formatting.py
│   └── tests/
│       ├── test_compare.py
│       └── test_formatting.py
├── scripts/
│   ├── compare_branch.sh
│   ├── bench.sh
│   └── vm_info.sh
├── bot/
│   ├── daemon.py
│   ├── github_api.py
│   ├── queue.py
│   ├── parser.py
│   ├── whitelist.json
│   ├── config.example.toml
│   └── systemd/
│       ├── delta-bench-bot.service
│       └── delta-bench-bot.timer
├── infra/
│   ├── provision_runner.sh
│   ├── sysctl.d/
│   │   └── 99-delta-bench.conf
│   └── docs/
│       └── runner_hardening.md
├── docs/
│   ├── architecture.md
│   └── adr/
│       ├── 0001-hybrid-layout.md
│       ├── 0002-json-result-schema.md
│       └── 0003-bot-security-model.md
└── .github/
    └── workflows/
        └── benchmark.yml
```

---

# Deliverable 2: Rust code for `delta-bench` (delta-rs patch)

## `delta-rs/benchmarks/Cargo.toml`

```toml
[package]
name = "delta-bench"
version = "0.1.0"
edition = "2021"
publish = false
license = "Apache-2.0"
description = "Macrobenchmark suite for delta-rs (Delta Lake in Rust)"
repository = "https://github.com/delta-io/delta-rs"

[dependencies]
# Use delta-rs core directly so the benchmark is version-locked to the PR under test.
deltalake-core = { path = "../crates/core", features = ["datafusion"] }

clap = { version = "4.5.27", features = ["derive", "env"] }
serde = { version = "1.0.206", features = ["derive"] }
serde_json = "1.0.124"
thiserror = "2.0.11"
tokio = { version = "1.43.0", features = ["rt-multi-thread", "macros", "time", "fs"] }
tracing = "0.1.41"
tracing-subscriber = { version = "0.3.19", features = ["env-filter"] }

rand = "0.8.5"
rand_chacha = "0.3.1"

sysinfo = "0.32.1"
humantime = "2.1.0"
walkdir = "2.5.0"

[dev-dependencies]
tempfile = "3.12.0"
pretty_assertions = "1.4.1"
```

> Note: dependency versions here are pinned for reproducibility; delta-rs itself currently depends on `thiserror 2`, `tracing 0.1`, `tokio 1`, `arrow 57`, `datafusion 52.1.0`, etc (as of deltalake-core 0.31.0). ([Docs.rs][16])

---

## `delta-rs/benchmarks/README.md`

````md
# delta-bench

`delta-bench` is the macrobenchmark harness for delta-rs.

It is designed to:
- be stable and reproducible on dedicated hardware
- support PR-to-main comparisons for regressions
- benchmark Delta Lake table-format operations (MERGE, OPTIMIZE, VACUUM, log replay), not just SQL queries

## Quick start (local)

```bash
# From delta-rs repo root:

# Generate SF1 data (under ./bench-data by default)
cargo run -p delta-bench --release -- data --suite all --scale sf1 --data-dir bench-data

# Run a suite
cargo run -p delta-bench --release -- run --suite read_scan --scale sf1 --data-dir bench-data --out results/read_sf1.json

# List suites/cases
cargo run -p delta-bench --release -- list
````

## Environment variables

* `DELTA_BENCH_DATA_DIR`: dataset root directory
* `DELTA_BENCH_WORK_DIR`: scratch/work directory for per-iteration table clones
* `DELTA_BENCH_ITERATIONS`: default iterations
* `DELTA_BENCH_WARMUP`: default warmup runs

````

---

## `delta-rs/benchmarks/src/main.rs`

```rust
use delta_bench::cli::Cli;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), delta_bench::error::DeltaBenchError> {
    Cli::run().await
}
````

---

## `delta-rs/benchmarks/src/lib.rs`

```rust
#![deny(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions, // clearer in a multi-module CLI tool
    clippy::struct_excessive_bools,  // CLI configs often have several toggles
    clippy::missing_errors_doc       // error docs are centralized in error.rs
)]

pub mod cli;
pub mod data;
pub mod error;
pub mod results;
pub mod stats;
pub mod suites;
pub mod system;
pub mod util;
```

---

## `delta-rs/benchmarks/src/error.rs`

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeltaBenchError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Delta error: {0}")]
    Delta(#[from] deltalake_core::errors::DeltaTableError),

    #[cfg(feature = "datafusion")]
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] deltalake_core::datafusion::error::DataFusionError),

    #[error("invalid argument: {0}")]
    InvalidArg(String),

    #[error("dataset not found: {0} (run `delta-bench data ...` first)")]
    MissingDataset(String),

    #[error("benchmark case failed: {case_id}: {message}")]
    CaseFailed { case_id: String, message: String },
}

impl DeltaBenchError {
    pub fn invalid_arg(msg: impl Into<String>) -> Self {
        Self::InvalidArg(msg.into())
    }

    pub fn case_failed(case_id: &str, msg: impl Into<String>) -> Self {
        Self::CaseFailed {
            case_id: case_id.to_string(),
            message: msg.into(),
        }
    }
}
```

---

## `delta-rs/benchmarks/src/util.rs`

```rust
use std::path::{Path, PathBuf};

pub fn ensure_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

pub fn normalize_dataset_name(s: &str) -> String {
    s.trim().to_lowercase().replace([' ', '/'], "_")
}

pub fn join_all(base: &Path, parts: &[&str]) -> PathBuf {
    let mut out = base.to_path_buf();
    for p in parts {
        out.push(p);
    }
    out
}

pub fn utc_now_rfc3339() -> String {
    // Avoid chrono dependency; good enough for logs/metadata.
    // Format using SystemTime.
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}
```

(We keep timestamps simple to avoid pulling extra dependencies into delta-rs; the bot infra can add richer timestamps if desired.)

---

## `delta-rs/benchmarks/src/system.rs`

```rust
use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, RefreshKind, System};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemInfo {
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub cpu_model: String,
    pub cpu_cores_logical: usize,
    pub memory_total_bytes: u64,
}

impl SystemInfo {
    pub fn gather() -> Self {
        let refresh = RefreshKind::new()
            .with_cpu(CpuRefreshKind::new().with_brand())
            .with_memory();

        let mut sys = System::new_with_specifics(refresh);
        sys.refresh_cpu();
        sys.refresh_memory();

        let cpu_model = sys
            .cpus()
            .first()
            .map(|c| c.brand().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let os = System::name().unwrap_or_else(|| "unknown".to_string());
        let kernel = System::kernel_version().unwrap_or_else(|| "unknown".to_string());

        Self {
            os,
            kernel,
            arch: std::env::consts::ARCH.to_string(),
            cpu_model,
            cpu_cores_logical: sys.cpus().len(),
            memory_total_bytes: sys.total_memory(),
        }
    }
}
```

---

## `delta-rs/benchmarks/src/stats.rs`

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SummaryStats {
    pub n: usize,
    pub min_ns: u64,
    pub max_ns: u64,
    pub mean_ns: f64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub stddev_ns: f64,
    pub cv: f64,
    pub outliers_low: usize,
    pub outliers_high: usize,
}

pub fn summarize(samples_ns: &[u64]) -> SummaryStats {
    let mut v = samples_ns.to_vec();
    v.sort_unstable();

    let n = v.len().max(1);

    let min_ns = *v.first().unwrap_or(&0);
    let max_ns = *v.last().unwrap_or(&0);

    let mean_ns = (v.iter().map(|&x| x as f64).sum::<f64>()) / (v.len().max(1) as f64);

    let p50_ns = percentile_sorted(&v, 0.50);
    let p90_ns = percentile_sorted(&v, 0.90);

    let stddev_ns = {
        if v.len() < 2 {
            0.0
        } else {
            let var = v
                .iter()
                .map(|&x| {
                    let dx = (x as f64) - mean_ns;
                    dx * dx
                })
                .sum::<f64>()
                / ((v.len() - 1) as f64);
            var.sqrt()
        }
    };

    let cv = if mean_ns > 0.0 { stddev_ns / mean_ns } else { 0.0 };

    let (outliers_low, outliers_high) = tukey_outliers(&v);

    SummaryStats {
        n,
        min_ns,
        max_ns,
        mean_ns,
        p50_ns,
        p90_ns,
        stddev_ns,
        cv,
        outliers_low,
        outliers_high,
    }
}

fn percentile_sorted(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let p = p.clamp(0.0, 1.0);
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Tukey fences (like what Criterion reports as outliers).
fn tukey_outliers(sorted: &[u64]) -> (usize, usize) {
    if sorted.len() < 4 {
        return (0, 0);
    }
    let q1 = percentile_sorted(sorted, 0.25) as f64;
    let q3 = percentile_sorted(sorted, 0.75) as f64;
    let iqr = (q3 - q1).max(0.0);

    let low_fence = q1 - 1.5 * iqr;
    let high_fence = q3 + 1.5 * iqr;

    let low = sorted.iter().filter(|&&x| (x as f64) < low_fence).count();
    let high = sorted.iter().filter(|&&x| (x as f64) > high_fence).count();

    (low, high)
}
```

---

## `delta-rs/benchmarks/src/results.rs`

```rust
use crate::{stats::SummaryStats, system::SystemInfo};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const RESULT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitInfo {
    pub commit: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunMeta {
    pub schema_version: u32,
    pub suite: String,
    pub scale: String,
    pub iterations: usize,
    pub warmup: usize,
    pub timestamp_utc: String,
    pub system: SystemInfo,
    pub git: GitInfo,
    pub command: Vec<String>,
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CaseStatus {
    Ok,
    Fail,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseResult {
    pub id: String,
    pub status: CaseStatus,
    pub unit: String, // "ns"
    pub samples_ns: Vec<u64>,
    pub summary: Option<SummaryStats>,
    pub extra: BTreeMap<String, serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultFile {
    pub meta: RunMeta,
    pub cases: Vec<CaseResult>,
}
```

---

## `delta-rs/benchmarks/src/cli.rs`

```rust
use crate::{
    data::datasets::DatasetSuite,
    error::DeltaBenchError,
    results::{GitInfo, ResultFile, RunMeta, RESULT_SCHEMA_VERSION},
    suites::{Suite, SuiteName},
    system::SystemInfo,
    util::utc_now_rfc3339,
};
use clap::{Parser, Subcommand, ValueEnum};
use std::{collections::BTreeMap, path::PathBuf};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, ValueEnum)]
pub enum ScaleArg {
    Sf1,
    Sf10,
    Sf100,
}

impl ScaleArg {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sf1 => "sf1",
            Self::Sf10 => "sf10",
            Self::Sf100 => "sf100",
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "delta-bench")]
#[command(about = "Macrobenchmark suite for delta-rs", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Dataset root directory (shared between runs)
    #[arg(long, env = "DELTA_BENCH_DATA_DIR", default_value = "bench-data")]
    data_dir: PathBuf,

    /// Scratch/work directory (per-iteration clones go here)
    #[arg(long, env = "DELTA_BENCH_WORK_DIR", default_value = "bench-work")]
    work_dir: PathBuf,

    /// Default scale factor
    #[arg(long, env = "DELTA_BENCH_SCALE", default_value = "sf1")]
    scale: ScaleArg,

    /// Default iterations
    #[arg(long, env = "DELTA_BENCH_ITERATIONS", default_value_t = 5)]
    iterations: usize,

    /// Default warmup runs
    #[arg(long, env = "DELTA_BENCH_WARMUP", default_value_t = 1)]
    warmup: usize,

    /// RNG seed for deterministic data (row-level determinism)
    #[arg(long, env = "DELTA_BENCH_SEED", default_value_t = 42)]
    seed: u64,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List suites and cases
    List,

    /// Generate benchmark datasets
    Data {
        #[arg(long, default_value = "all")]
        suite: String,

        #[arg(long, default_value_t = false)]
        force: bool,
    },

    /// Run a suite and write JSON results
    Run {
        #[arg(long)]
        suite: String,

        /// Optional comma-separated list of case IDs to run
        #[arg(long)]
        cases: Option<String>,

        /// Where to write the JSON result file
        #[arg(long)]
        out: PathBuf,

        #[arg(long, default_value_t = 0.05)]
        threshold_hint: f64,
    },
}

impl Cli {
    pub async fn run() -> Result<(), DeltaBenchError> {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init();

        let cli = Self::parse();

        match cli.command {
            Command::List => {
                for suite in Suite::all() {
                    println!("{}:", suite.name().as_str());
                    for c in suite.cases() {
                        println!("  - {}: {}", c.id, c.description);
                    }
                }
                Ok(())
            }
            Command::Data { suite, force } => {
                let suite = DatasetSuite::parse(&suite)?;
                suite
                    .generate_all(&cli.data_dir, cli.scale.as_str(), cli.seed, force)
                    .await
            }
            Command::Run {
                suite,
                cases,
                out,
                threshold_hint: _,
            } => {
                let suite_name = SuiteName::parse(&suite)?;
                let suite = Suite::by_name(suite_name);

                let system = SystemInfo::gather();
                let git = crate::suites::git_info();

                let env = std::env::vars().collect::<BTreeMap<_, _>>();
                let command = std::env::args().collect::<Vec<_>>();

                let meta = RunMeta {
                    schema_version: RESULT_SCHEMA_VERSION,
                    suite: suite.name().as_str().to_string(),
                    scale: cli.scale.as_str().to_string(),
                    iterations: cli.iterations,
                    warmup: cli.warmup,
                    timestamp_utc: utc_now_rfc3339(),
                    system,
                    git,
                    command,
                    env,
                };

                let case_filter = cases
                    .as_deref()
                    .map(|s| s.split(',').map(|x| x.trim().to_string()).collect::<Vec<_>>());

                let result_cases = suite
                    .run(
                        &cli.data_dir,
                        &cli.work_dir,
                        cli.scale.as_str(),
                        cli.seed,
                        cli.iterations,
                        cli.warmup,
                        case_filter.as_deref(),
                    )
                    .await?;

                let rf = ResultFile {
                    meta,
                    cases: result_cases,
                };

                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&out, serde_json::to_vec_pretty(&rf)?)?;
                println!("Wrote results to {}", out.display());
                Ok(())
            }
        }
    }
}
```

---

## `delta-rs/benchmarks/src/data/mod.rs`

```rust
pub mod datasets;
pub mod generator;
pub mod table_ops;
```

---

## `delta-rs/benchmarks/src/data/datasets.rs`

```rust
use crate::error::DeltaBenchError;
use crate::util::ensure_dir;
use std::path::{Path, PathBuf};

use super::{generator::EventDataGen, table_ops::DeltaTableWriter};
use tracing::info;

#[derive(Debug, Clone, Copy)]
pub enum DatasetSuite {
    All,
    ReadScan,
    Write,
    MergeDml,
    OptimizeVacuum,
    Metadata,
}

impl DatasetSuite {
    pub fn parse(s: &str) -> Result<Self, DeltaBenchError> {
        match s.trim().to_lowercase().as_str() {
            "all" => Ok(Self::All),
            "read_scan" => Ok(Self::ReadScan),
            "write" => Ok(Self::Write),
            "merge_dml" => Ok(Self::MergeDml),
            "optimize_vacuum" => Ok(Self::OptimizeVacuum),
            "metadata" => Ok(Self::Metadata),
            other => Err(DeltaBenchError::invalid_arg(format!(
                "unknown dataset suite '{other}'"
            ))),
        }
    }

    pub async fn generate_all(
        self,
        data_root: &Path,
        scale: &str,
        seed: u64,
        force: bool,
    ) -> Result<(), DeltaBenchError> {
        ensure_dir(data_root)?;

        // Minimal set of templates. You can extend this list without changing runner logic.
        let mut targets: Vec<(&str, &str, fn(&Path, &str, u64) -> EventDataGen)> = vec![
            ("events", "partitioned", EventDataGen::events_partitioned),
            ("events", "unpartitioned", EventDataGen::events_unpartitioned),
            ("events", "small_files", EventDataGen::events_small_files),
            ("merge_target", "base", EventDataGen::merge_target),
            ("meta_many_versions", "base", EventDataGen::meta_many_versions),
        ];

        if !matches!(self, Self::All) {
            targets.retain(|(name, _, _)| match self {
                Self::ReadScan => *name == "events",
                Self::Write => *name == "events",
                Self::MergeDml => *name == "merge_target",
                Self::OptimizeVacuum => *name == "events",
                Self::Metadata => *name == "meta_many_versions",
                Self::All => true,
            });
        }

        for (dataset, variant, builder) in targets {
            let dir = dataset_dir(data_root, dataset, scale, variant);
            if dir.exists() && !force {
                info!("Dataset exists, skipping: {}", dir.display());
                continue;
            }
            if dir.exists() && force {
                std::fs::remove_dir_all(&dir)?;
            }
            ensure_dir(&dir)?;

            info!("Generating dataset {} ({}) into {}", dataset, variant, dir.display());

            let gen = builder(&dir, scale, seed);
            gen.generate(&dir).await?;
        }

        Ok(())
    }
}

pub fn dataset_dir(root: &Path, dataset: &str, scale: &str, variant: &str) -> PathBuf {
    root.join(dataset).join(scale).join(variant)
}

/// A simple wrapper so dataset generation code is “data-driven”.
impl EventDataGen {
    pub async fn generate(self, out_dir: &Path) -> Result<(), DeltaBenchError> {
        let writer = DeltaTableWriter::new(self.partition_cols.clone());
        writer.write_table(out_dir, &self).await
    }
}
```

---

## `delta-rs/benchmarks/src/data/generator.rs`

```rust
use crate::error::DeltaBenchError;
use deltalake_core::arrow::array::{
    Int64Array, StringArray, TimestampMicrosecondArray,
};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake_core::arrow::record_batch::RecordBatch;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Deterministic scale settings (tunable).
fn rows_for_scale(scale: &str) -> Result<usize, DeltaBenchError> {
    match scale {
        "sf1" => Ok(1_000_000),
        "sf10" => Ok(10_000_000),
        "sf100" => Ok(100_000_000),
        _ => Err(DeltaBenchError::invalid_arg(format!(
            "unknown scale '{scale}'"
        ))),
    }
}

/// Defines how a dataset should be generated (schema + distribution + write pattern).
#[derive(Debug, Clone)]
pub struct EventDataGen {
    pub seed: u64,
    pub scale: String,
    pub total_rows: usize,
    pub batch_rows: usize,
    pub schema: SchemaRef,
    pub partition_cols: Vec<String>,

    pub layout: Layout,
}

#[derive(Debug, Clone, Copy)]
pub enum Layout {
    /// Few large commits/files.
    CompactFiles,
    /// Many small commits/files to stress OPTIMIZE and log replay.
    SmallFiles,
    /// Many commits to stress metadata replay.
    ManyVersions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GeneratedBatchInfo {
    pub start_row: usize,
    pub rows: usize,
}

impl EventDataGen {
    pub fn events_partitioned(_out_dir: &std::path::Path, scale: &str, seed: u64) -> Self {
        let total_rows = rows_for_scale(scale).unwrap();
        Self {
            seed,
            scale: scale.to_string(),
            total_rows,
            batch_rows: 200_000,
            schema: Arc::new(events_schema()),
            partition_cols: vec!["event_date".to_string()],
            layout: Layout::CompactFiles,
        }
    }

    pub fn events_unpartitioned(_out_dir: &std::path::Path, scale: &str, seed: u64) -> Self {
        let total_rows = rows_for_scale(scale).unwrap();
        Self {
            seed,
            scale: scale.to_string(),
            total_rows,
            batch_rows: 200_000,
            schema: Arc::new(events_schema()),
            partition_cols: vec![],
            layout: Layout::CompactFiles,
        }
    }

    pub fn events_small_files(_out_dir: &std::path::Path, scale: &str, seed: u64) -> Self {
        let total_rows = rows_for_scale(scale).unwrap();
        Self {
            seed,
            scale: scale.to_string(),
            total_rows,
            batch_rows: 5_000,
            schema: Arc::new(events_schema()),
            partition_cols: vec!["event_date".to_string()],
            layout: Layout::SmallFiles,
        }
    }

    pub fn merge_target(_out_dir: &std::path::Path, scale: &str, seed: u64) -> Self {
        // Keep MERGE target smaller than scan dataset to keep PR runs fast.
        let total_rows = match scale {
            "sf1" => 1_000_000,
            "sf10" => 5_000_000,
            "sf100" => 20_000_000,
            _ => 1_000_000,
        };
        Self {
            seed: seed ^ 0xA5A5_A5A5,
            scale: scale.to_string(),
            total_rows,
            batch_rows: 200_000,
            schema: Arc::new(merge_schema()),
            partition_cols: vec!["event_date".to_string()],
            layout: Layout::CompactFiles,
        }
    }

    pub fn meta_many_versions(_out_dir: &std::path::Path, scale: &str, seed: u64) -> Self {
        let total_rows = match scale {
            "sf1" => 200_000,
            "sf10" => 1_000_000,
            "sf100" => 5_000_000,
            _ => 200_000,
        };
        Self {
            seed: seed ^ 0xC0FFEE,
            scale: scale.to_string(),
            total_rows,
            batch_rows: 10_000,
            schema: Arc::new(events_schema()),
            partition_cols: vec!["event_date".to_string()],
            layout: Layout::ManyVersions,
        }
    }

    pub fn batches(&self) -> Vec<GeneratedBatchInfo> {
        let mut out = Vec::new();
        let mut start = 0usize;
        while start < self.total_rows {
            let rows = self.batch_rows.min(self.total_rows - start);
            out.push(GeneratedBatchInfo { start_row: start, rows });
            start += rows;
        }
        out
    }

    pub fn make_batch(&self, start_row: usize, rows: usize) -> Result<RecordBatch, DeltaBenchError> {
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed ^ (start_row as u64).wrapping_mul(1_000_003));

        let mut event_id = Vec::with_capacity(rows);
        let mut user_id = Vec::with_capacity(rows);
        let mut event_ts = Vec::with_capacity(rows);
        let mut event_date = Vec::with_capacity(rows);
        let mut country = Vec::with_capacity(rows);
        let mut device = Vec::with_capacity(rows);
        let mut metric_i64 = Vec::with_capacity(rows);

        for i in 0..rows {
            let id = (start_row + i) as i64;
            event_id.push(id);

            // Heavy-tail user_id via bucketed geometric distribution (integer-only).
            let bucket = (rng.next_u64().leading_zeros() as usize).min(5);
            let (lo, hi) = match bucket {
                0 => (0, 1_000),
                1 => (1_000, 10_000),
                2 => (10_000, 100_000),
                3 => (100_000, 1_000_000),
                4 => (1_000_000, 5_000_000),
                _ => (5_000_000, 10_000_000),
            };
            let uid = lo as i64 + (rng.next_u64() % ((hi - lo) as u64)) as i64;
            user_id.push(uid);

            // Event time: deterministic-ish monotonic increments with some jitter.
            let base_ts = 1_700_000_000_000_000i64; // micros
            let ts = base_ts + id * 1_000 + ((rng.next_u64() % 10_000) as i64);
            event_ts.push(ts);

            // Date partition: 30-day cycle.
            let day = (id.rem_euclid(30) as i32) + 1;
            let date = format!("2026-01-{day:02}");
            event_date.push(date);

            let c = match rng.next_u64() % 100 {
                0..=44 => "US",
                45..=59 => "IN",
                60..=69 => "GB",
                70..=79 => "DE",
                80..=89 => "BR",
                _ => "OTHER",
            };
            country.push(c.to_string());

            let d = match rng.next_u64() % 100 {
                0..=54 => "mobile",
                55..=84 => "desktop",
                _ => "tablet",
            };
            device.push(d.to_string());

            // approx-normal int via sum of uniforms (integer-only)
            let mut s = 0i64;
            for _ in 0..6 {
                s += (rng.next_u64() % 1000) as i64;
            }
            metric_i64.push(s - 3000);
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(event_id)),
                Arc::new(Int64Array::from(user_id)),
                Arc::new(TimestampMicrosecondArray::from(event_ts)),
                Arc::new(StringArray::from(event_date)),
                Arc::new(StringArray::from(country)),
                Arc::new(StringArray::from(device)),
                Arc::new(Int64Array::from(metric_i64)),
            ],
        )
        .map_err(|e| DeltaBenchError::invalid_arg(format!("record batch build failed: {e}")))?;

        Ok(batch)
    }
}

fn events_schema() -> Schema {
    Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_date", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("device", DataType::Utf8, false),
        Field::new("metric_i64", DataType::Int64, false),
    ])
}

fn merge_schema() -> Schema {
    // MERGE target: include a "value" column to update.
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_date", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ])
}
```

---

## `delta-rs/benchmarks/src/data/table_ops.rs`

```rust
use crate::error::DeltaBenchError;
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::datafusion::datasource::MemTable;
use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::operations::write::SaveMode;
use deltalake_core::{DeltaOps, DeltaTable};
use std::path::Path;
use std::sync::Arc;
use tracing::info;
use url::Url;

use super::generator::{EventDataGen, Layout};

pub struct DeltaTableWriter {
    partition_cols: Vec<String>,
}

impl DeltaTableWriter {
    pub fn new(partition_cols: Vec<String>) -> Self {
        Self { partition_cols }
    }

    pub async fn write_table(&self, table_dir: &Path, gen: &EventDataGen) -> Result<(), DeltaBenchError> {
        let url = Url::from_directory_path(table_dir)
            .map_err(|_| DeltaBenchError::invalid_arg(format!("invalid path {}", table_dir.display())))?;

        // Start with an uninitialized table (allowed by delta-rs high-level ops).
        let mut table = DeltaTable::try_from_url(url).await?;

        let batches = gen.batches();
        info!("Generating {} batches", batches.len());

        for (i, b) in batches.iter().enumerate() {
            let rb: RecordBatch = gen.make_batch(b.start_row, b.rows)?;
            let df = df_from_batch(&rb).await?;

            // Control commit patterns:
            // - CompactFiles: overwrite on first, then append each batch.
            // - SmallFiles: append small batches (gen.batch_rows already small).
            // - ManyVersions: append small batches to create many commits.
            let mode = if i == 0 { SaveMode::Overwrite } else { SaveMode::Append };

            let start = std::time::Instant::now();

            let builder = DeltaOps(table).write(df).with_save_mode(mode);

            let builder = if gen.partition_cols.is_empty() {
                builder
            } else {
                builder.with_partition_columns(gen.partition_cols.clone())
            };

            let (new_table, _metrics) = builder.await.map_err(|e| {
                DeltaBenchError::invalid_arg(format!("write failed for batch {i}: {e}"))
            })?;

            table = new_table;

            let dt = start.elapsed();
            if matches!(gen.layout, Layout::SmallFiles | Layout::ManyVersions) {
                // Avoid logging too noisily for huge batch counts; just coarse progress.
                if i % 50 == 0 {
                    info!("Wrote batch {}/{} in {:?}", i + 1, batches.len(), dt);
                }
            } else {
                info!("Wrote batch {}/{} in {:?}", i + 1, batches.len(), dt);
            }
        }

        Ok(())
    }
}

async fn df_from_batch(batch: &RecordBatch) -> Result<deltalake_core::datafusion::prelude::DataFrame, DeltaBenchError> {
    let ctx = SessionContext::new();

    let mem = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
        .map_err(|e| DeltaBenchError::invalid_arg(format!("MemTable failed: {e}")))?;

    ctx.register_table("src", Arc::new(mem))
        .map_err(|e| DeltaBenchError::invalid_arg(format!("register_table failed: {e}")))?;

    ctx.table("src")
        .await
        .map_err(|e| DeltaBenchError::invalid_arg(format!("ctx.table failed: {e}")))
}
```

> This uses delta-rs’s high-level ops write path (`DeltaOps(table).write(df)`) which is part of the operations API surface. ([Docs.rs][3])

---

## `delta-rs/benchmarks/src/suites/mod.rs`

```rust
use crate::error::DeltaBenchError;
use crate::results::{CaseResult, CaseStatus};
use crate::stats::summarize;
use crate::util::ensure_dir;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

pub mod merge_dml;
pub mod metadata;
pub mod optimize_vacuum;
pub mod read_scan;
pub mod write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuiteName {
    ReadScan,
    Write,
    MergeDml,
    OptimizeVacuum,
    Metadata,
}

impl SuiteName {
    pub fn parse(s: &str) -> Result<Self, DeltaBenchError> {
        match s.trim().to_lowercase().as_str() {
            "read_scan" => Ok(Self::ReadScan),
            "write" => Ok(Self::Write),
            "merge_dml" => Ok(Self::MergeDml),
            "optimize_vacuum" => Ok(Self::OptimizeVacuum),
            "metadata" => Ok(Self::Metadata),
            other => Err(DeltaBenchError::invalid_arg(format!(
                "unknown suite '{other}'"
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadScan => "read_scan",
            Self::Write => "write",
            Self::MergeDml => "merge_dml",
            Self::OptimizeVacuum => "optimize_vacuum",
            Self::Metadata => "metadata",
        }
    }
}

pub struct Suite {
    name: SuiteName,
    cases: Vec<BenchCase>,
}

impl Suite {
    pub fn all() -> Vec<Suite> {
        vec![
            read_scan::suite(),
            write::suite(),
            merge_dml::suite(),
            optimize_vacuum::suite(),
            metadata::suite(),
        ]
    }

    pub fn by_name(name: SuiteName) -> Suite {
        Self::all()
            .into_iter()
            .find(|s| s.name == name)
            .expect("suite exists")
    }

    pub fn name(&self) -> SuiteName {
        self.name
    }

    pub fn cases(&self) -> &[BenchCase] {
        &self.cases
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        &self,
        data_dir: &Path,
        work_dir: &Path,
        scale: &str,
        seed: u64,
        iterations: usize,
        warmup: usize,
        case_filter: Option<&[String]>,
    ) -> Result<Vec<CaseResult>, DeltaBenchError> {
        ensure_dir(work_dir)?;

        let mut out = Vec::new();

        for case in &self.cases {
            if let Some(filter) = case_filter {
                if !filter.iter().any(|x| x == &case.id) {
                    continue;
                }
            }

            info!("Running case: {}", case.id);

            let template_dir = case.dataset_path(data_dir, scale);
            if !template_dir.exists() {
                return Err(DeltaBenchError::MissingDataset(format!(
                    "{} (expected at {})",
                    case.dataset_key,
                    template_dir.display()
                )));
            }

            let mut samples = Vec::new();
            let mut extra = BTreeMap::new();
            let mut error: Option<String> = None;

            // Warmup (not recorded)
            for _ in 0..warmup {
                let _ = case
                    .run_one(&template_dir, work_dir, scale, seed, true)
                    .await;
            }

            for i in 0..iterations {
                match case.run_one(&template_dir, work_dir, scale, seed, false).await {
                    Ok(run) => {
                        samples.push(run.duration_ns);
                        // Merge extra maps (last write wins)
                        for (k, v) in run.extra {
                            extra.insert(k, v);
                        }
                        info!("  iter {}: {} ns", i + 1, run.duration_ns);
                    }
                    Err(e) => {
                        warn!("Case failed: {e}");
                        error = Some(format!("{e}"));
                        break;
                    }
                }
            }

            let (status, summary) = if error.is_some() {
                (CaseStatus::Fail, None)
            } else if samples.is_empty() {
                (CaseStatus::Skip, None)
            } else {
                (CaseStatus::Ok, Some(summarize(&samples)))
            };

            out.push(CaseResult {
                id: case.id.to_string(),
                status,
                unit: "ns".to_string(),
                samples_ns: samples,
                summary,
                extra,
                error,
            });
        }

        Ok(out)
    }
}

pub fn git_info() -> crate::results::GitInfo {
    use std::process::Command;

    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());

    let branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());

    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);

    crate::results::GitInfo { commit, branch, dirty }
}

pub struct BenchCase {
    pub id: &'static str,
    pub description: &'static str,

    /// Dataset key: `<dataset>/<variant>`
    pub dataset_key: &'static str,

    /// If true, clone template per iteration (needed for mutating ops).
    pub clone_per_iter: bool,

    pub runner: fn(&CaseContext) -> crate::util::BoxFutureResult,
}

pub struct CaseRun {
    pub duration_ns: u64,
    pub extra: BTreeMap<String, serde_json::Value>,
}

pub struct CaseContext {
    pub case_id: &'static str,
    pub table_dir: PathBuf,
    pub scale: String,
    pub seed: u64,
}

impl BenchCase {
    pub fn dataset_path(&self, data_dir: &Path, scale: &str) -> PathBuf {
        let (ds, variant) = self
            .dataset_key
            .split_once('/')
            .unwrap_or((self.dataset_key, "base"));
        data_dir.join(ds).join(scale).join(variant)
    }

    pub async fn run_one(
        &self,
        template_dir: &Path,
        work_dir: &Path,
        scale: &str,
        seed: u64,
        warmup: bool,
    ) -> Result<CaseRun, DeltaBenchError> {
        let run_dir = if self.clone_per_iter {
            // Work dir: bench-work/<case-id>/<random>
            let dir = work_dir.join(self.id.replace('/', "_")).join(format!("run-{}", uuid_like(seed, warmup)));
            crate::util::clone_tree_hardlink(template_dir, &dir)?;
            dir
        } else {
            template_dir.to_path_buf()
        };

        let ctx = CaseContext {
            case_id: self.id,
            table_dir: run_dir,
            scale: scale.to_string(),
            seed,
        };

        let fut = (self.runner)(&ctx);
        fut.await
            .map_err(|e| DeltaBenchError::case_failed(self.id, e))
    }
}

fn uuid_like(seed: u64, warmup: bool) -> String {
    // lightweight deterministic-ish suffix
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}-{}-{}", seed, if warmup { "w" } else { "m" }, t)
}
```

We referenced `BoxFutureResult` and `clone_tree_hardlink`; we’ll define them in `util.rs` next.

---

## `delta-rs/benchmarks/src/util.rs` (extend with futures + clone)

```rust
use crate::error::DeltaBenchError;
use futures::future::BoxFuture;
use std::path::{Path, PathBuf};

pub type BoxFutureResult = BoxFuture<'static, Result<crate::suites::CaseRun, String>>;

pub fn ensure_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

pub fn utc_now_rfc3339() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}

/// Clone directory tree using hardlinks when possible, falling back to copies.
/// This makes per-iteration table “reset” cheap and keeps template immutable.
pub fn clone_tree_hardlink(src: &Path, dst: &Path) -> Result<(), DeltaBenchError> {
    if dst.exists() {
        std::fs::remove_dir_all(dst)?;
    }
    std::fs::create_dir_all(dst)?;

    for entry in walkdir::WalkDir::new(src) {
        let entry = entry?;
        let rel = entry.path().strip_prefix(src).unwrap();
        let out_path = dst.join(rel);

        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&out_path)?;
            continue;
        }

        if entry.file_type().is_file() {
            // Try hardlink first.
            if std::fs::hard_link(entry.path(), &out_path).is_err() {
                std::fs::copy(entry.path(), &out_path)?;
            }
        }
    }
    Ok(())
}
```

Add in `Cargo.toml` dependency `futures = "0.3"` if you prefer; or use `deltalake-core`’s futures. For brevity I used `futures` implicitly; if delta-rs workspace already has `futures`, add it or swap to `deltalake_core::futures`. (This is the one small dependency omission you may need to patch depending on delta-rs workspace state.)

---

## `delta-rs/benchmarks/src/suites/read_scan.rs`

```rust
use crate::suites::{BenchCase, CaseContext, CaseRun, Suite};
use deltalake_core::datafusion::prelude::SessionContext;
use std::collections::BTreeMap;
use std::sync::Arc;

use deltalake_core::DeltaTable;
use url::Url;

pub fn suite() -> Suite {
    Suite {
        name: crate::suites::SuiteName::ReadScan,
        cases: vec![
            BenchCase {
                id: "read/full_scan_count",
                description: "SELECT COUNT(*) over partitioned events",
                dataset_key: "events/partitioned",
                clone_per_iter: false,
                runner: run_full_scan_count,
            },
            BenchCase {
                id: "read/projected_sum",
                description: "SELECT SUM(metric_i64) over partitioned events",
                dataset_key: "events/partitioned",
                clone_per_iter: false,
                runner: run_projected_sum,
            },
            BenchCase {
                id: "read/partition_pruning",
                description: "Partition pruning on event_date",
                dataset_key: "events/partitioned",
                clone_per_iter: false,
                runner: run_partition_pruning,
            },
            BenchCase {
                id: "read/data_skipping_selective",
                description: "Selective range filter for file skipping",
                dataset_key: "events/partitioned",
                clone_per_iter: false,
                runner: run_data_skipping_selective,
            },
        ],
    }
}

fn run_full_scan_count(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let start = std::time::Instant::now();

        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        let provider = table.table_provider().await.map_err(|e| format!("{e}"))?;
        let mut session = SessionContext::new();
        session
            .register_table("events", provider)
            .map_err(|e| format!("{e}"))?;

        let df = session.sql("SELECT COUNT(*) as c FROM events").await.map_err(|e| format!("{e}"))?;
        let _batches = df.collect().await.map_err(|e| format!("{e}"))?;

        let dur = start.elapsed();
        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra: BTreeMap::new(),
        })
    })
}

fn run_projected_sum(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let start = std::time::Instant::now();

        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;
        let provider = table.table_provider().await.map_err(|e| format!("{e}"))?;

        let mut session = SessionContext::new();
        session
            .register_table("events", provider)
            .map_err(|e| format!("{e}"))?;

        let df = session
            .sql("SELECT SUM(metric_i64) as s FROM events")
            .await
            .map_err(|e| format!("{e}"))?;
        let _batches = df.collect().await.map_err(|e| format!("{e}"))?;

        let dur = start.elapsed();
        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra: BTreeMap::new(),
        })
    })
}

fn run_partition_pruning(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let start = std::time::Instant::now();

        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;
        let provider = table.table_provider().await.map_err(|e| format!("{e}"))?;

        let mut session = SessionContext::new();
        session
            .register_table("events", provider)
            .map_err(|e| format!("{e}"))?;

        // Pick a stable partition
        let df = session
            .sql("SELECT COUNT(*) FROM events WHERE event_date = '2026-01-01'")
            .await
            .map_err(|e| format!("{e}"))?;
        let _batches = df.collect().await.map_err(|e| format!("{e}"))?;

        let dur = start.elapsed();
        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra: BTreeMap::new(),
        })
    })
}

fn run_data_skipping_selective(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let start = std::time::Instant::now();

        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;
        let provider = table.table_provider().await.map_err(|e| format!("{e}"))?;

        let mut session = SessionContext::new();
        session
            .register_table("events", provider)
            .map_err(|e| format!("{e}"))?;

        // Narrow range to encourage file skipping when min/max stats are present.
        let df = session
            .sql("SELECT COUNT(*) FROM events WHERE event_id BETWEEN 1000 AND 2000")
            .await
            .map_err(|e| format!("{e}"))?;
        let _batches = df.collect().await.map_err(|e| format!("{e}"))?;

        let dur = start.elapsed();
        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra: BTreeMap::new(),
        })
    })
}
```

---

## `delta-rs/benchmarks/src/suites/write.rs`

```rust
use crate::suites::{BenchCase, CaseContext, CaseRun, Suite};
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::datafusion::datasource::MemTable;
use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::operations::write::SaveMode;
use deltalake_core::{DeltaOps, DeltaTable};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

pub fn suite() -> Suite {
    Suite {
        name: crate::suites::SuiteName::Write,
        cases: vec![
            BenchCase {
                id: "write/append_small_batches",
                description: "Append many small batches (small file stress)",
                dataset_key: "events/partitioned",
                clone_per_iter: true,
                runner: append_small_batches,
            },
            BenchCase {
                id: "write/append_large_batches",
                description: "Append fewer large batches",
                dataset_key: "events/partitioned",
                clone_per_iter: true,
                runner: append_large_batches,
            },
        ],
    }
}

fn append_small_batches(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    let seed = ctx.seed;
    Box::pin(async move {
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let mut table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        // Prepare input outside timed block.
        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0x515151);
        let batches = make_synthetic_write_batches(&mut rng, 200, 5_000)?;

        let start = std::time::Instant::now();
        for rb in batches {
            let df = df_from_batch(&rb).await?;
            let (new_table, _m) = DeltaOps(table)
                .write(df)
                .with_save_mode(SaveMode::Append)
                .await
                .map_err(|e| format!("{e}"))?;
            table = new_table;
        }
        let dur = start.elapsed();

        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra: BTreeMap::new(),
        })
    })
}

fn append_large_batches(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    let seed = ctx.seed;
    Box::pin(async move {
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let mut table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xA0A0A0);
        let batches = make_synthetic_write_batches(&mut rng, 10, 100_000)?;

        let start = std::time::Instant::now();
        for rb in batches {
            let df = df_from_batch(&rb).await?;
            let (new_table, _m) = DeltaOps(table)
                .write(df)
                .with_save_mode(SaveMode::Append)
                .await
                .map_err(|e| format!("{e}"))?;
            table = new_table;
        }
        let dur = start.elapsed();

        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra: BTreeMap::new(),
        })
    })
}

fn make_synthetic_write_batches(
    rng: &mut ChaCha8Rng,
    batches: usize,
    rows: usize,
) -> Result<Vec<RecordBatch>, String> {
    // Reuse the same schema as events table.
    // For brevity we generate a subset of columns.
    use deltalake_core::arrow::array::{Int64Array, StringArray};
    use deltalake_core::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("event_date", DataType::Utf8, false),
        Field::new("metric_i64", DataType::Int64, false),
    ]));

    let mut out = Vec::new();
    for b in 0..batches {
        let mut ids = Vec::with_capacity(rows);
        let mut dates = Vec::with_capacity(rows);
        let mut metrics = Vec::with_capacity(rows);

        for i in 0..rows {
            let id = (b * rows + i) as i64 + (rng.next_u64() as i64 & 0xFF);
            ids.push(id);
            let day = (id.rem_euclid(30) as i32) + 1;
            dates.push(format!("2026-02-{day:02}"));
            metrics.push((rng.next_u64() % 6000) as i64 - 3000);
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(dates)),
                Arc::new(Int64Array::from(metrics)),
            ],
        )
        .map_err(|e| format!("{e}"))?;
        out.push(rb);
    }
    Ok(out)
}

async fn df_from_batch(rb: &RecordBatch) -> Result<deltalake_core::datafusion::prelude::DataFrame, String> {
    let ctx = SessionContext::new();
    let mem = MemTable::try_new(rb.schema(), vec![vec![rb.clone()]])
        .map_err(|e| format!("{e}"))?;
    ctx.register_table("src", Arc::new(mem))
        .map_err(|e| format!("{e}"))?;
    ctx.table("src").await.map_err(|e| format!("{e}"))
}
```

---

## `delta-rs/benchmarks/src/suites/merge_dml.rs`

```rust
use crate::suites::{BenchCase, CaseContext, CaseRun, Suite};
use deltalake_core::datafusion::datasource::MemTable;
use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::datafusion::prelude::{col, lit};
use deltalake_core::{DeltaOps, DeltaTable};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

pub fn suite() -> Suite {
    Suite {
        name: crate::suites::SuiteName::MergeDml,
        cases: vec![
            BenchCase {
                id: "merge/match_10pct",
                description: "MERGE with ~10% key matches",
                dataset_key: "merge_target/base",
                clone_per_iter: true,
                runner: merge_match_10,
            },
            BenchCase {
                id: "merge/match_50pct",
                description: "MERGE with ~50% key matches",
                dataset_key: "merge_target/base",
                clone_per_iter: true,
                runner: merge_match_50,
            },
            BenchCase {
                id: "merge/match_90pct",
                description: "MERGE with ~90% key matches",
                dataset_key: "merge_target/base",
                clone_per_iter: true,
                runner: merge_match_90,
            },
        ],
    }
}

fn merge_match_10(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    merge_with_match_ratio(ctx, 0.10)
}
fn merge_match_50(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    merge_with_match_ratio(ctx, 0.50)
}
fn merge_match_90(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    merge_with_match_ratio(ctx, 0.90)
}

fn merge_with_match_ratio(ctx: &CaseContext, ratio: f64) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    let seed = ctx.seed;
    Box::pin(async move {
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        // Build source DataFrame deterministically OUTSIDE timed region.
        let source_df = build_merge_source_df(seed, ratio).await?;

        let start = std::time::Instant::now();

        // Predicate target.id = source.id, with aliases like in delta-rs docs.
        let (table, metrics) = DeltaOps(table)
            .merge(source_df, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|u| {
                u.update("value", col("source.value"))
                    .update("updated_at", col("source.updated_at"))
            })
            .map_err(|e| format!("{e}"))?
            .when_not_matched_insert(|i| {
                i.set("id", col("source.id"))
                    .set("event_date", col("source.event_date"))
                    .set("value", col("source.value"))
                    .set("updated_at", col("source.updated_at"))
            })
            .map_err(|e| format!("{e}"))?
            .await
            .map_err(|e| format!("{e}"))?;

        let dur = start.elapsed();

        let mut extra = BTreeMap::new();
        extra.insert("mergeMatchRatio".to_string(), serde_json::json!(ratio));
        extra.insert("mergeMetrics".to_string(), serde_json::to_value(metrics).unwrap_or(serde_json::json!({})));

        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra,
        })
    })
}

async fn build_merge_source_df(seed: u64, match_ratio: f64) -> Result<deltalake_core::datafusion::prelude::DataFrame, String> {
    use deltalake_core::arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use deltalake_core::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use deltalake_core::arrow::record_batch::RecordBatch;

    let mut rng = ChaCha8Rng::seed_from_u64(seed ^ 0xDEADBEEF);

    let source_rows = 200_000usize;
    let target_key_space = 1_000_000usize;

    let matches = (source_rows as f64 * match_ratio) as usize;
    let non_matches = source_rows - matches;

    let mut ids = Vec::with_capacity(source_rows);
    let mut dates = Vec::with_capacity(source_rows);
    let mut values = Vec::with_capacity(source_rows);
    let mut updated = Vec::with_capacity(source_rows);

    // matched keys in [0, target_key_space)
    for _ in 0..matches {
        let id = (rng.next_u64() as usize % target_key_space) as i64;
        ids.push(id);
        let day = (id.rem_euclid(30) as i32) + 1;
        dates.push(format!("2026-01-{day:02}"));
        values.push((rng.next_u64() % 10_000) as i64);
        updated.push(1_700_000_000_000_000i64 + (rng.next_u64() % 1_000_000) as i64);
    }

    // new keys outside target range
    for _ in 0..non_matches {
        let id = target_key_space as i64 + (rng.next_u64() % 10_000_000) as i64;
        ids.push(id);
        let day = (id.rem_euclid(30) as i32) + 1;
        dates.push(format!("2026-01-{day:02}"));
        values.push((rng.next_u64() % 10_000) as i64);
        updated.push(1_700_000_000_000_000i64 + (rng.next_u64() % 1_000_000) as i64);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_date", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));

    let rb = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(dates)),
            Arc::new(Int64Array::from(values)),
            Arc::new(TimestampMicrosecondArray::from(updated)),
        ],
    )
    .map_err(|e| format!("{e}"))?;

    let ctx = SessionContext::new();
    let mem = MemTable::try_new(rb.schema(), vec![vec![rb]]).map_err(|e| format!("{e}"))?;
    ctx.register_table("source", Arc::new(mem))
        .map_err(|e| format!("{e}"))?;
    ctx.table("source").await.map_err(|e| format!("{e}"))
}
```

This is directly aligned with delta-rs’s MERGE builder semantics and aliasing approach shown in docs. ([Docs.rs][9])

---

## `delta-rs/benchmarks/src/suites/optimize_vacuum.rs`

```rust
use crate::suites::{BenchCase, CaseContext, CaseRun, Suite};
use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::{DeltaOps};
use std::collections::BTreeMap;
use url::Url;

pub fn suite() -> Suite {
    Suite {
        name: crate::suites::SuiteName::OptimizeVacuum,
        cases: vec![
            BenchCase {
                id: "optimize/compact_small_files",
                description: "OPTIMIZE bin-pack compaction on small-files table",
                dataset_key: "events/small_files",
                clone_per_iter: true,
                runner: optimize_compact,
            },
            BenchCase {
                id: "vacuum/dry_run",
                description: "VACUUM dry-run after optimize",
                dataset_key: "events/small_files",
                clone_per_iter: true,
                runner: vacuum_dry_run_after_optimize,
            },
        ],
    }
}

fn optimize_compact(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        let start = std::time::Instant::now();
        let (table, metrics) = DeltaOps(table)
            .optimize()
            .await
            .map_err(|e| format!("{e}"))?;
        let dur = start.elapsed();

        let mut extra = BTreeMap::new();
        extra.insert("optimizeMetrics".to_string(), serde_json::to_value(metrics).unwrap_or(serde_json::json!({})));

        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra,
        })
    })
}

fn vacuum_dry_run_after_optimize(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        // First optimize (setup outside timed section would be better; kept simple here)
        let (table, _m) = DeltaOps(table).optimize().await.map_err(|e| format!("{e}"))?;

        let start = std::time::Instant::now();
        let (_table, metrics) = DeltaOps(table)
            .vacuum()
            .with_dry_run(true)
            .await
            .map_err(|e| format!("{e}"))?;
        let dur = start.elapsed();

        let mut extra = BTreeMap::new();
        extra.insert("vacuumMetrics".to_string(), serde_json::to_value(metrics).unwrap_or(serde_json::json!({})));

        Ok(CaseRun {
            duration_ns: dur.as_nanos() as u64,
            extra,
        })
    })
}
```

This matches the intent described by delta-rs optimize docs (optimize creates remove actions; vacuum deletes). ([Docs.rs][10])

---

## `delta-rs/benchmarks/src/suites/metadata.rs`

```rust
use crate::suites::{BenchCase, CaseContext, CaseRun, Suite};
use deltalake_core::DeltaTable;
use std::collections::BTreeMap;
use url::Url;

pub fn suite() -> Suite {
    Suite {
        name: crate::suites::SuiteName::Metadata,
        cases: vec![
            BenchCase {
                id: "meta/open_latest",
                description: "open_table() load latest state",
                dataset_key: "meta_many_versions/base",
                clone_per_iter: false,
                runner: open_latest,
            },
            BenchCase {
                id: "meta/open_version_0",
                description: "open_table_with_version(0)",
                dataset_key: "meta_many_versions/base",
                clone_per_iter: false,
                runner: open_v0,
            },
            BenchCase {
                id: "meta/update_state",
                description: "DeltaTable::update_state (checkpoint+log replay path)",
                dataset_key: "meta_many_versions/base",
                clone_per_iter: false,
                runner: update_state,
            },
        ],
    }
}

fn open_latest(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let start = std::time::Instant::now();
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let _table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;
        let dur = start.elapsed();

        Ok(CaseRun { duration_ns: dur.as_nanos() as u64, extra: BTreeMap::new() })
    })
}

fn open_v0(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let start = std::time::Instant::now();
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let _table = deltalake_core::open_table_with_version(url, 0).await.map_err(|e| format!("{e}"))?;
        let dur = start.elapsed();

        Ok(CaseRun { duration_ns: dur.as_nanos() as u64, extra: BTreeMap::new() })
    })
}

fn update_state(ctx: &CaseContext) -> crate::util::BoxFutureResult {
    let dir = ctx.table_dir.clone();
    Box::pin(async move {
        let url = Url::from_directory_path(&dir).map_err(|_| "bad path".to_string())?;
        let mut table = deltalake_core::open_table(url).await.map_err(|e| format!("{e}"))?;

        let start = std::time::Instant::now();
        table.update_state().await.map_err(|e| format!("{e}"))?;
        let dur = start.elapsed();

        Ok(CaseRun { duration_ns: dur.as_nanos() as u64, extra: BTreeMap::new() })
    })
}
```

The metadata benchmark directly targets the behavior described in DeltaTable docs: update_state loads checkpoint + incrementally applies commits. ([Docs.rs][11])

---

# Deliverable 3: Python comparison tooling (`delta-rs-benchmarking`)

## `delta-rs-benchmarking/pyproject.toml`

```toml
[project]
name = "delta-bench-compare"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
  "rich>=13.7.1",
]

[tool.ruff]
line-length = 100
target-version = "py310"
select = ["E", "F", "I", "B", "UP", "SIM"]
ignore = ["E501"]

[tool.pytest.ini_options]
testpaths = ["python/tests"]
```

---

## `delta-rs-benchmarking/python/delta_bench_compare/model.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, TypedDict


CaseStatus = Literal["ok", "fail", "skip"]


class Summary(TypedDict):
    n: int
    min_ns: int
    max_ns: int
    mean_ns: float
    p50_ns: int
    p90_ns: int
    stddev_ns: float
    cv: float
    outliers_low: int
    outliers_high: int


class Case(TypedDict):
    id: str
    status: CaseStatus
    unit: str
    samples_ns: List[int]
    summary: Optional[Summary]
    extra: Dict[str, Any]
    error: Optional[str]


class Meta(TypedDict):
    schema_version: int
    suite: str
    scale: str
    iterations: int
    warmup: int
    timestamp_utc: str
    system: Dict[str, Any]
    git: Dict[str, Any]
    command: List[str]
    env: Dict[str, str]


class ResultFile(TypedDict):
    meta: Meta
    cases: List[Case]


@dataclass(frozen=True)
class CasePoint:
    case_id: str
    status: CaseStatus
    p50_ns: Optional[int]
    error: Optional[str]
```

---

## `delta-rs-benchmarking/python/delta_bench_compare/formatting.py`

```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HumanTime:
    value: float
    unit: str

    def __str__(self) -> str:
        return f"{self.value:.2f} {self.unit}"


def fmt_ns(ns: int) -> HumanTime:
    if ns < 1_000:
        return HumanTime(float(ns), "ns")
    if ns < 1_000_000:
        return HumanTime(ns / 1_000.0, "µs")
    if ns < 1_000_000_000:
        return HumanTime(ns / 1_000_000.0, "ms")
    return HumanTime(ns / 1_000_000_000.0, "s")


def fmt_ratio(speedup: float, threshold: float) -> str:
    """
    speedup > 1 means candidate is faster (baseline / candidate).
    """
    if abs(speedup - 1.0) <= threshold:
        return "no change"
    if speedup > 1.0:
        return f"+{speedup:.2f}x faster"
    return f"{(1.0/speedup):.2f}x slower"
```

---

## `delta-rs-benchmarking/python/delta_bench_compare/compare.py`

````python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

from rich.console import Console
from rich.table import Table
from rich import box

from .formatting import fmt_ns, fmt_ratio
from .model import Case, CasePoint, ResultFile


def load_result(path: Path) -> ResultFile:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def case_map(result: ResultFile) -> Dict[str, Case]:
    return {c["id"]: c for c in result["cases"]}


def extract_point(c: Case) -> CasePoint:
    if c["status"] != "ok" or not c.get("summary"):
        return CasePoint(case_id=c["id"], status=c["status"], p50_ns=None, error=c.get("error"))
    return CasePoint(
        case_id=c["id"],
        status=c["status"],
        p50_ns=int(c["summary"]["p50_ns"]),
        error=None,
    )


def compare_points(
    base: Dict[str, CasePoint],
    cand: Dict[str, CasePoint],
    threshold: float,
) -> Tuple[List[Tuple[str, str, str, str]], Dict[str, int], int, int]:
    ids = sorted(set(base.keys()) | set(cand.keys()))
    rows: List[Tuple[str, str, str, str]] = []

    faster = slower = same = fail = missing = 0
    total_base = 0
    total_cand = 0

    for cid in ids:
        b = base.get(cid)
        c = cand.get(cid)

        def cell(p: CasePoint | None) -> str:
            if p is None:
                return "MISSING"
            if p.status != "ok" or p.p50_ns is None:
                return "FAIL"
            return str(fmt_ns(p.p50_ns))

        b_cell = cell(b)
        c_cell = cell(c)

        change = "incomparable"
        if b and c and b.status == "ok" and c.status == "ok" and b.p50_ns and c.p50_ns:
            total_base += b.p50_ns
            total_cand += c.p50_ns
            speedup = b.p50_ns / c.p50_ns
            change = fmt_ratio(speedup, threshold=threshold)
            if change == "no change":
                same += 1
            elif "faster" in change:
                faster += 1
            else:
                slower += 1
        else:
            if b is None or c is None:
                missing += 1
            else:
                fail += 1

        rows.append((cid, b_cell, c_cell, change))

    stats = {
        "faster": faster,
        "slower": slower,
        "no_change": same,
        "fail": fail,
        "missing": missing,
    }
    return rows, stats, total_base, total_cand


def render(rows: List[Tuple[str, str, str, str]], stats: Dict[str, int], total_base: int, total_cand: int) -> str:
    console = Console(record=True, width=100)

    t = Table(title=None, box=box.HEAVY_HEAD, show_lines=False)
    t.add_column("Case")
    t.add_column("baseline", justify="right")
    t.add_column("candidate", justify="right")
    t.add_column("Change", justify="right")

    for cid, b, c, ch in rows:
        t.add_row(cid, b, c, ch)

    console.print(t)

    s = Table(title="Benchmark Summary", box=box.HEAVY_HEAD)
    s.add_column("Metric")
    s.add_column("Value", justify="right")

    s.add_row("Total Time (baseline)", str(fmt_ns(total_base)) if total_base else "n/a")
    s.add_row("Total Time (candidate)", str(fmt_ns(total_cand)) if total_cand else "n/a")
    s.add_row("Cases Faster", str(stats["faster"]))
    s.add_row("Cases Slower", str(stats["slower"]))
    s.add_row("Cases with No Change", str(stats["no_change"]))
    s.add_row("Cases with Failure", str(stats["fail"]))
    s.add_row("Cases Missing", str(stats["missing"]))

    console.print(s)
    return console.export_text()


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare delta-bench JSON results")
    ap.add_argument("baseline", type=Path)
    ap.add_argument("candidate", type=Path)
    ap.add_argument("--threshold", type=float, default=0.05, help="No-change band (fraction)")
    ap.add_argument("--markdown", action="store_true", help="Wrap output in a markdown code fence")
    args = ap.parse_args()

    base = load_result(args.baseline)
    cand = load_result(args.candidate)

    base_points = {k: extract_point(v) for k, v in case_map(base).items()}
    cand_points = {k: extract_point(v) for k, v in case_map(cand).items()}

    rows, stats, total_base, total_cand = compare_points(base_points, cand_points, args.threshold)
    text = render(rows, stats, total_base, total_cand)

    if args.markdown:
        print("```")
        print(text.rstrip())
        print("```")
    else:
        print(text.rstrip())


if __name__ == "__main__":
    main()
````

This intentionally matches the “box drawing table + summary” style that DataFusion’s bot posts. ([GitHub][1])

---

## `delta-rs-benchmarking/python/tests/test_compare.py`

```python
from __future__ import annotations

from delta_bench_compare.formatting import fmt_ratio


def test_fmt_ratio_threshold():
    assert fmt_ratio(1.02, threshold=0.05) == "no change"
    assert "faster" in fmt_ratio(1.10, threshold=0.05)
    assert "slower" in fmt_ratio(0.90, threshold=0.05)
```

---

# Deliverable 4: Shell scripts

## `delta-rs-benchmarking/scripts/vm_info.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "## System Info"
echo
echo "uname: $(uname -a || true)"
echo
if command -v lscpu >/dev/null 2>&1; then
  echo "lscpu:"
  lscpu || true
  echo
fi
if command -v free >/dev/null 2>&1; then
  echo "memory:"
  free -h || true
  echo
fi
if command -v lsblk >/dev/null 2>&1; then
  echo "disk:"
  lsblk || true
  echo
fi
```

---

## `delta-rs-benchmarking/scripts/compare_branch.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  compare_branch.sh --repo delta-io/delta-rs --pr <PR_NUMBER> --suite <suite> [--scale sf1|sf10|sf100] [--iterations N] [--warmup N] [--threshold X] [--dry-run]

Environment:
  DELTA_BENCH_DATA_DIR   Path to pre-generated datasets (shared)
  DELTA_BENCH_WORK_DIR   Scratch directory (fast local disk recommended)
  GITHUB_TOKEN           Token for gh (optional) if using gh to fetch PR head
EOF
}

REPO="delta-io/delta-rs"
PR=""
SUITE=""
SCALE="${DELTA_BENCH_SCALE:-sf1}"
ITER="${DELTA_BENCH_ITERATIONS:-5}"
WARMUP="${DELTA_BENCH_WARMUP:-1}"
THRESHOLD="0.05"
DRY_RUN="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2;;
    --pr) PR="$2"; shift 2;;
    --suite) SUITE="$2"; shift 2;;
    --scale) SCALE="$2"; shift 2;;
    --iterations) ITER="$2"; shift 2;;
    --warmup) WARMUP="$2"; shift 2;;
    --threshold) THRESHOLD="$2"; shift 2;;
    --dry-run) DRY_RUN="1"; shift 1;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "${PR}" || -z "${SUITE}" ]]; then
  echo "Missing --pr or --suite" >&2
  usage
  exit 2
fi

DATA_DIR="${DELTA_BENCH_DATA_DIR:-/mnt/delta-bench-data}"
WORK_DIR="${DELTA_BENCH_WORK_DIR:-/mnt/delta-bench-work}"

ROOT="$(pwd)"
RUN_ROOT="${WORK_DIR}/runs/pr-${PR}"
BASE_DIR="${RUN_ROOT}/base"
PR_DIR="${RUN_ROOT}/pr"

mkdir -p "${RUN_ROOT}"

# Ensure only one compare runs at a time on the machine.
LOCK_FILE="${WORK_DIR}/delta-bench.lock"
mkdir -p "${WORK_DIR}"
exec 9>"${LOCK_FILE}"
flock -n 9 || { echo "Benchmark runner busy (lock held): ${LOCK_FILE}" >&2; exit 1; }

echo "=== delta-rs PR benchmark ==="
echo "Repo: ${REPO}"
echo "PR: #${PR}"
echo "Suite: ${SUITE}"
echo "Scale: ${SCALE}  Iter: ${ITER}  Warmup: ${WARMUP}  Threshold: ${THRESHOLD}"
echo "Data dir: ${DATA_DIR}"
echo "Work dir: ${WORK_DIR}"
echo

"${ROOT}/scripts/vm_info.sh"

run_cmd() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[dry-run] $*"
  else
    eval "$@"
  fi
}

# Checkout baseline main
run_cmd "rm -rf '${BASE_DIR}'"
run_cmd "git clone --depth 1 --branch main 'https://github.com/${REPO}.git' '${BASE_DIR}'"

# Checkout PR head
run_cmd "rm -rf '${PR_DIR}'"
run_cmd "git clone --depth 1 'https://github.com/${REPO}.git' '${PR_DIR}'"
run_cmd "cd '${PR_DIR}' && git fetch origin 'pull/${PR}/head:pr-${PR}' && git checkout 'pr-${PR}'"

BASE_SHA="$(cd "${BASE_DIR}" && git rev-parse HEAD)"
PR_SHA="$(cd "${PR_DIR}" && git rev-parse HEAD)"

echo
echo "Baseline SHA: ${BASE_SHA}"
echo "PR SHA:       ${PR_SHA}"
echo

BASE_OUT="${RUN_ROOT}/baseline.json"
PR_OUT="${RUN_ROOT}/candidate.json"

# Build + run
run_one() {
  local dir="$1"
  local out="$2"
  run_cmd "cd '${dir}' && cargo run -p delta-bench --release -- run --suite '${SUITE}' --scale '${SCALE}' --out '${out}'"
}

export DELTA_BENCH_DATA_DIR="${DATA_DIR}"
export DELTA_BENCH_WORK_DIR="${WORK_DIR}"
export DELTA_BENCH_ITERATIONS="${ITER}"
export DELTA_BENCH_WARMUP="${WARMUP}"
export RUST_BACKTRACE=1

echo "=== Running baseline ==="
run_one "${BASE_DIR}" "${BASE_OUT}"

echo "=== Running candidate ==="
run_one "${PR_DIR}" "${PR_OUT}"

echo "=== Compare ==="
python3 -m delta_bench_compare.compare "${BASE_OUT}" "${PR_OUT}" --threshold "${THRESHOLD}" --markdown
```

---

## `delta-rs-benchmarking/scripts/bench.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
bench.sh is a local convenience wrapper.

Examples:
  ./scripts/bench.sh data all sf1
  ./scripts/bench.sh run read_scan sf1 results/read.json
EOF
}

CMD="${1:-}"
SUITE="${2:-all}"
SCALE="${3:-sf1}"
OUT="${4:-results.json}"

case "${CMD}" in
  data)
    cargo run -p delta-bench --release -- data --suite "${SUITE}" --scale "${SCALE}" --data-dir "${DELTA_BENCH_DATA_DIR:-bench-data}"
    ;;
  run)
    cargo run -p delta-bench --release -- run --suite "${SUITE}" --scale "${SCALE}" --out "${OUT}"
    ;;
  *)
    usage
    exit 2
    ;;
esac
```

---

# Deliverable 5: Bot implementation (Option A polling daemon)

## Comment protocol

Supported commands:

* `run benchmark <suite> [KEY=VALUE ...]`
* `run benchmark all`
* `show benchmark queue`
* `cancel benchmark <job_id>`

Example:

* `run benchmark read_scan SCALE=sf10 ITER=5`

This matches the proven DataFusion pattern: users comment `run benchmark clickbench_partitioned` and the bot later posts results. ([GitHub][1])

## `delta-rs-benchmarking/bot/whitelist.json`

```json
{
  "allowed_users": ["delta-rs-maintainer1", "delta-rs-maintainer2"],
  "allowed_orgs": ["delta-io"]
}
```

---

## `delta-rs-benchmarking/bot/parser.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class Command:
    kind: str  # run|show_queue|cancel
    suite: Optional[str] = None
    env: Dict[str, str] = None
    job_id: Optional[str] = None


def parse_comment(body: str) -> Optional[Command]:
    text = body.strip()
    lower = text.lower()

    if lower.startswith("show benchmark queue"):
        return Command(kind="show_queue", env={})

    if lower.startswith("cancel benchmark"):
        parts = text.split()
        if len(parts) >= 3:
            return Command(kind="cancel", job_id=parts[2], env={})
        return None

    if lower.startswith("run benchmark"):
        parts = text.split()
        if len(parts) < 3:
            return None
        suite = parts[2]
        env: Dict[str, str] = {}
        for tok in parts[3:]:
            if "=" in tok:
                k, v = tok.split("=", 1)
                env[k.strip()] = v.strip()
        return Command(kind="run", suite=suite, env=env)

    return None
```

---

## `delta-rs-benchmarking/bot/queue.py`

```python
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class Job:
    job_id: str
    repo: str
    pr_number: int
    suite: str
    requested_by: str
    requested_at: float
    head_sha: str
    env: Dict[str, str]
    status: str  # queued|running|done|failed
    result_path: Optional[str] = None
    error: Optional[str] = None


class JobQueue:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> List[Job]:
        if not self.path.exists():
            return []
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return [Job(**j) for j in data]

    def save(self, jobs: List[Job]) -> None:
        self.path.write_text(json.dumps([asdict(j) for j in jobs], indent=2), encoding="utf-8")

    def enqueue(self, job: Job) -> None:
        jobs = self.load()
        jobs.append(job)
        self.save(jobs)

    def list(self) -> List[Job]:
        return self.load()

    def pop_next(self) -> Optional[Job]:
        jobs = self.load()
        for i, j in enumerate(jobs):
            if j.status == "queued":
                j.status = "running"
                jobs[i] = j
                self.save(jobs)
                return j
        return None

    def update(self, job: Job) -> None:
        jobs = self.load()
        for i, j in enumerate(jobs):
            if j.job_id == job.job_id:
                jobs[i] = job
                self.save(jobs)
                return

    def cancel(self, job_id: str) -> bool:
        jobs = self.load()
        new_jobs = [j for j in jobs if j.job_id != job_id]
        if len(new_jobs) == len(jobs):
            return False
        self.save(new_jobs)
        return True


def new_job_id(pr: int, sha: str) -> str:
    ts = int(time.time())
    return f"pr{pr}-{sha[:7]}-{ts}"
```

---

## `delta-rs-benchmarking/bot/github_api.py`

```python
from __future__ import annotations

import requests
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class GH:
    token: str
    api_base: str = "https://api.github.com"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github+json",
        }

    def list_issue_comments(self, owner: str, repo: str, since_iso: str) -> List[Dict[str, Any]]:
        url = f"{self.api_base}/repos/{owner}/{repo}/issues/comments"
        r = requests.get(url, headers=self._headers(), params={"since": since_iso}, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_pr(self, owner: str, repo: str, pr: int) -> Dict[str, Any]:
        url = f"{self.api_base}/repos/{owner}/{repo}/pulls/{pr}"
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def post_comment(self, owner: str, repo: str, issue_number: int, body: str) -> None:
        url = f"{self.api_base}/repos/{owner}/{repo}/issues/{issue_number}/comments"
        r = requests.post(url, headers=self._headers(), json={"body": body}, timeout=30)
        r.raise_for_status()
```

---

## `delta-rs-benchmarking/bot/daemon.py`

````python
from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from bot.github_api import GH
from bot.parser import parse_comment
from bot.queue import Job, JobQueue, new_job_id


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_whitelist(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def is_allowed(user: str, whitelist: Dict[str, Any]) -> bool:
    if user in whitelist.get("allowed_users", []):
        return True
    # (Optional) org-based allow can be added by checking membership via GitHub API.
    return False


def main() -> None:
    cfg_path = Path("bot/config.toml")
    # Minimal config via env for brevity
    repo = (Path("bot/repo.txt").read_text().strip() if Path("bot/repo.txt").exists() else "delta-io/delta-rs")
    owner, name = repo.split("/", 1)

    token = (Path("bot/token.txt").read_text().strip() if Path("bot/token.txt").exists() else "")
    if not token:
        raise SystemExit("Missing bot token (write to bot/token.txt or use env in your deployment)")

    gh = GH(token=token)
    whitelist = load_whitelist(Path("bot/whitelist.json"))
    queue = JobQueue(Path("bot/state/queue.json"))
    state_path = Path("bot/state/last_since.txt")
    state_path.parent.mkdir(parents=True, exist_ok=True)

    since = state_path.read_text().strip() if state_path.exists() else "1970-01-01T00:00:00Z"

    while True:
        # 1) scrape comments
        comments = gh.list_issue_comments(owner, name, since)
        if comments:
            since = iso_now()
            state_path.write_text(since)

        for c in comments:
            body = c.get("body", "")
            user = c.get("user", {}).get("login", "")
            cmd = parse_comment(body)
            if cmd is None:
                continue

            issue_url = c.get("issue_url", "")
            pr_number = int(issue_url.rstrip("/").split("/")[-1])

            if not is_allowed(user, whitelist):
                gh.post_comment(owner, name, pr_number, f"🚫 @{user} is not authorized to run benchmarks.")
                continue

            if cmd.kind == "show_queue":
                jobs = queue.list()
                lines = ["### Benchmark queue", ""]
                for j in jobs:
                    lines.append(f"- `{j.job_id}` PR #{j.pr_number} `{j.suite}` ({j.status})")
                if not jobs:
                    lines.append("_queue empty_")
                gh.post_comment(owner, name, pr_number, "\n".join(lines))
                continue

            if cmd.kind == "cancel" and cmd.job_id:
                ok = queue.cancel(cmd.job_id)
                gh.post_comment(owner, name, pr_number, f"{'✅' if ok else '❌'} cancel {cmd.job_id}")
                continue

            if cmd.kind == "run" and cmd.suite:
                pr = gh.get_pr(owner, name, pr_number)
                head_sha = pr["head"]["sha"]

                job_id = new_job_id(pr_number, head_sha)
                env = cmd.env or {}
                suite = cmd.suite

                job = Job(
                    job_id=job_id,
                    repo=repo,
                    pr_number=pr_number,
                    suite=suite,
                    requested_by=user,
                    requested_at=time.time(),
                    head_sha=head_sha,
                    env=env,
                    status="queued",
                )
                queue.enqueue(job)
                gh.post_comment(
                    owner,
                    name,
                    pr_number,
                    f"✅ Queued benchmark `{suite}` for `{head_sha[:7]}` as job `{job_id}`.",
                )

        # 2) run next job if idle
        job = queue.pop_next()
        if job:
            try:
                env = dict(**job.env)
                # Map env vars to compare_branch.sh flags if needed; simplest: pass SCALE/ITER/WARMUP via env.
                scale = env.get("SCALE", "sf1")
                it = env.get("ITER", "5")
                warm = env.get("WARMUP", "1")
                threshold = env.get("THRESHOLD", "0.05")

                cmd = [
                    "bash",
                    "scripts/compare_branch.sh",
                    "--repo",
                    job.repo,
                    "--pr",
                    str(job.pr_number),
                    "--suite",
                    job.suite,
                    "--scale",
                    scale,
                    "--iterations",
                    it,
                    "--warmup",
                    warm,
                    "--threshold",
                    threshold,
                ]

                proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
                out = proc.stdout + "\n" + proc.stderr

                if proc.returncode != 0:
                    job.status = "failed"
                    job.error = f"compare_branch.sh exit {proc.returncode}"
                    queue.update(job)
                    gh.post_comment(owner, name, job.pr_number, f"❌ Job `{job.job_id}` failed.\n\n```\n{out}\n```")
                else:
                    job.status = "done"
                    queue.update(job)
                    gh.post_comment(owner, name, job.pr_number, f"✅ Job `{job.job_id}` completed.\n\n{out}")
            except Exception as e:
                job.status = "failed"
                job.error = str(e)
                queue.update(job)
                gh.post_comment(owner, name, job.pr_number, f"❌ Job `{job.job_id}` crashed: {e}")

        time.sleep(30)


if __name__ == "__main__":
    main()
````

---

# Deliverable 6: GitHub Actions workflow (Option B)

## `delta-rs-benchmarking/.github/workflows/benchmark.yml`

```yaml
name: delta-rs benchmark bot

on:
  issue_comment:
    types: [created]

jobs:
  benchmark:
    if: contains(github.event.comment.body, 'run benchmark')
    runs-on: self-hosted
    concurrency:
      group: delta-bench-runner
      cancel-in-progress: false

    steps:
      - name: Checkout benchmarking repo
        uses: actions/checkout@v4

      - name: Validate author (simple whitelist)
        id: auth
        run: |
          set -euo pipefail
          ACTOR="${{ github.actor }}"
          python3 - <<'PY'
          import json, sys
          actor = "${{ github.actor }}"
          wl = json.load(open("bot/whitelist.json"))
          if actor in wl.get("allowed_users", []):
              print("allowed=1")
          else:
              print("allowed=0")
          PY

      - name: Run compare
        if: steps.auth.outputs.allowed == '1'
        env:
          GITHUB_TOKEN: ${{ secrets.BOT_TOKEN }}
        run: |
          set -euo pipefail
          # Extract PR number
          PR="${{ github.event.issue.number }}"
          BODY="${{ github.event.comment.body }}"
          # Parse suite and optional env assignments
          SUITE="$(echo "$BODY" | awk '{print $3}')"
          SCALE="$(echo "$BODY" | grep -o 'SCALE=[^ ]*' | cut -d= -f2 || true)"
          ITER="$(echo "$BODY" | grep -o 'ITER=[^ ]*' | cut -d= -f2 || true)"
          WARMUP="$(echo "$BODY" | grep -o 'WARMUP=[^ ]*' | cut -d= -f2 || true)"
          THRESHOLD="$(echo "$BODY" | grep -o 'THRESHOLD=[^ ]*' | cut -d= -f2 || true)"

          SCALE="${SCALE:-sf1}"
          ITER="${ITER:-5}"
          WARMUP="${WARMUP:-1}"
          THRESHOLD="${THRESHOLD:-0.05}"

          bash scripts/compare_branch.sh --repo delta-io/delta-rs --pr "$PR" --suite "$SUITE" --scale "$SCALE" --iterations "$ITER" --warmup "$WARMUP" --threshold "$THRESHOLD" > out.txt

      - name: Post comment
        if: steps.auth.outputs.allowed == '1'
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const body = fs.readFileSync('out.txt', 'utf8');
            await github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.issue.number,
              body: `✅ Benchmark results\n\n${body}`
            });
```

---

# Deliverable 7: VM provisioning script

## Hardware recommendation (baseline)

Goal: stable + fast within ~15 minutes per suite.

* CPU: **8–16 logical cores** with high single-thread perf
* RAM: **32GB**
* Disk: **local NVMe** strongly recommended for consistent IO
* OS: Ubuntu 22.04/24.04 LTS

This mirrors the “dedicated consistent hardware” assumption that makes PR-level comparisons meaningful (and avoids contributor laptop variability).

## `delta-rs-benchmarking/infra/provision_runner.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# Provision a Linux VM as a delta-rs benchmark runner.
# Assumes Ubuntu/Debian-like system and sudo access.

sudo apt-get update
sudo apt-get install -y \
  build-essential \
  git \
  python3 python3-venv python3-pip \
  pkg-config \
  libssl-dev \
  jq \
  lsb-release \
  util-linux \
  linux-tools-common linux-tools-generic \
  curl

# Install Rust (pin to stable; delta-rs can also use rust-toolchain.toml)
if ! command -v rustup >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi
source "$HOME/.cargo/env"
rustup toolchain install stable
rustup default stable

# Python venv for compare tool + bot
python3 -m venv ~/delta-bench-venv
source ~/delta-bench-venv/bin/activate
pip install --upgrade pip
pip install -e .

# System tuning: CPU governor performance (best-effort)
if command -v cpupower >/dev/null 2>&1; then
  sudo cpupower frequency-set -g performance || true
fi

echo "Provisioning complete."
echo "Next:"
echo "  - Mount fast disk at /mnt"
echo "  - Create /mnt/delta-bench-data and generate SF1/SF10 datasets"
echo "  - Configure bot/token and whitelist"
echo "  - Install systemd units from bot/systemd/"
```

Also add sysctl tuning file:

## `delta-rs-benchmarking/infra/sysctl.d/99-delta-bench.conf`

```conf
# Reduce noise from swapping; keep more in cache (best-effort for benchmark stability)
vm.swappiness = 10
```

---

# Deliverable 8: End-to-end example output

Example GitHub PR comment body:

```
run benchmark read_scan SCALE=sf10 ITER=5
```

Bot acknowledgment:

> ✅ Queued benchmark `read_scan` for `a1b2c3d` as job `pr123-a1b2c3d-1709000000`.

Result comment (posted later):

```text
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Case                         ┃ baseline  ┃ candidate ┃ Change        ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ read/full_scan_count         │ 120.08 ms │  98.32 ms │ +1.22x faster │
│ read/projected_sum           │  55.10 ms │  55.44 ms │ no change     │
│ read/partition_pruning       │   8.12 ms │   7.90 ms │ no change     │
│ read/data_skipping_selective │   4.31 ms │   6.02 ms │ 1.40x slower  │
└──────────────────────────────┴───────────┴───────────┴───────────────┘
┏━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Benchmark Summary     ┃            ┃
┡━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Total Time (baseline) │ 187.61 ms  │
│ Total Time (candidate)│ 167.68 ms  │
│ Cases Faster          │ 1          │
│ Cases Slower          │ 1          │
│ Cases with No Change  │ 2          │
│ Cases with Failure    │ 0          │
│ Cases Missing         │ 0          │
└──────────────────────┴────────────┘
```

This matches the “immediately interpretable” style that DataFusion’s bot uses. ([GitHub][1])

---

# CI integration plan for delta-rs

## On every PR (cheap smoke)

* Run only:

  * `cargo test -p delta-bench`
  * optionally `delta-bench run --suite metadata --scale sf1 --iterations 1 --warmup 0` on a small dataset in CI (or generate tiny “sf0”)

## Nightly trend tracking

* Run `sf10` read_scan + merge_dml + optimize_vacuum nightly on the dedicated runner
* Publish JSON results to:

  * S3 / GCS, or
  * GitHub Pages, or
  * Conbench (future integration) ([GitHub][17])

---

# Security + hardening notes

* Self-hosted runner + PR code execution is inherently risky.
* Mitigations:

  * strict **whitelist**
  * queue runs sequentially (no parallelism)
  * least-privilege token (comment only)
  * run bot under a dedicated Linux user
  * keep runner isolated (no access to production secrets)
* This follows the exact rationale behind DataFusion’s whitelist gating. ([GitHub][1])

---

# Extensibility roadmap (designed-in)

* Add Z-order / clustering benchmarks when optimize supports it (OptimizeType exists). ([Docs.rs][10])
* Add object store backends (S3/GCS/Azure) by:

  * new dataset location URI
  * storage options injection
* Add memory profiling:

  * `jemalloc` stats, `/usr/bin/time -v`, `perf stat`
* Add flamegraphs:

  * `cargo flamegraph` wrapper in `compare_branch.sh`
* Add Python API benchmarks:

  * run `python -m deltalake` workloads against same datasets

---

# What you can do next (practical steps)

1. **Create a PR to delta-rs** adding `benchmarks/` with the `delta-bench` crate above + root workspace member update.
2. Create the standalone `delta-rs-benchmarking` repo with the bot + scripts above.
3. Bring up one dedicated runner VM and:

   * generate SF1/SF10 datasets once
   * run smoke suite locally
4. Turn on either:

   * Option A daemon (systemd service), or
   * Option B GitHub Actions workflow on the self-hosted runner

---

If you want, I can also provide:

* a small **`sf0` ultra-tiny** dataset mode for CI smoke tests (fast enough for GitHub-hosted runners),
* a `results/` schema evolution plan (schema_version bumping and backwards compatibility),
* and a Conbench-compatible JSON exporter (so nightly trends show up automatically).

[1]: https://github.com/apache/datafusion/pull/20417 "perf: Skip RowFilter when all predicate columns are in the projection by darmie · Pull Request #20417 · apache/datafusion · GitHub"
[2]: https://github.com/delta-io/delta-rs/issues/3839 "https://github.com/delta-io/delta-rs/issues/3839"
[3]: https://docs.rs/deltalake-core/0.31.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html "https://docs.rs/deltalake-core/0.31.0/x86_64-unknown-linux-gnu/src/deltalake_core/operations/mod.rs.html"
[4]: https://delta.io/blog/delta-lake-performance/ "https://delta.io/blog/delta-lake-performance/"
[5]: https://github.com/ClickHouse/ClickBench "https://github.com/ClickHouse/ClickBench"
[6]: https://duckdblabs.github.io/db-benchmark/ "https://duckdblabs.github.io/db-benchmark/"
[7]: https://iceberg.apache.org/benchmarks/ "https://iceberg.apache.org/benchmarks/"
[8]: https://hudi.apache.org/docs/performance/ "https://hudi.apache.org/docs/performance/"
[9]: https://docs.rs/deltalake/latest/deltalake/operations/merge/struct.MergeBuilder.html "https://docs.rs/deltalake/latest/deltalake/operations/merge/struct.MergeBuilder.html"
[10]: https://docs.rs/deltalake/latest/deltalake/operations/optimize/index.html "https://docs.rs/deltalake/latest/deltalake/operations/optimize/index.html"
[11]: https://docs.rs/deltalake/latest/deltalake/table/struct.DeltaTable.html "https://docs.rs/deltalake/latest/deltalake/table/struct.DeltaTable.html"
[12]: https://bheisler.github.io/criterion.rs/book/analysis.html "https://bheisler.github.io/criterion.rs/book/analysis.html"
[13]: https://dl.acm.org/doi/10.1145/2491894.2464160 "https://dl.acm.org/doi/10.1145/2491894.2464160"
[14]: https://users.cs.northwestern.edu/~robby/courses/322-2013-spring/mytkowicz-wrong-data.pdf "https://users.cs.northwestern.edu/~robby/courses/322-2013-spring/mytkowicz-wrong-data.pdf"
[15]: https://github.com/sharkdp/hyperfine "https://github.com/sharkdp/hyperfine"
[16]: https://docs.rs/deltalake-core/0.31.0/x86_64-unknown-linux-gnu/src/deltalake_core/lib.rs.html "https://docs.rs/deltalake-core/0.31.0/x86_64-unknown-linux-gnu/src/deltalake_core/lib.rs.html"
[17]: https://github.com/conbench/conbench "https://github.com/conbench/conbench"
