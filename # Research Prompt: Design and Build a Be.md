# Research Prompt: Design and Build a Best-in-Class Benchmark Suite and Automated PR Benchmark Bot for delta-rs

## Objective

Design a complete, production-quality benchmark suite and automated PR benchmark bot for the [delta-io/delta-rs](https://github.com/delta-io/delta-rs) project — a native Rust implementation of Delta Lake with Python bindings. The deliverable is a fully implementable specification: repository layout, all source code, scripts, CI configuration, and documentation — ready to be contributed as a PR or standalone repo.

The system must match or exceed the quality of the benchmark infrastructure used by [Apache DataFusion](https://github.com/apache/datafusion) (the `alamb-ghbot` system built by Andrew Lamb, powered by the [datafusion-benchmarking](https://github.com/alamb/datafusion-benchmarking) repo), while being purpose-built for the unique workload characteristics of a Delta Lake table format library rather than a SQL query engine.

---

## Context and Background

### What is delta-rs?

delta-rs is a native Rust library for Delta Lake with Python bindings (via PyO3). Its workspace structure:

```
delta-rs/
├── Cargo.toml              # Workspace root
├── crates/
│   ├── core/               # Core Delta Lake implementation (deltalake-core)
│   ├── aws/                # S3 storage backend (deltalake-aws)
│   ├── azure/              # Azure storage backend
│   ├── gcp/                # GCS storage backend
│   ├── deltalake/          # Public API crate (re-exports)
│   └── mount/              # Filesystem mounting
├── delta-inspect/           # CLI tool
└── python/                  # Python bindings
```

Key dependencies: Arrow 55+, DataFusion 50+, delta_kernel 0.10+, object_store, parquet.

delta-rs operations include: table reads/scans, writes (append/overwrite), MERGE INTO, UPDATE, DELETE, OPTIMIZE (compaction, z-order), VACUUM, checkpointing, time travel, schema evolution, and partition pruning.

### The DataFusion Benchmark Bot (reference implementation)

Study the following in detail to understand the reference system:

1. **The bot in action**: https://github.com/apache/datafusion/pull/20417 — examine the full conversation to see how maintainers trigger benchmarks via PR comments (`run benchmark clickbench_partitioned`), how the bot acknowledges and queues jobs, how results are posted, and how the whitelist works.

2. **The benchmarking repo**: https://github.com/alamb/datafusion-benchmarking — study `scrape_comments.py`, `gh_compare_branch.sh`, and the overall architecture.

3. **DataFusion's built-in benchmark tooling**: https://github.com/apache/datafusion/tree/main/benchmarks — study `bench.sh`, `compare.py`, the `dfbench` binary, and how benchmark suites (TPC-H, TPC-DS, ClickBench) are structured with JSON output.

4. **The bot account**: https://github.com/alamb-ghbot — understand the separation between the bot identity and the benchmark infrastructure.

5. **The benchmark results format**: Study the comparison tables the bot produces — the `┏━━━` formatted output with "faster", "slower", "no change" classifications and the summary statistics.

### Why delta-rs needs this

delta-rs currently has no standardized performance benchmarking infrastructure. Contributors run ad-hoc local benchmarks on varied hardware (Apple Silicon vs x86, different core counts). Reviewers often merge performance-sensitive changes (DataFusion upgrades, kernel refactors, merge/compaction optimizations) without objective performance data. This creates risk of silent regressions.

---

## Research Requirements

### Part 1: Benchmark Suite Design

Research and specify the complete benchmark suite, covering:

#### 1.1 Benchmark Workload Selection

Research industry-standard approaches to benchmarking data lake table formats. Study:

- How Delta Lake (Spark) benchmarks itself (the delta-io/delta project's benchmarks)
- How Apache Iceberg benchmarks table operations
- How Apache Hudi benchmarks performance
- TPC-DS and TPC-H relevance to table format benchmarks (and where they fall short)
- How DuckDB, Polars, and other modern engines benchmark their Parquet/lakehouse readers
- Academic literature on storage format benchmarking methodology
- The [ClickBench](https://github.com/ClickHouse/ClickBench) methodology and why DataFusion adopted it
- How [db-benchmark](https://github.com/duckdblabs/db-benchmark) (H2O.ai) structures its suite

Design benchmark suites that cover:

| Category | Operations to Benchmark | Why It Matters |
|----------|------------------------|----------------|
| **Read / Scan** | Full scan, projected scan, filtered scan with partition pruning, filtered scan with data skipping (min/max stats), predicate pushdown effectiveness | Core read path performance, most common operation |
| **Write** | Append (small batches, large batches), overwrite, write with partitioning, write with various compression codecs | Write throughput, a primary user operation |
| **Merge (DML)** | MERGE INTO with varying match ratios (10%, 50%, 90%), source-target size ratios, update-only merges, insert-only merges, mixed | Most complex operation, highest regression risk |
| **Update / Delete** | Row-level UPDATE with selective predicates, DELETE with partition pruning, DELETE with file-level skipping | DML operations that touch the write path |
| **Optimize** | Bin-pack compaction (many small files → fewer large files), Z-ORDER optimization, file size targeting | Critical maintenance operation |
| **Vacuum** | Cleanup with varying tombstone counts, dry-run vs execute | Maintenance operation with metadata-heavy workload |
| **Metadata** | Table load time (cold start), checkpoint read, time travel to version N, schema evolution detection, log replay performance | Often overlooked but impacts every operation |
| **Checkpoint** | Checkpoint creation, checkpoint read, multi-part checkpoint handling | Affects table load time at scale |

For each benchmark:
- Define the SQL or API call being measured
- Specify what is NOT measured (data generation, table setup)
- Define warmup strategy (how many warmup iterations before timing)
- Define measurement strategy (wall clock, per-operation breakdown if possible)

#### 1.2 Data Generation

Design synthetic data generation that:

- Is **deterministic** (same seed produces identical tables across runs)
- Supports **scale factors** (SF1 = ~1M rows / ~100MB, SF10 = ~10M rows / ~1GB, SF100 = ~100M rows / ~10GB)
- Produces **realistic column distributions** (not uniform random — use Zipf, normal, categorical distributions that mimic real-world data)
- Includes **multiple table shapes**: wide tables (100+ columns), narrow tables (5 columns), deeply partitioned tables, unpartitioned tables
- Generates **realistic Delta table state**: tables with many small files (for compaction benchmarks), tables with deletion vectors, tables with multiple versions (for time travel), tables with checkpoints
- Is **reproducible** across platforms (x86 and ARM must produce identical data from the same seed)

Research how TPC-DS dbgen, DataFusion's `bench.sh data` command, and ClickBench data preparation work. Apply those patterns.

#### 1.3 Statistical Rigor

Research best practices for benchmark measurement:

- How to determine the number of iterations needed for statistical significance
- How to handle warmup (JIT-like effects in Rust are minimal, but OS page cache, file system cache, and CPU cache effects are real)
- Whether to report min, mean, median, or percentiles (study what Criterion.rs, hyperfine, and academic benchmarking literature recommend)
- How to classify "faster" vs "slower" vs "no change" — what threshold to use (DataFusion uses ~5%, but research whether this is optimal)
- How to handle outliers (should extreme values be trimmed?)
- How to account for system noise (study how Google's [PerfKit Benchmarker](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker) and [Conbench](https://conbench.github.io/conbench/) handle this)
- Whether to include confidence intervals in the comparison output

#### 1.4 Benchmark Binary Design

Design a Rust binary (`delta-bench`) following the patterns established by DataFusion's `dfbench`. The binary should:

- Be a proper CLI with `clap` argument parsing
- Support subcommands: `data` (generate), `run` (execute), `list` (show suites)
- Accept parameters for: scale factor, iterations, output path, specific query selection, environment variable overrides
- Output structured JSON compatible with the comparison script
- Support running against local filesystem tables (primary) with future extensibility to object stores
- Be designed to live either inside the delta-rs workspace (`benchmarks/` directory, like DataFusion) or as a standalone repo
- Include proper error handling, progress reporting, and graceful failure on individual queries

### Part 2: Comparison Tooling

#### 2.1 Comparison Script

Design a Python comparison script (`compare.py`) that:

- Takes two JSON result files (baseline and candidate) as input
- Produces the formatted comparison table matching DataFusion's style:

```
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query          ┃       main ┃    pr-branch  ┃        Change ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ read_full_scan │  120.08 ms │     98.32 ms  │ +1.22x faster │
│ merge_50pct    │ 1092.83 ms │   1105.41 ms  │     no change │
│ compact_small  │  450.56 ms │    612.88 ms  │  1.36x slower │
│ write_append   │      FAIL  │    312.44 ms  │  incomparable │
└────────────────┴────────────┴───────────────┴───────────────┘
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ Benchmark Summary                    ┃            ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ Total Time (main)                    │  1663.47ms │
│ Total Time (pr-branch)               │  2016.73ms │
│ Queries Faster                       │          1 │
│ Queries Slower                       │          1 │
│ Queries with No Change               │          1 │
│ Queries with Failure                 │          1 │
└──────────────────────────────────────┴────────────┘
```

- Supports configurable threshold for "no change" classification
- Handles missing queries gracefully (query added/removed between versions)
- Optionally outputs markdown for GitHub comment formatting
- Can generate charts/visualizations (PNG) for trend tracking

#### 2.2 Branch Comparison Script

Design a shell script (`compare_branch.sh`) that:

- Accepts a branch name, benchmark suite name, and optional environment variables
- Checks out and builds both `main` and the PR branch in release mode
- Generates benchmark data (or verifies existing data is current)
- Runs the benchmark suite on both builds
- Invokes `compare.py` and captures the output
- Handles build failures, timeout, and OOM gracefully
- Reports system information (OS, CPU, memory, kernel version) in the output header

### Part 3: Automated PR Benchmark Bot

#### 3.1 Bot Architecture

Research and design the bot infrastructure. Study:

- The `alamb-ghbot` / `scrape_comments.py` polling approach
- GitHub Actions `issue_comment` webhook approach  
- GitHub Apps vs personal access tokens for bot authentication
- Probot framework for GitHub bots
- How to handle job queuing (sequential execution is critical for reproducible results)

Design two implementation options:

**Option A: Standalone Bot (like DataFusion)**
- A Python daemon that polls PR comments via GitHub API
- Runs on a dedicated VM with consistent hardware
- Job queue with sequential execution
- Bot account posts results as comments

**Option B: GitHub Actions with Self-Hosted Runner**
- `issue_comment` triggered workflow
- Self-hosted runner on dedicated hardware
- Concurrency controls to prevent parallel benchmark runs
- Results posted via `actions/github-script` or `gh` CLI

For both options, specify:
- Authentication and secrets management
- Whitelist mechanism for authorized users
- Queue management (view queue, cancel jobs)
- Error handling and timeout behavior
- How to handle force-pushed branches (re-fetch before building)
- Rate limiting to prevent abuse

#### 3.2 Comment Protocol

Define the exact comment syntax the bot responds to:

```
run benchmark <suite> [ENV_VAR=value ...]
run benchmark all
show benchmark queue
cancel benchmark <job_id>
```

And the exact response format for:
- Acknowledgment (with commit SHA, diff link, system info)
- Queue position notification
- Results posting (with collapsible details)
- Error reporting
- Unauthorized user response

#### 3.3 Infrastructure Specification

Specify the dedicated benchmark VM:

- Cloud provider and instance type (GCP `c2-standard-8` or AWS `c5.2xlarge` or equivalent — justify the choice)
- OS and kernel configuration (CPU governor, NUMA pinning, IRQ affinity, transparent hugepages settings)
- Disk configuration (NVMe vs SSD, filesystem, mount options)
- Rust toolchain pinning strategy
- Data persistence (pre-generated benchmark data on persistent disk)
- Monitoring and alerting (how to detect if the bot is down)
- Cost optimization (always-on vs on-demand with scheduler)
- Security hardening (the bot has write access to PR comments — minimize attack surface)

### Part 4: Implementation Quality Standards

#### 4.1 Rust Code Quality

All Rust code must:

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `#![deny(clippy::all, clippy::pedantic)]` with justified `#[allow]` attributes
- Include comprehensive doc comments with examples
- Use `thiserror` for error types (not `anyhow` in library code)
- Have proper `Display` and `Debug` implementations
- Use `tracing` for structured logging (not `println!` for production output)
- Be formatted with `rustfmt` and linted with `clippy`
- Include unit tests for data generation determinism, result serialization, and comparison logic
- Include integration tests that run a minimal benchmark end-to-end

#### 4.2 Python Code Quality

All Python code must:

- Target Python 3.10+
- Use type hints throughout
- Include a `pyproject.toml` with `ruff` for linting
- Use `rich` for table formatting (the box-drawing comparison tables)
- Include argument parsing with `argparse` or `click`
- Have docstrings on all public functions
- Include tests with `pytest`

#### 4.3 Shell Script Quality

All shell scripts must:

- Use `#!/usr/bin/env bash` and `set -euo pipefail`
- Include usage documentation
- Handle errors with descriptive messages
- Use shellcheck-clean code
- Support `--dry-run` mode for testing

#### 4.4 Documentation

Provide:

- `README.md` with quick-start guide, architecture overview, and examples
- `CONTRIBUTING.md` with instructions for adding new benchmark suites
- Inline documentation in all configuration files
- A `docs/` directory with architecture decisions (ADRs) for key design choices

### Part 5: Integration with delta-rs

#### 5.1 Placement Options

Research and recommend whether the benchmark suite should:

**A) Live inside the delta-rs repo** (like DataFusion's `benchmarks/` directory)
- Pro: version-locked to the crate, no dependency sync issues
- Con: increases repo size, benchmark data generation adds CI time

**B) Live as a standalone repo** (like `apache/datafusion-benchmarks`)
- Pro: independent release cycle, cleaner separation
- Con: version pinning complexity, cross-repo coordination

**C) Hybrid approach**
- Benchmark binary inside delta-rs, bot infrastructure in a separate repo

Justify your recommendation based on the delta-rs project's governance, release cadence, and contributor workflow.

#### 5.2 CI Integration

Design how benchmarks integrate with delta-rs CI:

- Should any benchmarks run on every PR (smoke test)?
- Should there be a nightly benchmark run against `main` for trend tracking?
- How should results be stored long-term (database, GitHub Pages, S3)?
- Should benchmark regressions ever block merges (advisory vs gating)?

#### 5.3 Future Extensibility

Design for future growth:

- Adding benchmarks for new operations (e.g., CLUSTER BY, liquid clustering)
- Benchmarking against cloud storage (S3, GCS, ADLS) in addition to local
- Comparing delta-rs performance across releases over time (trend charts)
- Comparing delta-rs against Delta Spark or other implementations
- Supporting the Python API benchmarks (not just Rust)
- Memory profiling in addition to wall-clock timing
- Flamegraph generation for regression investigation

---

## Deliverables

Produce the following concrete artifacts:

1. **Complete repository layout** with every file path and its purpose
2. **All Rust source code** for the `delta-bench` binary (data generation, benchmark suites, runner, results)
3. **All Python scripts** (compare.py, scrape_comments.py or equivalent)
4. **All shell scripts** (compare_branch.sh, bench.sh)
5. **GitHub Actions workflow** (`.github/workflows/benchmark.yml`)
6. **Cargo.toml** with exact dependency versions
7. **README.md** and all documentation
8. **VM setup script** for provisioning the benchmark runner
9. **Example output** showing what a benchmark run and comparison looks like end-to-end

---

## Quality Bar

The benchmark suite should be good enough that:

1. A delta-rs maintainer could drop a `run benchmark read_scan` comment on any PR and get useful, trustworthy results within 15 minutes
2. A contributor could run `delta-bench run read_scan` locally and get directional results on their laptop
3. The comparison output is immediately interpretable by someone unfamiliar with the tool
4. Adding a new benchmark query requires adding ~20 lines of code, not restructuring the system
5. The data generation is fast enough to not be a bottleneck (< 2 minutes for SF1)
6. Results are reproducible to within ±3% on the same hardware across runs
7. The system handles failures gracefully — a benchmark that panics doesn't take down the whole suite
8. The codebase itself is a model of Rust engineering quality that the delta-rs community would be proud to maintain

---

## Research Sources to Prioritize

1. https://github.com/apache/datafusion/tree/main/benchmarks (DataFusion benchmark suite)
2. https://github.com/alamb/datafusion-benchmarking (DataFusion bot infrastructure)  
3. https://github.com/apache/datafusion/pull/20417 (Bot in action — study the full conversation)
4. https://github.com/apache/datafusion-benchmarks (Cross-engine comparison benchmarks)
5. https://github.com/delta-io/delta-rs (delta-rs repository — study the crate structure, operations API, and existing tests)
6. https://github.com/delta-io/delta-kernel-rs (kernel crate — understand the boundary between kernel and delta-rs)
7. https://github.com/ClickHouse/ClickBench (ClickBench methodology)
8. https://criterion.rs/ (Criterion.rs — the gold standard for Rust microbenchmarks, study its statistical methodology even though we're doing macro benchmarks)
9. https://github.com/sharkdp/hyperfine (hyperfine — command-line benchmarking, study its statistical output)
10. https://conbench.github.io/conbench/ (Conbench — Apache Arrow's continuous benchmarking platform, study its approach to tracking regressions over time)
11. https://docs.rs/deltalake-core/latest (delta-rs API documentation — understand what operations exist)
12. https://github.com/delta-io/delta-rs/blob/main/crates/core/src/operations/ (study each operation module to understand what needs benchmarking)
13. Research papers on benchmarking methodology: "Rigorous Benchmarking in Reasonable Time" (Kalibera & Jones), "Producing Wrong Data Without Doing Anything Obviously Wrong!" (Mytkowicz et al.)