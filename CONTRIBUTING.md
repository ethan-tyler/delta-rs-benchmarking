# Contributing

## Local Setup

Set up the harness and managed `delta-rs` checkout:

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

If you plan to run Python interop or longitudinal workflows, install the optional Python dependencies:

```bash
python3 -m pip install -r python/requirements-audit.txt
```

## Required Checks

Before opening a pull request, run the same baseline checks enforced by CI:

```bash
cargo test --locked
(cd python && python3 -m pytest -q tests)
```

When touching label validation, also run:

```bash
cargo test cli_validation
```

When touching longitudinal storage, reporting, or retention, also run:

```bash
(cd python && python3 -m pytest -q tests/test_longitudinal_store.py tests/test_longitudinal_reporting.py tests/test_longitudinal_retention.py tests/test_longitudinal_e2e_smoke.py)
```

## Pull Request Expectations

- keep changes scoped to one task or behavior change per commit when practical
- add or update tests before changing production behavior
- update docs when CLI behavior, workflow contracts, or storage formats change
- do not bypass CI failures; fix them or document the blocker clearly in the PR

## Benchmarking Notes

- Prefer `./scripts/compare_branch.sh --current-vs-main all` for exploratory branch comparisons.
- Use `./scripts/compare_branch.sh --current-vs-main --compare-mode decision all` only on pinned self-hosted hardware with schema v4 outputs.
- Prefer the longitudinal pipeline for time-series or release-history analysis.
- Do not commit generated fixtures, results, or managed checkout state.
