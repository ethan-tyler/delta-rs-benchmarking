# ADR 0001: Repo-First Harness

## Decision

Build initial production `delta-bench` in `delta-rs-benchmarking` and benchmark `delta-rs` via a local managed checkout (`.delta-rs-under-test`).

## Rationale

- Faster iteration while architecture is evolving.
- Keeps bot/automation concerns separate from `delta-rs` core repository.
- Supports eventual upstreaming once APIs stabilize.
