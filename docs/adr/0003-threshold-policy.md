# ADR 0003: Threshold Policy

## Decision

Use a default no-change threshold of ±5% (`0.05`) for branch comparisons.

## Rationale

- Matches practical macro-benchmark noise on developer-hosted systems.
- Aligns with common tooling defaults and prior art.
- Can be overridden with `--noise-threshold` when hardware stability allows tighter bounds.

## Enforcement

Wave 1 is advisory only. Comparisons are informational and do not gate merges.
