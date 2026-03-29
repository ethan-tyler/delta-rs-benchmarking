# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) when tagged releases are introduced.

## [Unreleased]

### Added

- Repository governance files: `SECURITY.md`, `CONTRIBUTING.md`, `LICENSE`, `.github/CODEOWNERS`, and this changelog.

### Changed

- README now frames the repository as benchmark tooling for `delta-rs` contributors and keeps harness change guidance in one place.
- CI now enforces a shared Rust/Python test baseline plus Rust and Python dependency audits on pushes and pull requests.
- Longitudinal benchmarking now resumes from an atomic `matrix-state.json` checkpoint and stores normalized history in `store.sqlite3`.

### Removed

- Standalone `CONTRIBUTING.md` and `SECURITY.md` pages; the repository no longer advertises separate open-source governance pages.

### Fixed

- Managed delta-rs checkout flows now serialize access, reject unsafe lock overrides, and clean up lock artifacts consistently.
- Self-hosted benchmark and longitudinal workflows now enforce hardened preflight before execution.
- Bash, Python, and Rust label validation now follow the same contract.

### Security

- Self-hosted workflows require run mode, no public IPv4, and egress-policy validation before benchmark work begins.

### Audit Closure

Wave re-verified on March 25, 2026 with `cargo test --locked`, `(cd python && python3 -m pytest -q tests)`, the self-hosted workflow hardening assertion, and the dependency audit commands listed in [docs/reference.md](docs/reference.md#repository-verification-baseline).

| Audit ID  | Commits                                               | Closure summary                                                                                              | Verification evidence                                                                                                                                   |
| --------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `REL-01`  | `746e177`, `53b5788`, `9ec3c83`, `dbe34d7`            | Serialized checkout access and lock-hygiene fixes for prepare/compare flows.                                 | `cargo test --locked`; `(cd python && python3 -m pytest -q tests)`                                                                                      |
| `SEC-01`  | `4a26ca3`, `7146c0a`                                  | Enforced hardened preflight on self-hosted benchmark and release-history workflows.                          | Self-hosted workflow assertion; `cargo test --locked`; `(cd python && python3 -m pytest -q tests)`                                                      |
| `REL-02`  | `f28bde9`, `1bdd197`                                  | Added the shared CI test workflow and locked Rust baseline.                                                  | `cargo test --locked`; `(cd python && python3 -m pytest -q tests)`                                                                                      |
| `REL-03`  | `73bc830`, `15bd777`                                  | Added Rust/Python dependency auditing and aligned Python auditing to the real runtime dependency set.        | `cargo audit --ignore RUSTSEC-2026-0037 --ignore RUSTSEC-2026-0041 --ignore RUSTSEC-2026-0049`; `python3 -m pip_audit -r python/requirements-audit.txt` |
| `PERF-01` | `d03d6de`, `4ea50ac`, `1e15849`, `31c0803`, `a622f84` | Made matrix state atomic, schema-validated, and bound to a configuration fingerprint.                        | `(cd python && python3 -m pytest -q tests)`                                                                                                             |
| `ARCH-01` | `e88f510`, `87a8e3d`, `4862b7a`                       | Unified label validation across runtimes and moved longitudinal analytics to the guarded SQLite store model. | `cargo test --locked`; `(cd python && python3 -m pytest -q tests)`                                                                                      |
| `DX-01`   | `314cf07`                                             | Added governance, ownership, release-hygiene, and audit-closure documentation.                               | `./scripts/docs_check.sh`; `(cd python && python3 -m pytest -q tests/test_docs_contract.py)`                                                            |
