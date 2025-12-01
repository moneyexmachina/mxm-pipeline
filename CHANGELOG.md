# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]
### Added
- Placeholder for upcoming changes.

---

## [0.1.0] - 2025-12-01

### Added
- Initial package scaffold using Poetry with a strict `src/` layout.
- CLI entrypoint `mxm-pipeline` with commands:
  - `list` — enumerate registered flows.
  - `graph` — ASCII dependency graph for a flow.
  - `run` — execute a flow with typed `--param key=value` overrides.
  - `status` — stub for future backends.
- Full three-layer architecture:
  - **Spec layer** (`spec.py`): `FlowSpec`, `TaskSpec`, `AssetDecl`.
  - **API layer** (`api.py`): `compile_flow()`, `execute_flow()`.
  - **Adapter layer** (`adapters/prefect_adapter.py`): backend translation + runtime wrapper.
- Complete Prefect 2/3 adapter rewrite:
  - Deterministic flow construction with validation (duplicates, unknown upstreams, cycles).
  - Runtime execution with dependency resolution, retries, and result merging.
  - Parameter precedence: **runtime > flow > task**.
  - MXMFlow protocol and `PrefectMXMFlow` wrapper.
  - Asset logging: structured `ASSET write <id> partition=<k>:<v>`.
  - Clean suppression of Prefect/Rich noise during tests.
- Integration of `mxm-types`:
  - `JSONValue`, `JSONObj`, `JSONMap`, `JSONLike`, and shared protocol types.
  - All layers now strictly typed and pyright-clean.
- Registry system for discovering flows and exposing the demo flow.
- Updated demo flow consistent with MXM task signature conventions.
- ASCII graph printer for flow visualisation (used by CLI).
- Full test suite:
  - Spec tests, adapter build/execute tests, API tests, CLI smoke tests.
  - 100% strict pyright and ruff cleanliness.
- CI via GitHub Actions:
  - Lint (ruff, black, isort), type-check (pyright strict), tests (pytest).
  - Daily cron smoke test running CLI commands.
- Makefile targets: `install`, `lock`, `fmt`, `lint`, `type`, `test`, `check`, plus `flows/*` utilities.
- Documentation skeleton:
  - ADR 0001 (“asset layer from day 1”).
  - Work-package notes under `docs/workpackages/`.
  - Initial RUNBOOK structure.
- README badges for version, license, Python, Pyright, and CI status.

### Changed
- Dependency constraints aligned with Prefect 3.x compatibility (`typer~=0.19`, Python `>=3.13,<3.15`).
- Extra `orchestration` dependency group including Prefect + NetworkX.
- Unified flow-building/flow-running API to remove adapter leakage into CLI and tests.

### Fixed
- Pyright “partially unknown” errors resolved via explicit typed defaults.
- Ruff UP040 fixed by migrating to PEP-695 `type` aliases for JSON types.
- Silenced Prefect/Rich shutdown noise during test teardown.

### Removed
- Legacy adapter functions with ambiguous build/run semantics.
- No longer expose Prefect internals at CLI or API level.

### Notes
- Asset layer is **present but minimal** at this stage: adapter only tags/logs asset writes; no catalog/freshness/caching yet (slated for M4).
---

<!-- Reference links -->
[Unreleased]: https://github.com/moneyexmachina/mxm-pipeline/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/moneyexmachina/mxm-pipeline/releases/tag/v0.1.0

