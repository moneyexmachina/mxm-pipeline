# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Initial package scaffold with Poetry and `src/` layout.
- CLI entrypoint `mxm-pipeline` with commands: `list`, `graph`, `run`, `status` (stub).
- Typed spec: `AssetDecl`, `TaskSpec`, `FlowSpec`; recursive `JSONValue` alias; `TaskFn`.
- CI via GitHub Actions: lint (ruff/black/isort), type-check (pyright strict), tests (pytest).
- Daily smoke workflow (cron) exercising CLI.
- Makefile targets: `install`, `lock`, `fmt`, `lint`, `type`, `test`, `check`, plus `flows/graph/run`.
- Docs: ADR 0001 (“asset layer from day 1”), work-package in `docs/workpackages/`.
- README badges (version, license, Python, Pyright, CI).


- Prefect 2.x adapter: translate `FlowSpec`/`TaskSpec` into runnable Prefect flows.
  - `build_prefect_flow(flow_spec)` — validates duplicate names, unknown upstreams, cycles; returns a Prefect `@flow` callable.
  - `run_prefect_flow(flow_spec, params)` — wraps callables as Prefect tasks (retries, retry delay), wires dependencies, merges params with precedence **runtime > flow > task**, emits structured asset logs (`ASSET write <id> partition=<key>:<val>`), returns `{task_name: result}`.
- Test suite for adapter (build + run), fully **pyright strict** and **ruff** clean.
- Pytest session fixture to silence Prefect/Rich shutdown noise during tests (no effect on runtime behavior).

### Changed
- Dependency constraints compatible with Prefect 3.x (Typer `~0.19`, Python `>=3.13,<3.15`).
- `pyproject.toml`: added extra `orchestration = ["prefect", "networkx"]`.

### Fixed
- Pyright “partially unknown” issues via typed default factories.
- Ruff UP040 by adopting PEP 695 `type` alias for `JSONValue`.

### Removed
- N/A

### Notes
- Asset layer is **present but minimal** at this stage: adapter only tags/logs asset writes; no catalog/freshness/caching yet (slated for M4).
---

<!-- Keep this reference placeholder. After the first release tag,
     update [Unreleased] to compare against the latest tag, e.g.:
     [Unreleased]: https://github.com/moneyexmachina/mxm-pipeline/compare/v0.1.0...HEAD
-->

[Unreleased]: https://github.com/moneyexmachina/mxm-pipeline/compare/HEAD...main
