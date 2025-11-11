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

### Changed
- Dependency constraints compatible with Prefect 3.x (Typer `~0.19`, Python `>=3.13,<3.15`).

### Fixed
- Pyright “partially unknown” issues via typed default factories.
- Ruff UP040 by adopting PEP 695 `type` alias for `JSONValue`.

### Removed
- N/A

---

<!-- Keep this reference placeholder. After the first release tag,
     update [Unreleased] to compare against the latest tag, e.g.:
     [Unreleased]: https://github.com/moneyexmachina/mxm-pipeline/compare/v0.1.0...HEAD
-->

[Unreleased]: https://github.com/moneyexmachina/mxm-pipeline/compare/HEAD...main
