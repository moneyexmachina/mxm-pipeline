# M2.1 — CLI Integration for Prefect Adapter

Connects Prefect backend to CLI: `list`, `graph <flow>`, `run <flow> [--param k=v...]`.

## Scope
- Add flow registry (`registry.py`) and demo flow (`demos/prefect_demo.py`).
- ASCII DAG printer (`graph_ascii.py`).
- Extend `mxm.pipeline.cli` with list/graph/run.
- Smoke tests via `subprocess`.
- Docs (RUNBOOK) and CHANGELOG updates.

## Acceptance
- `mxm-pipeline list` shows `demo`.
- `mxm-pipeline graph demo` prints A->B, A->C, B->D, C->D.
- `mxm-pipeline run demo --param as_of=2025-11-11` returns JSON with A,B,C,D keys.
- `make lint`, `make type`, `make test` all green.

## Notes
- Prefect “quiet” env vars set in CLI path.
- Exit codes: 0 ok, 1 runtime error, 2 unknown flow.

## Checklist
- [ ] Registry + demo flow
- [ ] ASCII graph
- [ ] CLI commands
- [ ] Tests
- [ ] RUNBOOK
- [ ] CHANGELOG

