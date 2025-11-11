# Context Pack — `mxm-pipeline v0.1.0`

## Core Decision & Rationale

We adopt **Prefect 2.x** as the orchestration backend and build our own **text-first CLI/TUI wrapper** (`mxm-pipeline`).
Flows and tasks are declared in pure Python; Prefect handles state, retries, and scheduling.
A minimal **optional asset declaration** is introduced now, so every task that writes data can declare and emit a standard `_asset.json` sidecar via `mxm-dataio`.

This ensures:

* ✅ Immediate productivity with Prefect DAGs.
* 🔁 Future compatibility with an **asset-centric view** (Dagster-style materializations & partitions).
* 🧮 Unified provenance: `_meta.json` for run context, `_asset.json` for persistent dataset state.
* 💬 Operator ergonomics: CLI/TUI-first, web UI optional.

Hence, v0.1.0 delivers a working orchestration engine **plus the conceptual hooks** for an asset catalog later—no rewrites required.

---

## Principles

* **Unix / text-first**: code & config in Git, reproducible & auditable.
* **DAG as core abstraction**; explicit dependencies, retries, backoff.
* **Operator comfort**: CLI/TUI over web dashboards.
* **Provenance everywhere** via `mxm-dataio`.
* **Local-first**, scalable later.
* **Backend-swappable** through a clean spec + adapter design.
* **Asset-aware from day 1**: optional `AssetDecl` per task; `_asset.json` sidecar.

---

# Scope (v0.1.0)

### Deliver

1. **Package `mxm-pipeline/`**

   * `spec.py` – `FlowSpec`, `TaskSpec`, `AssetDecl`
   * `adapters/prefect_adapter.py`
   * `cli.py` – Typer commands: `list`, `graph`, `run`, `status`
   * `tui.py` – (optional) Textual stub: flows list + ASCII DAG
   * `README.md`
2. **Reference Flow** — `justetf` in `mxm-datakraken`

   * Plain callables in `tasks.py`
   * FlowSpec + registration in `flow_justetf.py`
   * Provenance + asset metadata writes
3. **Makefile + CI** – dry-run daily workflow.
4. **Docs** – `RUNBOOK.md`, usage examples.

### Out of Scope (for v0.1.0)

* Central asset catalog DB or lineage browser
* Multi-tenant deployment, RBAC, agents beyond local
* Rich TUI/log streaming
* Asset recomputation planner (reserved for ≥ v0.2.0)

---

# Architecture

```
mxm-datakraken.tasks ─▶ mxm-pipeline.spec (FlowSpec/TaskSpec/AssetDecl)
                             │
                             ├─▶ adapters.prefect_adapter (build/run Prefect flows)
                             ├─▶ CLI/TUI  (list, graph, run, status)
                             └─▶ mxm-dataio (provenance + asset metadata)
```

---

# Spec (interfaces)

```python
@dataclass
class AssetDecl:
    id: str                         # e.g. "justetf.profiles.normalized"
    partition_key: Optional[str]    # e.g. "as_of"
    format: str = "parquet"
    path_template: Optional[str] = None

@dataclass
class TaskSpec:
    name: str
    fn: Callable
    retries: int = 2
    retry_delay_s: int = 30
    params: Dict[str, object] = field(default_factory=dict)
    upstream: List[str] = field(default_factory=list)
    produces: Optional[AssetDecl] = None   # optional

@dataclass
class FlowSpec:
    name: str
    schedule_cron: Optional[str] = None
    params: Dict[str, object] = field(default_factory=dict)
    tasks: List[TaskSpec] = field(default_factory=list)
```

### `mxm-dataio.write_asset_meta()` helper

Writes `_asset.json` beside artifacts:

```python
{
  "asset_id": "justetf.profiles.normalized",
  "partition": "2025-11-11",
  "artifact_path": ".../profiles.parquet",
  "version": "a1b2c3d4e5f6g7h8",
  "created_at": "2025-11-11T06:30:00Z",
  "upstream_assets": [...]
}
```

Prefect adapter ignores this for now—future catalog will ingest it.

---

# justETF Reference Flow

Tasks:

```
get_isin_index → get_subindex_candidates → fetch_profiles
→ normalize_profiles → update_snapshots → validate_schemas
```

Each producer task:

* Writes Parquet + `_meta.json` (provenance)
* Calls `write_asset_meta()` (asset record)
* Declares `produces=AssetDecl(...)` in its `TaskSpec`

---

# Makefile & CI

```makefile
.PHONY: flows graph/justetf run/justetf validate
flows:          ; poetry run mxm-pipeline list
graph/justetf:  ; poetry run mxm-pipeline graph justetf
run/justetf:    ; poetry run mxm-pipeline run justetf --params '{"as_of":"$(AS_OF)"}'
validate:       ; poetry run pytest -q tests/validate_justetf.py
```

CI workflow: daily cron (06:30 CET) → dry-run + validate.

---

# Acceptance Criteria

* CLI commands `list`, `graph`, `run`, `status` work.
* justETF flow completes locally & in CI.
* `_meta.json` + `_asset.json` written for producing tasks.
* Makefile targets and docs present.
* Prefect backend functional; no external DB required.

---

# Milestones

| ID | Deliverable     | Focus                             | Est.  |
| -- | --------------- | --------------------------------- | ----- |
| M1 | Skeleton        | package scaffold + spec + CLI     | 2 h   |
| M2 | Prefect adapter | task→flow build & run             | 1.5 h |
| M3 | justETF flow    | implement tasks + asset decl      | 2 h   |
| M4 | Provenance      | `mxm-dataio` asset meta + _ tests | 1 h   |
| M5 | CI + Docs       | Makefile + workflow + RUNBOOK     | 1 h   |

---

# Risks & Mitigations

| Risk                                    | Mitigation                                    |
| --------------------------------------- | --------------------------------------------- |
| Over-engineering asset layer            | Keep `produces` optional; ignore until needed |
| Format divergence (CLI vs Neovim tools) | Unify `pyproject.toml` formatter config       |
| Network instability                     | Prefect retries + idempotent writes           |
| Spec drift                              | Smoke tests import flow and assert task graph |

---

# Roadmap Beyond v0.1.0

* **v0.2.0** – Enhanced status + TUI (log tail, retry)
* **v0.3.0** – Asset catalog prototype (ingest `_asset.json`)
* **v0.4.0** – Incremental re-materialization planner
* **v0.5.0** – Backend plugin system (Prefect ↔ Dagster adapter)

---

# Ready-to-use Commit Messages

* `pipeline: add spec + prefect adapter + CLI (list/graph/run)`
* `pipeline: add AssetDecl optionality + mxm-dataio asset meta helper`
* `datakraken/justetf: implement flow + asset metadata`
* `ci: add justETF dry-run schedule`
* `docs: add README + RUNBOOK`

---

# Quick Kick-off Checklist

```bash
poetry add prefect==2.* rich==13.* textual==0.62.* networkx==3.*
poetry add -D ruff black isort pyarrow pandera
poetry run mxm-pipeline list
poetry run mxm-pipeline graph justetf
poetry run mxm-pipeline run justetf --params '{"as_of":"2025-11-11"}'
```

---

With this version, `mxm-pipeline v0.1.0` ships a fully usable orchestration layer **and** the DNA for a future asset catalog—zero future re-architecture required.
