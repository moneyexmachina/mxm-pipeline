# RUNBOOK — mxm-pipeline (v0.1.0)

This document is for developers and maintainers working with `mxm-pipeline`
inside the **Money Ex Machina** ecosystem.  
It explains how to define, register, run, debug, and extend workflows using the
`FlowSpec`/`TaskSpec` abstraction and the Prefect backend.

# 1. Purpose

`mxm-pipeline` provides the **orchestration core** for MXM.  
It defines a backend-agnostic model for workflows, plus a stable API for:

- compiling a `FlowSpec` into a backend-specific executable (`MXMFlow`)
- executing workflows programmatically or via the CLI
- registering flows for discovery

The current backend implementation targets **Prefect (local-only)**.

This runbook describes how to work with this system in development, testing,
and CI environments.

# 2. Project layout

Only the modules relevant to daily development are listed here.

```
mxm/pipeline/
    spec.py            # FlowSpec, TaskSpec, AssetDecl
    api.py             # compile_flow(), execute_flow(), MXMFlow protocol
    registry.py        # flow registration and discovery
    adapters/
        prefect_adapter.py   # Prefect backend implementation
    cli/
        main.py        # mxm-pipeline commands
```

# 3. Adding a new flow

## 3.1 Define task functions

Task functions must be ordinary Python callables.  
Arguments and return types must be JSON-serialisable or convertible to `JSONValue`.

```python
def load_data() -> list[int]:
    ...

def transform(xs: list[int]) -> list[int]:
    ...

def aggregate(xs: list[int]) -> int:
    ...
```

## 3.2 Create a FlowSpec

```python
from mxm.pipeline.spec import TaskSpec, FlowSpec

spec = FlowSpec(
    name="example",
    tasks=[
        TaskSpec("load", load_data),
        TaskSpec("transform", transform, upstream=["load"]),
        TaskSpec("aggregate", aggregate, upstream=["transform"]),
    ],
)
```

Upstream dependencies must be explicit.  
Cycles and unknown references are validated during compilation.

## 3.3 Register the flow

Add in your module:

```python
from mxm.pipeline.registry import register_flow
register_flow(spec)
```

Registered flows appear automatically in the CLI (`mxm-pipeline list`) and can
be retrieved via:

```python
from mxm.pipeline.registry import get_flow
spec = get_flow("example")
```

# 4. Running flows

## 4.1 Using the CLI

### List flows
```bash
mxm-pipeline list
```

### Show dependency graph
```bash
mxm-pipeline graph example
```

### Execute
```bash
mxm-pipeline run example --param limit=100 --param mode="fast"
```

The CLI uses:
- `compile_flow(spec)`
- `execute_flow(flow, params)`

Flow results are printed in JSON form.

## 4.2 Using the Python API

```python
from mxm.pipeline.api import compile_flow, execute_flow
from mxm.pipeline.registry import get_flow

spec = get_flow("example")
flow = compile_flow(spec)
result = execute_flow(flow, {"limit": 100})
```

The API returns a `dict[str, JSONValue]` mapping task names to results.

## 4.3 Parameter precedence

Parameters may be defined:
- in the `FlowSpec`
- in each `TaskSpec`
- at runtime (CLI or API)

The merge order is:

```
runtime params  >  flow_spec.params  >  task_spec.params
```

Unexpected keys are ignored according to the function signature.

# 5. Backend requirements (Prefect adapter)

The Prefect backend is enabled by:

```bash
pip install "mxm-pipeline[prefect]"
```

or via Poetry:

```bash
poetry install -E prefect
```

### Execution model

- Runs are **local** and **synchronous**.
- No Prefect server or agent is required.
- The adapter uses Prefect tasks under the hood but hides all Prefect types.

### Retries

`TaskSpec(retries=N, retry_delay_s=M)` is honoured by the adapter via Prefect’s
retry mechanism.

### Asset logs (early design)

If a task produces assets via `TaskSpec(..., produces=<AssetDecl>)`, the adapter
emits:

```
ASSET write <id> [partition=k:v]
```

Asset cataloguing and freshness policies will arrive in M4.

# 6. Debugging workflows

## 6.1 Flow construction errors

Compilation validates:

- duplicate task names
- unknown upstream dependencies
- cycles
- incompatible function signatures

Typical resolution: fix the `FlowSpec`.

## 6.2 Runtime errors

A failing task surfaces as a raised exception from `execute_flow()`.  
The result dictionary may be partially complete.

To debug:

- inspect the printed error
- run `mxm-pipeline graph <flow>` to confirm topology
- add logging to your task functions
- temporarily enable Prefect logging (see adapter notes)

## 6.3 Inspecting the built backend flow

```python
from mxm.pipeline.adapters.prefect_adapter import build_prefect_flow
pf_flow = build_prefect_flow(spec)
print(pf_flow)
```

This is useful when diagnosing Prefect-level errors.

# 7. Development workflow

## 7.1 Linting, typing, tests

Run all checks:

```bash
make check
```

Or individually:

```
make lint     # ruff
make type     # pyright (strict)
make test     # pytest
```

CI runs the same checks via GitHub Actions.

## 7.2 Adding a new backend (outline only)

A backend implementation must supply:

- `build_*_flow(spec)` → compiled backend object
- `execute_*_flow(flow, params)` → result dictionary
- A wrapper implementing `MXMFlow`

See `prefect_adapter.py` as a reference.

# 8. Release process

1. Update `CHANGELOG.md`
2. Bump version in `pyproject.toml`
3. Commit + push + merge PR
4. Tag release:
   ```bash
   git tag v0.1.0
   git push --tags
   ```
5. Create GitHub release
6. Publish to PyPI (Trusted Publishing)

# 9. Known limitations (v0.1.0)

- Local execution only; no scheduling or distributed execution.
- No asset caching or freshness policies yet.
- No retry semantics beyond Prefect’s built-in features.
- The CLI does not yet support multiple backends (`--backend`).
- Adapter currently assumes deterministic, side-effect-free tasks.
