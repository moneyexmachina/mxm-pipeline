# mxm-pipeline

![Version](https://img.shields.io/github/v/release/moneyexmachina/mxm-pipeline)
![License](https://img.shields.io/github/license/moneyexmachina/mxm-pipeline)
![Python](https://img.shields.io/badge/python-3.13+-blue)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![CI](https://github.com/moneyexmachina/mxm-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/moneyexmachina/mxm-pipeline/actions/workflows/ci.yml)
**Prefect-backed orchestration • CLI/TUI-first • Provenance everywhere • Optional asset sidecars**

`mxm-pipeline` provides the orchestration core for **Money Ex Machina**.  
It offers a backend-agnostic model and API for defining, compiling, and executing
directed workflows (task graphs) using simple declarative specifications.  
The package is intentionally small, strictly typed, and designed for compositional
use across the MXM ecosystem, with data pipelines as a primary application but
no hard dependency on any specific domain.

The current backend implementation targets **Prefect (local execution)**.  
Future adapters (Airflow, Dagster, temporal.io, Ray) will plug into the same
public API.

## Installation

```bash
pip install mxm-pipeline
```

To enable orchestration backends (currently Prefect):

```bash
pip install "mxm-pipeline[orchestration]"
```

Requires **Python 3.13+**.

## Quick Start

### 1. List available flows

```bash
mxm-pipeline list
```

### 2. Visualise a flow graph

```bash
mxm-pipeline graph demo
```

### 3. Run a flow

```bash
mxm-pipeline run demo --param x=4 --param y=7
```

### 4. Use the API directly

```python
from mxm.pipeline import api
from mxm.pipeline.registry import get_flow

spec = get_flow("demo")
flow = api.compile_flow(spec)
result = api.execute_flow(flow, {"x": 4, "y": 7})

print(result)
```

The API always returns a `dict[str, JSONValue]` mapping task names to results.


## How It Works

`mxm-pipeline` is structured into **three explicit layers**:

### 1. Spec Layer (`spec.py`)

Defines the declarative building blocks of a pipeline:

- `TaskSpec`
- `FlowSpec`
- `AssetDecl`

A flow describes tasks, dependencies, parameters, and assets without reference
to any execution engine.

### 2. API Layer (`api.py`)

The public interface:

- `compile_flow(spec) -> MXMFlow`
- `execute_flow(flow, params) -> dict[str, JSONValue]`

This layer is backend-neutral.  
It hides Prefect (or any future orchestration system) from callers.

### 3. Adapter Layer (`adapters/`)

Each backend implements:

- `build_*_flow(spec)`
- `execute_*_flow()`
- An `MXMFlow` wrapper exposing a uniform interface.

The current implementation is:

```
adapters/prefect_adapter.py
```

It performs deterministic flow construction, retry management, parameter merging,
asset logging, and dependency resolution.

## Registry

Flows are registered via:

```python
from mxm.pipeline.registry import register_flow
```

All registered flows appear automatically in the CLI:

```bash
mxm-pipeline list
```

This allows packages within the MXM ecosystem to expose flows without importing
Prefect or the adapter layer.

## Status and Scope

The package is stable for local execution and suitable for programmatic use in
other MXM components.

Out of scope for `v0.1.0`:

- Distributed or cloud execution
- Asset cataloguing and freshness policies (scheduled for M4)
- Workflow scheduling and event-driven triggers
- UI or dashboard layers

The design emphasises simplicity, transparency, and reliability for development
and research workflows.

## Roadmap

- M2.2 — Backend failsafe, error modes, structured runtime logs
- M3 — Event-driven orchestration, enriched CLI, DAG export formats
- M4 — Full asset layer (catalogue, caching, partitioning, freshness)
- M5 — Multi-backend support (Airflow, Ray, Dagster)
- M6 — Operators / runtime console for the Money Machine

## Development

Format, lint, type-check, and test the project:

```bash
make check
```

All code is required to pass:

- `ruff`
- `black`
- `isort`
- `pyright` (strict)
- `pytest`

## License

MIT License. See [LICENSE](LICENSE).
