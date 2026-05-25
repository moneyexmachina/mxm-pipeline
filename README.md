# mxm-pipeline

![Version](https://img.shields.io/github/v/release/moneyexmachina/mxm-pipeline)
![License](https://img.shields.io/github/license/moneyexmachina/mxm-pipeline)
![Python](https://img.shields.io/badge/python-3.13+-blue)
[![Checked with pyright](https://microsoft.github.io/pyright/img/pyright_badge.svg)](https://microsoft.github.io/pyright/)
[![CI](https://github.com/moneyexmachina/mxm-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/moneyexmachina/mxm-pipeline/actions/workflows/ci.yml)

Declarative Prefect orchestration substrate with semantic execution events for Money Ex Machina.

`mxm-pipeline` provides a small orchestration layer for defining workflows as declarative task graphs and compiling them into executable Prefect flows.

The package intentionally does **not** implement its own orchestration engine.

Instead:

- Prefect owns operational execution truth
- `mxm-pipeline` owns semantic execution meaning

The package provides:

- declarative flow specifications (`FlowSpec`, `TaskSpec`)
- deterministic Prefect flow compilation
- runtime execution-context injection
- semantic event emission and persistence
- CLI utilities for local execution and inspection

The current implementation targets local Prefect execution.

## Purpose

`mxm-pipeline` exists to provide a stable orchestration substrate for MXM systems while keeping orchestration semantics intentionally small and explicit.

The package separates:

- operational execution state
- semantic/domain meaning
- logs and execution traces

Operational orchestration concerns are delegated to Prefect:

- flow runs
- task runs
- retries
- scheduling
- orchestration lifecycle

`mxm-pipeline` augments execution with:

- semantic event emission
- execution context injection
- deterministic flow compilation
- lightweight runtime helpers

The design goal is compositional, inspectable workflows without building a parallel orchestration framework.

## Installation

```bash
pip install mxm-pipeline
```

To enable orchestration support:

```bash
pip install "mxm-pipeline[orchestration]"
```

Requires **Python 3.13+**.

## Usage

### Define tasks

```python
from mxm.pipeline.spec import FlowSpec, TaskSpec


def produce_number(x: int) -> int:
    return x * 2


def consume_number(
    value: int,
    *,
    execution_context,
) -> int:
    execution_context.emit_semantic_event(
        event_type="value_consumed",
        domain_key="demo.value",
        payload={"value": value},
    )

    return value + 1
```

### Define a flow

```python
flow = FlowSpec(
    name="demo",
    tasks=[
        TaskSpec(
            name="produce",
            fn=produce_number,
            params={"x": 4},
        ),
        TaskSpec(
            name="consume",
            fn=consume_number,
            upstream=["produce"],
        ),
    ],
)
```

### Compile and execute

```python
from mxm.pipeline.api import compile_flow, execute_flow
from mxm.pipeline.reporting.layout import ReportingLayout

layout = ReportingLayout.from_root("./runtime")

compiled = compile_flow(
    flow,
    reporting_layout=layout,
)

result = execute_flow(compiled)

print(result)
```

### CLI usage

List flows:

```bash
mxm-pipeline list
```

Show dependency graph:

```bash
mxm-pipeline graph demo
```

Run a flow:

```bash
mxm-pipeline run demo --param x=4
```

## Runtime Model

The runtime model is intentionally small:

```text
FlowSpec
  ↓
Prefect FlowRun
  ↓
Prefect TaskRun
  ↓
MXM ExecutionContext
  ↓
SemanticEvent(s)
```

`ExecutionContext` is injected into tasks that accept an `execution_context` argument.

The context provides:

- Prefect runtime identifiers
- logging access
- semantic event emission
- lightweight execution metadata

The context does not own orchestration state.

## Semantic Events

Semantic events are append-only domain records emitted during execution.

Examples:

- dataset materialized
- partial coverage obtained
- degraded upstream state
- stale reference data detected
- validation failure
- no-op execution

Principle:

> operational records say what happened  
> semantic events say what it meant  
> logs say how it unfolded

Semantic events are persisted independently from Prefect operational state.

## Architecture

`mxm-pipeline` is structured into four primary layers.

### 1. Specification Layer

Declarative workflow definitions:

- `FlowSpec`
- `TaskSpec`

Tasks define executable units and dependencies without embedding orchestration logic.

### 2. API Layer

Public orchestration interface:

- `compile_flow()`
- `execute_flow()`

This layer hides backend implementation details from callers.

### 3. Adapter Layer

Backend-specific compilation and execution.

Current implementation:

```text
adapters/prefect_adapter.py
```

The adapter:

- validates task graphs
- compiles Prefect flows/tasks
- injects execution contexts
- manages semantic event sinks
- executes flows under MXM runtime defaults

### 4. Reporting Layer

Semantic event persistence and reporting utilities.

Current implementation includes:

- semantic event models
- append-only semantic event storage
- SQLite-backed persistence
- reporting layouts and sinks

The reporting layer intentionally does not duplicate Prefect orchestration state.

## Design Principles

- **Prefect owns operational truth**  
  MXM does not reimplement orchestration state management.

- **Semantic meaning is explicit**  
  Domain outcomes are emitted as structured semantic events.

- **Declarative flow construction**  
  Workflows are defined as task graphs rather than imperative orchestration code.

- **Minimal runtime abstraction**  
  The package avoids building a general-purpose orchestration framework.

- **Strict typing**  
  Fully Pyright-clean and PEP 561 compliant.

- **Deterministic execution structure**  
  Task ordering and graph validation are stable and explicit.

## Development

```bash
poetry install

make check
```

All code is required to pass:

- `ruff`
- `black`
- `isort`
- `pyright` (strict)
- `pytest`

## ADRs

Architecture decisions are documented under:

```text
docs/adr/
```

Current ADR sequence:

- ADR 0001 — Optional asset layer from day 1
- ADR 0002 — Task-centric execution model with semantic events
- ADR 0003 — Prefect owns operational truth

## License

MIT License. See [LICENSE](LICENSE).

