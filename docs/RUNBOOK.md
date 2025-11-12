# Running flows with the Prefect adapter (M2)

## What this gives you
- A backend-agnostic spec (`FlowSpec`, `TaskSpec`) that can be executed via Prefect 2.x.
- Local, synchronous runs suitable for development and CI.

## Install

```
poetry install -E orchestration
# or: poetry add --optional 'prefect>=2.16' 'networkx>=3.2'
```

## Minimal example

```python
from mxm.pipeline.spec import FlowSpec, TaskSpec
from mxm.pipeline.adapters import prefect_adapter

def a() -> int: return 2
def b(x: int) -> int: return x + 3
def c(x: int) -> int: return x + 5
def d(bv: int, cv: int) -> int: return bv + cv

spec = FlowSpec(
    name="demo",
    tasks=[
        TaskSpec("A", fn=a),
        TaskSpec("B", fn=b, upstream=["A"]),
        TaskSpec("C", fn=c, upstream=["A"]),
        TaskSpec("D", fn=d, upstream=["B", "C"]),
    ],
)

results = prefect_adapter.run_prefect_flow(spec, params={})
# {'A': 2, 'B': 5, 'C': 7, 'D': 12}
```

## Param precedence
If the same key appears in multiple places, the adapter applies:

```
runtime params  >  flow_spec.params  >  task_spec.params
```

Unexpected keys are ignored per function signature.

## Retries
`TaskSpec.retries` and `retry_delay_s` are honored via Prefect’s `@task` wrapper.

## Assets (early)
If `TaskSpec.produces` is set, the adapter emits an INFO log:

```
ASSET write <asset_id> [partition=<key>:<value>]
```

No catalog/freshness or caching yet.

## Behavior guarantees (validated at build)
- Duplicate task names → error
- Unknown upstream references → error
- Cycles → error

## Test / CI notes
- Tests mute Prefect’s Rich console shutdown log to avoid writing to closed streams.
- Commands:

```
make lint        # ruff
make type        # pyright
make test        # pytest (quiet)
```
