from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

import pytest

from mxm.pipeline.adapters import prefect_adapter
from mxm.pipeline.spec import AssetDecl, FlowSpec, TaskSpec


def _no_op() -> int:
    # Used to ensure build does not execute user code
    return 42


def _counting_factory(counter: list[int]) -> Callable[[], int]:
    def _fn() -> int:
        counter.append(1)
        return len(counter)

    return _fn


def test_build_returns_callable_and_does_not_execute_user_code() -> None:
    # A1 — Build returns a callable Prefect flow and does not run task fns
    side_effects: list[int] = []
    t = TaskSpec(name="t1", fn=_counting_factory(side_effects))
    flow_spec = FlowSpec(name="demo-flow", tasks=[t])

    flow_fn = prefect_adapter.build_prefect_flow(flow_spec)

    assert callable(flow_fn)
    # Prefect flow usually carries .name; if not, fall back to __name__
    flow_name = getattr(flow_fn, "name", getattr(flow_fn, "__name__", None))
    assert flow_name == flow_spec.name

    # Ensure building did not run the task function
    assert side_effects == []


def test_build_raises_on_unknown_upstream() -> None:
    # A3 — Reference to an unknown upstream should raise a clear error
    t1 = TaskSpec(name="t1", fn=_no_op, upstream=["missing_task"])
    flow_spec = FlowSpec(name="bad-flow", tasks=[t1])

    with pytest.raises(ValueError) as err:
        _ = prefect_adapter.build_prefect_flow(flow_spec)

    assert "Unknown upstream" in str(err.value)


def test_build_raises_on_cycle() -> None:
    # A4 — Simple 2-node cycle: a -> b, b -> a
    a = TaskSpec(name="a", fn=_no_op, upstream=["b"])
    b = TaskSpec(name="b", fn=_no_op, upstream=["a"])
    flow_spec = FlowSpec(name="cyclic-flow", tasks=[a, b])

    with pytest.raises(ValueError) as err:
        _ = prefect_adapter.build_prefect_flow(flow_spec)

    assert "Cycle" in str(err.value)


def test_build_is_idempotent_and_does_not_mutate_spec() -> None:
    # A5 — Building twice produces two callables and does not mutate spec
    decl = AssetDecl(id="mxm/marketdata/ohlc", partition_key="as_of")
    t = TaskSpec(name="t1", fn=_no_op, produces=decl)
    flow_spec = FlowSpec(name="idempotent-flow", tasks=[t])

    # Keep a pristine copy to compare after builds
    pristine = replace(flow_spec, tasks=[replace(t)])

    flow_fn1 = prefect_adapter.build_prefect_flow(flow_spec)
    flow_fn2 = prefect_adapter.build_prefect_flow(flow_spec)

    assert callable(flow_fn1) and callable(flow_fn2)
    # Spec remains the same (dataclass equality on fields we care about)
    assert flow_spec == pristine


def test_build_raises_on_duplicate_task_names() -> None:
    # Extra guard — duplicate names can cause confusing wiring, fail fast.
    t1 = TaskSpec(name="dup", fn=_no_op)
    t2 = TaskSpec(name="dup", fn=_no_op)
    flow_spec = FlowSpec(name="dups-flow", tasks=[t1, t2])

    with pytest.raises(ValueError) as err:
        _ = prefect_adapter.build_prefect_flow(flow_spec)

    assert "duplicate" in str(err.value).lower()
