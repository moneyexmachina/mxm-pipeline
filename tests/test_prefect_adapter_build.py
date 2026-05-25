from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from mxm.pipeline.adapters import prefect_adapter
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.spec import FlowSpec, TaskSpec
from mxm.pipeline.types import MXMFlow
from mxm.types import JSONObj


def _no_op() -> int:
    # Used to ensure build does not execute user code
    return 42


def _counting_factory(counter: list[int]) -> Callable[[], int]:
    def _fn() -> int:
        counter.append(1)
        return len(counter)

    return _fn


def _layout(tmp_path: Path) -> ReportingLayout:
    return ReportingLayout(root=tmp_path)


def test_build_mxm_flow_for_prefect_returns_mxmflow_and_is_pure(
    tmp_path: Path,
) -> None:
    counter = {"n": 0}

    def _side_effect_task(params: JSONObj) -> int:
        counter["n"] += 1
        return 1

    t = TaskSpec(name="t1", fn=_side_effect_task)
    flow_spec = FlowSpec(name="demo-mxm", tasks=[t])

    flow = prefect_adapter.build_mxm_flow_for_prefect(
        flow_spec,
        reporting_layout=_layout(tmp_path),
    )

    assert isinstance(flow, MXMFlow)
    assert flow.name == flow_spec.name
    assert flow.backend == "prefect"
    assert counter["n"] == 0


def test_build_returns_callable_and_does_not_execute_user_code(
    tmp_path: Path,
) -> None:
    side_effects: list[int] = []
    t = TaskSpec(name="t1", fn=_counting_factory(side_effects))
    flow_spec = FlowSpec(name="demo-flow", tasks=[t])

    flow_fn = prefect_adapter.build_prefect_flow(
        flow_spec,
        reporting_layout=_layout(tmp_path),
    )

    assert callable(flow_fn)
    flow_name = getattr(flow_fn, "name", getattr(flow_fn, "__name__", None))
    assert flow_name == flow_spec.name
    assert side_effects == []


def test_build_raises_on_unknown_upstream(tmp_path: Path) -> None:
    t1 = TaskSpec(name="t1", fn=_no_op, upstream=["missing_task"])
    flow_spec = FlowSpec(name="bad-flow", tasks=[t1])

    with pytest.raises(ValueError) as err:
        _ = prefect_adapter.build_prefect_flow(
            flow_spec,
            reporting_layout=_layout(tmp_path),
        )

    assert "Unknown upstream" in str(err.value)


def test_build_raises_on_cycle(tmp_path: Path) -> None:
    a = TaskSpec(name="a", fn=_no_op, upstream=["b"])
    b = TaskSpec(name="b", fn=_no_op, upstream=["a"])
    flow_spec = FlowSpec(name="cyclic-flow", tasks=[a, b])

    with pytest.raises(ValueError) as err:
        _ = prefect_adapter.build_prefect_flow(
            flow_spec,
            reporting_layout=_layout(tmp_path),
        )

    assert "Cycle" in str(err.value)


def test_build_is_idempotent_and_does_not_mutate_spec(tmp_path: Path) -> None:
    t = TaskSpec(name="t1", fn=_no_op)
    flow_spec = FlowSpec(name="idempotent-flow", tasks=[t])

    pristine = replace(flow_spec, tasks=[replace(t)])

    flow_fn1 = prefect_adapter.build_prefect_flow(
        flow_spec,
        reporting_layout=_layout(tmp_path),
    )
    flow_fn2 = prefect_adapter.build_prefect_flow(
        flow_spec,
        reporting_layout=_layout(tmp_path),
    )

    assert callable(flow_fn1)
    assert callable(flow_fn2)
    assert flow_spec == pristine


def test_build_raises_on_duplicate_task_names(tmp_path: Path) -> None:
    t1 = TaskSpec(name="dup", fn=_no_op)
    t2 = TaskSpec(name="dup", fn=_no_op)
    flow_spec = FlowSpec(name="dups-flow", tasks=[t1, t2])

    with pytest.raises(ValueError) as err:
        _ = prefect_adapter.build_prefect_flow(
            flow_spec,
            reporting_layout=_layout(tmp_path),
        )

    assert "duplicate" in str(err.value).lower()
