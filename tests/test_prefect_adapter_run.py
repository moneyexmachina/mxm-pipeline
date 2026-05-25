"""
End-to-end execution tests for MXM flows using the Prefect adapter.

These tests go through:
    FlowSpec -> build_mxm_flow_for_prefect -> MXMFlow.execute
and verify:
    - dependency wiring
    - parameter precedence
    - retries
    - fan-in ordering
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from mxm.pipeline.adapters import prefect_adapter
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.spec import FlowSpec, TaskSpec

# --- Helpers ----------------------------------------------------------------


def _layout(tmp_path: Path) -> ReportingLayout:
    return ReportingLayout(root=tmp_path)


def _append_order(order: list[str], name: str) -> None:
    order.append(name)


def _const(val: int, order: list[str], name: str) -> int:
    _append_order(order, name)
    return val


def _add(x: int, y: int, order: list[str], name: str) -> int:
    _append_order(order, name)
    return x + y


def _add_k(x: int, *, k: int, order: list[str], name: str) -> int:
    _append_order(order, name)
    return x + k


def _flaky(
    counter: dict[str, int], max_failures: int, order: list[str], name: str
) -> int:
    """
    Fail the first `max_failures` times, then return the number of attempts.
    """
    _append_order(order, name)
    cnt = counter.get(name, 0) + 1
    counter[name] = cnt
    if cnt <= max_failures:
        raise RuntimeError(f"intentional failure attempt={cnt}")
    return cnt


# --- typed callable factories to avoid untyped lambdas -----------------------


def make_const(order: list[str], name: str, val: int) -> Callable[[], int]:
    def fn() -> int:
        return _const(val, order, name)

    return fn


def make_add1(order: list[str], name: str, inc: int) -> Callable[[int], int]:
    def fn(a: int) -> int:
        return _add(a, inc, order, name)

    return fn


def make_add2(order: list[str], name: str) -> Callable[[int, int], int]:
    def fn(b: int, c: int) -> int:
        return _add(b, c, order, name)

    return fn


def make_flaky0(
    attempts: dict[str, int], max_failures: int, order: list[str], name: str
) -> Callable[[], int]:
    def fn() -> int:
        return _flaky(attempts, max_failures, order, name)

    return fn


def make_add_k0(order: list[str], name: str) -> Callable[..., int]:
    def fn(*, k: int) -> int:
        return _add_k(0, k=k, order=order, name=name)

    return fn


# --- Tests ------------------------------------------------------------------


def test_run_executes_dependencies_and_returns_results(tmp_path: Path) -> None:
    order: list[str] = []
    tA = TaskSpec(name="A", fn=make_const(order, "A", 1))
    tB = TaskSpec(name="B", fn=make_add1(order, "B", 10), upstream=["A"])
    tC = TaskSpec(name="C", fn=make_add1(order, "C", 100), upstream=["B"])
    spec = FlowSpec(name="chain", tasks=[tA, tB, tC])

    results = prefect_adapter.run_mxm_flow_for_prefect(
        spec,
        params={},
        reporting_layout=_layout(tmp_path),
    )

    assert results["A"] == 1
    assert results["B"] == 11
    assert results["C"] == 111
    assert order == ["A", "B", "C"]


def test_run_passes_runtime_and_task_params_with_precedence(
    tmp_path: Path,
) -> None:
    order: list[str] = []
    t = TaskSpec(name="T", fn=make_add_k0(order, "T"), params={"k": 3})
    spec = FlowSpec(name="params-flow", params={"k": 5}, tasks=[t])

    results = prefect_adapter.run_mxm_flow_for_prefect(
        spec,
        params={"k": 7},
        reporting_layout=_layout(tmp_path),
    )

    assert results["T"] == 7
    assert order == ["T"]


def test_run_retries_and_succeeds_within_limits(tmp_path: Path) -> None:
    order: list[str] = []
    attempts: dict[str, int] = {}
    t = TaskSpec(
        name="F",
        fn=make_flaky0(attempts, max_failures=1, order=order, name="F"),
        retries=2,
        retry_delay_s=0,
    )
    spec = FlowSpec(name="retry-flow", tasks=[t])

    results = prefect_adapter.run_mxm_flow_for_prefect(
        spec,
        params={},
        reporting_layout=_layout(tmp_path),
    )

    assert results["F"] == 2
    assert order.count("F") >= 2


def test_run_fails_when_retries_exceeded(tmp_path: Path) -> None:
    order: list[str] = []
    attempts: dict[str, int] = {}
    t = TaskSpec(
        name="F",
        fn=make_flaky0(attempts, max_failures=3, order=order, name="F"),
        retries=2,
        retry_delay_s=0,
    )
    spec = FlowSpec(name="retry-fail", tasks=[t])

    with pytest.raises(RuntimeError):
        _ = prefect_adapter.run_mxm_flow_for_prefect(
            spec,
            params={},
            reporting_layout=_layout(tmp_path),
        )

    assert attempts["F"] >= 3


def test_run_parallel_branches_converge_topologically(tmp_path: Path) -> None:
    order: list[str] = []
    tA = TaskSpec(name="A", fn=make_const(order, "A", 2))
    tB = TaskSpec(name="B", fn=make_add1(order, "B", 3), upstream=["A"])
    tC = TaskSpec(name="C", fn=make_add1(order, "C", 5), upstream=["A"])
    tD = TaskSpec(name="D", fn=make_add2(order, "D"), upstream=["B", "C"])
    spec = FlowSpec(name="fan-in", tasks=[tA, tB, tC, tD])

    results = prefect_adapter.run_mxm_flow_for_prefect(
        spec,
        params={},
        reporting_layout=_layout(tmp_path),
    )

    assert results["A"] == 2
    assert results["B"] == 5
    assert results["C"] == 7
    assert results["D"] == 12

    ia = order.index("A")
    ib = order.index("B")
    ic = order.index("C")
    id_ = order.index("D")
    assert ia < ib and ia < ic and ib < id_ and ic < id_
