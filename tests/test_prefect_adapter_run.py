"""
End-to-end execution tests for MXM flows using the Prefect adapter.

These tests go through:
    FlowSpec -> build_mxm_flow_for_prefect -> MXMFlow.execute
and verify:
    - dependency wiring
    - parameter precedence
    - retries
    - fan-in ordering
    - asset logging
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

from mxm.pipeline.adapters import prefect_adapter
from mxm.pipeline.spec import AssetDecl, FlowSpec, TaskSpec

# --- Helpers ----------------------------------------------------------------


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


# --- typed callable factories to avoid untyped lambdas ------------------


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


# If pyright flags the short form above, expand explicit kwargs:
def make_add_k0(order: list[str], name: str) -> Callable[..., int]:
    def fn(*, k: int) -> int:
        return _add_k(0, k=k, order=order, name=name)

    return fn


# --- Tests ------------------------------------------------------------------


def test_run_executes_dependencies_and_returns_results() -> None:
    order: list[str] = []
    tA = TaskSpec(name="A", fn=make_const(order, "A", 1))
    tB = TaskSpec(name="B", fn=make_add1(order, "B", 10), upstream=["A"])
    tC = TaskSpec(name="C", fn=make_add1(order, "C", 100), upstream=["B"])
    spec = FlowSpec(name="chain", tasks=[tA, tB, tC])
    results = prefect_adapter.run_mxm_flow_for_prefect(spec, params={})
    assert results["A"] == 1
    assert results["B"] == 11
    assert results["C"] == 111
    assert order == ["A", "B", "C"]


def test_run_passes_runtime_and_task_params_with_precedence() -> None:
    order: list[str] = []
    t = TaskSpec(name="T", fn=make_add_k0(order, "T"), params={"k": 3})
    spec = FlowSpec(name="params-flow", params={"k": 5}, tasks=[t])
    results = prefect_adapter.run_mxm_flow_for_prefect(spec, params={"k": 7})
    assert results["T"] == 7
    assert order == ["T"]


def test_run_retries_and_succeeds_within_limits() -> None:
    order: list[str] = []
    attempts: dict[str, int] = {}
    t = TaskSpec(
        name="F",
        fn=make_flaky0(attempts, max_failures=1, order=order, name="F"),
        retries=2,
        retry_delay_s=0,
    )
    spec = FlowSpec(name="retry-flow", tasks=[t])
    results = prefect_adapter.run_mxm_flow_for_prefect(spec, params={})
    assert results["F"] == 2
    assert order.count("F") >= 2


def test_run_fails_when_retries_exceeded() -> None:
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
        _ = prefect_adapter.run_mxm_flow_for_prefect(spec, params={})
    assert attempts["F"] >= 3


def test_run_parallel_branches_converge_topologically() -> None:
    order: list[str] = []
    tA = TaskSpec(name="A", fn=make_const(order, "A", 2))
    tB = TaskSpec(name="B", fn=make_add1(order, "B", 3), upstream=["A"])
    tC = TaskSpec(name="C", fn=make_add1(order, "C", 5), upstream=["A"])
    tD = TaskSpec(name="D", fn=make_add2(order, "D"), upstream=["B", "C"])
    spec = FlowSpec(name="fan-in", tasks=[tA, tB, tC, tD])
    results = prefect_adapter.run_mxm_flow_for_prefect(spec, params={})
    assert results["A"] == 2
    assert results["B"] == 5
    assert results["C"] == 7
    assert results["D"] == 12
    ia = order.index("A")
    ib = order.index("B")
    ic = order.index("C")
    id_ = order.index("D")
    assert ia < ib and ia < ic and ib < id_ and ic < id_


def test_run_emits_asset_log_line(caplog: pytest.LogCaptureFixture) -> None:
    order: list[str] = []
    asset = AssetDecl(id="mxm/marketdata/ohlc", partition_key="as_of")
    t = TaskSpec(name="W", fn=make_const(order, "W", 1), produces=asset)
    spec = FlowSpec(name="assets", tasks=[t])
    with caplog.at_level("INFO"):
        results = prefect_adapter.run_mxm_flow_for_prefect(
            spec, params={"as_of": "2025-11-11"}
        )
    assert results["W"] == 1
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "ASSET write" in messages
    assert "mxm/marketdata/ohlc" in messages
    assert "partition=as_of:2025-11-11" in messages
