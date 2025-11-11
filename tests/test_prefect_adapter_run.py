from __future__ import annotations


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


# --- Tests ------------------------------------------------------------------


def test_run_executes_dependencies_and_returns_results() -> None:
    """
    B1 — Runs tasks respecting dependencies; returns per-task results.
    A -> B -> C with simple integer transformations.
    """
    order: list[str] = []

    tA = TaskSpec(name="A", fn=lambda: _const(1, order, "A"))
    tB = TaskSpec(name="B", fn=lambda a: _add(a, 10, order, "B"), upstream=["A"])
    tC = TaskSpec(name="C", fn=lambda b: _add(b, 100, order, "C"), upstream=["B"])

    spec = FlowSpec(name="chain", tasks=[tA, tB, tC])

    results = prefect_adapter.run_prefect_flow(spec, params={})
    assert results["A"] == 1
    assert results["B"] == 11
    assert results["C"] == 111

    # Strict order A -> B -> C
    assert order == ["A", "B", "C"]


def test_run_passes_runtime_and_task_params_with_precedence() -> None:
    """
    B2/B3 — Params precedence:
      runtime params > flow_spec.params > task_spec.params
    """
    order: list[str] = []

    # Task default k=3 at TaskSpec level; FlowSpec overrides to 5; runtime overrides to 7.
    t = TaskSpec(
        name="T", fn=lambda: 0
    )  # placeholder; we'll inject params via wrapper below

    # Rebuild TaskSpec with a callable that expects k as kwarg and order/name for observability.
    t = TaskSpec(
        name="T",
        fn=lambda *, k: _add_k(0, k=k, order=order, name="T"),
        params={"k": 3},
    )

    spec = FlowSpec(name="params-flow", params={"k": 5}, tasks=[t])

    results = prefect_adapter.run_prefect_flow(spec, params={"k": 7})
    assert results["T"] == 7
    assert order == ["T"]


def test_run_retries_and_succeeds_within_limits() -> None:
    """
    B4 — A task that fails once should succeed with retries=2.
    """
    order: list[str] = []
    attempts: dict[str, int] = {}

    t = TaskSpec(
        name="F",
        fn=lambda: _flaky(attempts, max_failures=1, order=order, name="F"),
        retries=2,
        retry_delay_s=0,
    )
    spec = FlowSpec(name="retry-flow", tasks=[t])

    results = prefect_adapter.run_prefect_flow(spec, params={})
    # Should have taken 2 attempts and succeeded, returning attempt count.
    assert results["F"] == 2
    # Multiple entries for the same task in execution order due to retries
    assert order.count("F") >= 2


def test_run_fails_when_retries_exceeded() -> None:
    """
    B4 counter-case — Fails after exhausting retries.
    """
    order: list[str] = []
    attempts: dict[str, int] = {}

    t = TaskSpec(
        name="F",
        fn=lambda: _flaky(attempts, max_failures=3, order=order, name="F"),
        retries=2,
        retry_delay_s=0,
    )
    spec = FlowSpec(name="retry-fail", tasks=[t])

    with pytest.raises(Exception):
        _ = prefect_adapter.run_prefect_flow(spec, params={})

    # Should have tried multiple times
    assert order.count("F") >= 2


def test_run_parallel_branches_converge_topologically() -> None:
    """
    B6 — A -> {B, C} -> D; ensure topological order and correct result.
    """
    order: list[str] = []

    tA = TaskSpec(name="A", fn=lambda: _const(2, order, "A"))
    tB = TaskSpec(name="B", fn=lambda a: _add(a, 3, order, "B"), upstream=["A"])
    tC = TaskSpec(name="C", fn=lambda a: _add(a, 5, order, "C"), upstream=["A"])
    tD = TaskSpec(
        name="D",
        fn=lambda b, c: _add(b, c, order, "D"),
        upstream=["B", "C"],
    )

    spec = FlowSpec(name="fan-in", tasks=[tA, tB, tC, tD])

    results = prefect_adapter.run_prefect_flow(spec, params={})
    assert results["A"] == 2
    assert results["B"] == 5
    assert results["C"] == 7
    assert results["D"] == 12

    # Topological constraints:
    ia = order.index("A")
    ib = order.index("B")
    ic = order.index("C")
    id_ = order.index("D")
    assert ia < ib and ia < ic
    assert ib < id_ and ic < id_
    # B and C relative order not asserted (they may run in any order)


def test_run_emits_asset_log_line(caplog: pytest.LogCaptureFixture) -> None:
    """
    B7 — When TaskSpec.produces is set, adapter emits a structured log line.
    """
    order: list[str] = []
    asset = AssetDecl(id="mxm/marketdata/ohlc", partition_key="as_of")

    t = TaskSpec(
        name="W",
        fn=lambda: _const(1, order, "W"),
        produces=asset,
    )
    spec = FlowSpec(name="assets", tasks=[t])

    with caplog.at_level("INFO"):
        results = prefect_adapter.run_prefect_flow(spec, params={"as_of": "2025-11-11"})

    assert results["W"] == 1

    # Expect a structured log entry containing the asset id and partition key/value
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "ASSET write" in messages
    assert "mxm/marketdata/ohlc" in messages
    assert "partition=as_of:2025-11-11" in messages


def test_run_raises_helpful_error_when_prefect_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    B8 — Simulate Prefect not installed by patching the import guard.
    """
    monkeypatch.setattr(
        prefect_adapter,
        "_require_prefect_flow",
        lambda: (_ for _ in ()).throw(RuntimeError("Prefect missing")),
        raising=True,
    )
    spec = FlowSpec(name="x", tasks=[])
    with pytest.raises(RuntimeError):
        _ = prefect_adapter.run_prefect_flow(spec, params={})
