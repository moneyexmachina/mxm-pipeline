from __future__ import annotations

import pytest

from mxm.pipeline.api import compile_flow, execute_flow
from mxm.pipeline.demos.prefect_demo import build_demo_flow
from mxm.pipeline.spec import FlowSpec, TaskSpec
from mxm.pipeline.types import BackendName, MXMFlow, RunOptions
from mxm.types import JSONMap, JSONObj


def _dummy_task() -> int:
    return 1


def _make_simple_flowspec() -> FlowSpec:
    t = TaskSpec(name="A", fn=_dummy_task)
    return FlowSpec(name="api-demo", tasks=[t])


def test_compile_flow_prefect_returns_mxmflow() -> None:
    spec = _make_simple_flowspec()

    flow = compile_flow(spec, backend="prefect")

    # Protocol check (runtime_checkable)
    assert isinstance(flow, MXMFlow)
    assert flow.name == spec.name
    assert flow.backend == "prefect"


def test_compile_flow_unsupported_backend_raises() -> None:
    spec = _make_simple_flowspec()

    with pytest.raises(ValueError) as err:
        _ = compile_flow(spec, backend="unknown")  # type: ignore[arg-type]

    msg = str(err.value)
    assert "Unsupported backend" in msg or "unsupported backend" in msg.lower()


class FakeFlow:
    backend: BackendName = "prefect"

    def __init__(self) -> None:
        self.name = "fake"
        self.called_with: dict[str, object] = {}

    def execute(
        self,
        params: JSONObj | None = None,
        options: RunOptions | None = None,
    ) -> JSONMap:
        self.called_with["params"] = params
        self.called_with["options"] = options
        return {"ok": True}


def test_execute_flow_normalises_none_and_forwards() -> None:
    flow = FakeFlow()

    # None → {} normalisation
    result1 = execute_flow(flow, params=None, options=None)
    assert result1 == {"ok": True}
    assert flow.called_with["params"] == {}
    assert flow.called_with["options"] == {}

    # Non-None passthrough
    result2 = execute_flow(flow, params={"x": 1}, options={"quiet": True})
    assert result2 == {"ok": True}
    assert flow.called_with["params"] == {"x": 1}
    assert flow.called_with["options"] == {"quiet": True}


def test_compile_and_execute_demo_flow_smoke() -> None:
    spec = build_demo_flow()

    flow = compile_flow(spec, backend="prefect")
    result = execute_flow(flow, params={"as_of": "2025-11-11"})

    assert set(result.keys()) == {"A", "B", "C", "D"}
    assert result["A"] == 2
    assert result["B"] == 5
    assert result["C"] == 7
    assert result["D"] == 12
