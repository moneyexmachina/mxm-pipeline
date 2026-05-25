from __future__ import annotations

from pathlib import Path

import pytest

from mxm.pipeline.api import compile_flow, execute_flow
from mxm.pipeline.demos.prefect_demo import build_demo_flow
from mxm.pipeline.execution.context import ExecutionContext
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import SemanticEventsStore
from mxm.pipeline.spec import FlowSpec, TaskSpec
from mxm.pipeline.types import BackendName, MXMFlow, RunOptions
from mxm.types import JSONMap, JSONObj


def _dummy_task() -> int:
    return 1


def _make_simple_flowspec() -> FlowSpec:
    t = TaskSpec(name="A", fn=_dummy_task)
    return FlowSpec(name="api-demo", tasks=[t])


def _layout(tmp_path: Path) -> ReportingLayout:
    return ReportingLayout(root=tmp_path)


def test_compile_flow_prefect_returns_mxmflow(tmp_path: Path) -> None:
    spec = _make_simple_flowspec()

    flow = compile_flow(
        spec,
        backend="prefect",
        reporting_layout=_layout(tmp_path),
    )

    assert isinstance(flow, MXMFlow)
    assert flow.name == spec.name
    assert flow.backend == "prefect"


def test_compile_flow_unsupported_backend_raises(tmp_path: Path) -> None:
    spec = _make_simple_flowspec()

    with pytest.raises(ValueError) as err:
        _ = compile_flow(
            spec,
            backend="unknown",  # type: ignore[arg-type]
            reporting_layout=_layout(tmp_path),
        )

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

    result1 = execute_flow(flow, params=None, options=None)
    assert result1 == {"ok": True}
    assert flow.called_with["params"] == {}
    assert flow.called_with["options"] == {}

    result2 = execute_flow(flow, params={"x": 1}, options={"quiet": True})
    assert result2 == {"ok": True}
    assert flow.called_with["params"] == {"x": 1}
    assert flow.called_with["options"] == {"quiet": True}


def test_compile_and_execute_demo_flow_smoke(tmp_path: Path) -> None:
    spec = build_demo_flow()

    flow = compile_flow(
        spec,
        backend="prefect",
        reporting_layout=_layout(tmp_path),
    )
    result = execute_flow(flow, params={"as_of": "2025-11-11"})

    assert set(result.keys()) == {"A", "B", "C", "D"}
    assert result["A"] == 2
    assert result["B"] == 5
    assert result["C"] == 7
    assert result["D"] == 12


def test_compile_and_execute_flow_persists_semantic_event(tmp_path: Path) -> None:
    layout = _layout(tmp_path)

    def _task_with_semantic_event(
        execution_context: ExecutionContext | None = None,
    ) -> int:
        assert execution_context is not None
        execution_context.emit_semantic_event(
            event_type="materialized",
            domain_key="demo.asset",
            payload={"rows": 123},
        )
        return 7

    spec = FlowSpec(
        name="semantic-api-flow",
        tasks=[TaskSpec(name="A", fn=_task_with_semantic_event)],
    )

    flow = compile_flow(
        spec,
        backend="prefect",
        reporting_layout=layout,
    )
    result = execute_flow(flow)

    assert result == {"A": 7}

    backend = SQLiteBackend(layout=layout)
    store = SemanticEventsStore(backend=backend)
    events = store.list_all()

    assert len(events) == 1

    event = events[0]
    assert event.event_type == "materialized"
    assert event.domain_key == "demo.asset"
    assert event.payload == {"rows": 123}
    assert event.flow_run_id != ""
    assert event.task_run_id != ""
