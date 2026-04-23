from __future__ import annotations

import logging
from pathlib import Path

from mxm.pipeline.execution.context import ExecutionContext
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.sinks import ReportingSemanticEventSink
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import SemanticEventsStore
from mxm.types import JSONMap
from mxm.types.timestamps import is_ts_ns


def _make_store_and_sink(
    tmp_path: Path,
) -> tuple[SemanticEventsStore, ReportingSemanticEventSink]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    store = SemanticEventsStore(backend=backend)
    sink = ReportingSemanticEventSink(store=store)
    return store, sink


def test_execution_context_emits_semantic_event_to_sqlite(
    tmp_path: Path,
) -> None:
    store, sink = _make_store_and_sink(tmp_path)

    ctx = ExecutionContext(
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        flow_name="demo-flow",
        task_name="demo-task",
        logger=logging.getLogger("test.reporting.semantic.integration"),
        semantic_event_sink=sink,
        metadata={"test_case": "semantic_event_integration"},
    )

    payload: JSONMap = {
        "rows": 123,
        "source": "demo",
    }

    event = ctx.emit_semantic_event(
        event_type="materialized",
        domain_key="demo.asset",
        payload=payload,
    )

    assert event.event_id != ""
    assert event.flow_run_id == "flow-run-001"
    assert event.task_run_id == "task-run-001"
    assert event.event_type == "materialized"
    assert event.domain_key == "demo.asset"
    assert event.payload == payload
    assert is_ts_ns(event.event_ts)

    stored_semantic_event = store.get(event.event_id)
    assert stored_semantic_event == event

    task_rows = store.list_for_task_run("task-run-001")
    assert task_rows == [event]

    flow_rows = store.list_for_flow_run("flow-run-001")
    assert flow_rows == [event]
