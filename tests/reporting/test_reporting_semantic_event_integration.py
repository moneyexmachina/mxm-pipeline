from __future__ import annotations

import logging
from pathlib import Path

from mxm.pipeline.execution.context import ExecutionContext
from mxm.pipeline.reporting.events import ExecutionEventType
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import RunStatus
from mxm.pipeline.reporting.recorder import ReportingRecorder
from mxm.pipeline.reporting.sinks import ReportingSemanticEventSink
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import ReportingStore
from mxm.types import JSONMap
from mxm.types.timestamps import is_ts_ns, ts_ns_from_str


def _make_reporting_stack(
    tmp_path: Path,
) -> tuple[ReportingRecorder, ReportingStore]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    store = ReportingStore.from_backend(backend)
    recorder = ReportingRecorder(store=store)
    return recorder, store


def test_execution_context_emits_semantic_event_into_reporting_trace(
    tmp_path: Path,
) -> None:
    recorder, store = _make_reporting_stack(tmp_path)

    # -------------------------------------------------------------------------
    # Prepare parent reporting state required by FK constraints
    # -------------------------------------------------------------------------

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T09:00:00.000000001Z"),
    )

    recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=1,
    )

    recorder.record_task_run_updated(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.RUNNING,
        attempt_count=1,
        started_at=ts_ns_from_str("2026-04-21T09:00:01.000000002Z"),
        finished_at=None,
    )

    recorder.record_task_attempt_started(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        attempt_number=1,
        started_at=ts_ns_from_str("2026-04-21T09:00:02.000000003Z"),
    )

    # -------------------------------------------------------------------------
    # Build ExecutionContext with recorder-backed semantic sink
    # -------------------------------------------------------------------------

    sink = ReportingSemanticEventSink(recorder=recorder)

    ctx = ExecutionContext(
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-001",
        flow_name="demo-flow",
        task_name="demo-task",
        attempt_number=1,
        logger=logging.getLogger("test.reporting.semantic.integration"),
        semantic_event_sink=sink,
        metadata={"test_case": "semantic_event_integration"},
    )

    payload: JSONMap = {
        "rows": 123,
        "source": "demo",
    }

    # -------------------------------------------------------------------------
    # Emit semantic event through context
    # -------------------------------------------------------------------------

    event = ctx.emit_semantic_event(
        event_type="materialized",
        domain_key="demo.asset",
        payload=payload,
    )

    # -------------------------------------------------------------------------
    # Returned event contract
    # -------------------------------------------------------------------------

    assert event.event_id != ""
    assert event.task_attempt_id == "task-attempt-001"
    assert event.event_type == "materialized"
    assert event.domain_key == "demo.asset"
    assert event.payload == payload
    assert is_ts_ns(event.event_ts)

    # -------------------------------------------------------------------------
    # Semantic event persistence
    # -------------------------------------------------------------------------

    stored_semantic_event = store.semantic_events.get(event.event_id)
    assert stored_semantic_event == event

    semantic_rows = store.semantic_events.list_for_task_attempt("task-attempt-001")
    assert semantic_rows == [event]

    # -------------------------------------------------------------------------
    # Matching execution trace persistence
    # -------------------------------------------------------------------------

    execution_rows = store.execution_events.list_for_task_attempt("task-attempt-001")

    # We already created one attempt-started execution event during setup,
    # and semantic emission should append one more.
    assert len(execution_rows) == 2

    execution_event = execution_rows[-1]
    assert execution_event.event_type == ExecutionEventType.SEMANTIC_EVENT_EMITTED
    assert execution_event.event_ts == event.event_ts
    assert execution_event.entity_type == "task_attempt"
    assert execution_event.entity_id == "task-attempt-001"
    assert execution_event.flow_run_id is None
    assert execution_event.task_run_id is None
    assert execution_event.task_attempt_id == "task-attempt-001"
    assert execution_event.payload == {
        "semantic_event_id": event.event_id,
        "semantic_event_type": "materialized",
        "domain_key": "demo.asset",
    }
