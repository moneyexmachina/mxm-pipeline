from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.events import ExecutionEvent, ExecutionEventType
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import (
    FlowRun,
    RunStatus,
    SemanticEvent,
    TaskAttempt,
    TaskRun,
)
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import (
    ExecutionEventsStore,
    FlowRunsStore,
    ReportingStore,
    SemanticEventsStore,
    TaskAttemptsStore,
    TaskRunsStore,
)
from mxm.types.timestamps import ts_ns_from_str


def _make_backend(tmp_path: Path) -> SQLiteBackend:
    layout = ReportingLayout(root=tmp_path)
    return SQLiteBackend(layout=layout)


def test_from_backend_constructs_all_component_stores(tmp_path: Path) -> None:
    backend = _make_backend(tmp_path)

    store = ReportingStore.from_backend(backend)

    assert isinstance(store.flow_runs, FlowRunsStore)
    assert isinstance(store.task_runs, TaskRunsStore)
    assert isinstance(store.task_attempts, TaskAttemptsStore)
    assert isinstance(store.execution_events, ExecutionEventsStore)
    assert isinstance(store.semantic_events, SemanticEventsStore)


def test_from_backend_reuses_same_backend_for_all_components(tmp_path: Path) -> None:
    backend = _make_backend(tmp_path)

    store = ReportingStore.from_backend(backend)

    assert store.flow_runs.backend is backend
    assert store.task_runs.backend is backend
    assert store.task_attempts.backend is backend
    assert store.execution_events.backend is backend
    assert store.semantic_events.backend is backend


def test_reporting_store_supports_happy_path_round_trip(tmp_path: Path) -> None:
    backend = _make_backend(tmp_path)
    store = ReportingStore.from_backend(backend)

    flow_run = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T13:00:00.123456789Z"),
    )
    task_run = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T13:00:01.123456789Z"),
        attempt_count=1,
    )
    task_attempt = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T13:00:02.123456789Z"),
    )
    execution_event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T13:00:03.123456789Z"),
        entity_type="task_attempt",
        entity_id="task-attempt-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-001",
        payload={"rows": 123},
    )
    semantic_event = SemanticEvent(
        event_id="event-002",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T13:00:04.123456789Z"),
        domain_key="demo.asset",
        payload={"rows": 123},
    )

    store.flow_runs.upsert(flow_run)
    store.task_runs.upsert(task_run)
    store.task_attempts.upsert(task_attempt)
    store.execution_events.append(execution_event)
    store.semantic_events.append(semantic_event)

    assert store.flow_runs.get("flow-run-001") == flow_run
    assert store.task_runs.get("task-run-001") == task_run
    assert store.task_attempts.get("task-attempt-001") == task_attempt
    assert store.execution_events.get("event-001") == execution_event
    assert store.semantic_events.get("event-002") == semantic_event
