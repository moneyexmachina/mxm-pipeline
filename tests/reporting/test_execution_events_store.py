from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.events import ExecutionEvent, ExecutionEventType
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import FlowRun, TaskAttempt, TaskRun
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import (
    ExecutionEventsStore,
    FlowRunsStore,
    TaskAttemptsStore,
    TaskRunsStore,
)
from mxm.types import JSONMap
from mxm.types.timestamps import ts_ns_from_str


def _make_stores(
    tmp_path: Path,
) -> tuple[
    FlowRunsStore,
    TaskRunsStore,
    TaskAttemptsStore,
    ExecutionEventsStore,
]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    return (
        FlowRunsStore(backend=backend),
        TaskRunsStore(backend=backend),
        TaskAttemptsStore(backend=backend),
        ExecutionEventsStore(backend=backend),
    )


def _insert_parent_flow(
    flow_runs: FlowRunsStore,
    *,
    flow_run_id: str,
    flow_name: str,
) -> None:
    flow_runs.upsert(
        FlowRun(
            flow_run_id=flow_run_id,
            flow_name=flow_name,
        )
    )


def _insert_parent_task_run(
    task_runs: TaskRunsStore,
    *,
    task_run_id: str,
    flow_run_id: str,
    task_name: str,
) -> None:
    task_runs.upsert(
        TaskRun(
            task_run_id=task_run_id,
            flow_run_id=flow_run_id,
            task_name=task_name,
        )
    )


def _insert_parent_task_attempt(
    task_attempts: TaskAttemptsStore,
    *,
    task_attempt_id: str,
    task_run_id: str,
    attempt_number: int,
) -> None:
    task_attempts.upsert(
        TaskAttempt(
            task_attempt_id=task_attempt_id,
            task_run_id=task_run_id,
            attempt_number=attempt_number,
        )
    )


def test_append_and_get_round_trip(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(
        flow_runs,
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )
    _insert_parent_task_run(
        task_runs,
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
    )
    _insert_parent_task_attempt(
        task_attempts,
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )

    payload: JSONMap = {
        "rows": 123,
        "status": "ok",
    }

    event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.123456789Z"),
        entity_type="task_attempt",
        entity_id="task-attempt-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-001",
        payload=payload,
    )

    execution_events.append(event)
    restored = execution_events.get("event-001")

    assert restored == event


def test_get_missing_returns_none(tmp_path: Path) -> None:
    _, _, _, execution_events = _make_stores(tmp_path)

    assert execution_events.get("missing-event") is None


def test_append_is_idempotent_by_event_id(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(
        flow_runs,
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )
    _insert_parent_task_run(
        task_runs,
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
    )
    _insert_parent_task_attempt(
        task_attempts,
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )

    event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.123456789Z"),
        entity_type="task_attempt",
        entity_id="task-attempt-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-001",
        payload={},
    )

    execution_events.append(event)
    execution_events.append(event)

    rows = execution_events.list_for_task_attempt("task-attempt-001")

    assert rows == [event]


def test_list_for_flow_run_returns_only_matching_rows(tmp_path: Path) -> None:
    flow_runs, _, _, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(flow_runs, flow_run_id="flow-run-001", flow_name="flow-a")
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-002", flow_name="flow-b")

    event_a1 = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.000000001Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        payload={},
    )
    event_a2 = ExecutionEvent(
        event_id="event-002",
        event_type=ExecutionEventType.FLOW_RUN_FINISHED,
        event_ts=ts_ns_from_str("2026-04-20T11:05:00.000000002Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        payload={},
    )
    event_b1 = ExecutionEvent(
        event_id="event-003",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:10:00.000000003Z"),
        entity_type="flow_run",
        entity_id="flow-run-002",
        flow_run_id="flow-run-002",
        payload={},
    )

    execution_events.append(event_a1)
    execution_events.append(event_a2)
    execution_events.append(event_b1)

    rows = execution_events.list_for_flow_run("flow-run-001")

    assert rows == [event_a1, event_a2]


def test_list_for_task_run_returns_only_matching_rows(tmp_path: Path) -> None:
    flow_runs, task_runs, _, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(
        flow_runs,
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )
    _insert_parent_task_run(
        task_runs,
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="task-a",
    )
    _insert_parent_task_run(
        task_runs,
        task_run_id="task-run-002",
        flow_run_id="flow-run-001",
        task_name="task-b",
    )

    event_a1 = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_RUN_CREATED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.000000001Z"),
        entity_type="task_run",
        entity_id="task-run-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        payload={},
    )
    event_a2 = ExecutionEvent(
        event_id="event-002",
        event_type=ExecutionEventType.TASK_RUN_SKIPPED,
        event_ts=ts_ns_from_str("2026-04-20T11:01:00.000000002Z"),
        entity_type="task_run",
        entity_id="task-run-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        payload={},
    )
    event_b1 = ExecutionEvent(
        event_id="event-003",
        event_type=ExecutionEventType.TASK_RUN_CREATED,
        event_ts=ts_ns_from_str("2026-04-20T11:02:00.000000003Z"),
        entity_type="task_run",
        entity_id="task-run-002",
        flow_run_id="flow-run-001",
        task_run_id="task-run-002",
        payload={},
    )

    execution_events.append(event_a1)
    execution_events.append(event_a2)
    execution_events.append(event_b1)

    rows = execution_events.list_for_task_run("task-run-001")

    assert rows == [event_a1, event_a2]


def test_list_for_task_attempt_returns_only_matching_rows(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(
        flow_runs,
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )
    _insert_parent_task_run(
        task_runs,
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
    )
    _insert_parent_task_attempt(
        task_attempts,
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )
    _insert_parent_task_attempt(
        task_attempts,
        task_attempt_id="task-attempt-002",
        task_run_id="task-run-001",
        attempt_number=2,
    )

    event_1 = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.000000001Z"),
        entity_type="task_attempt",
        entity_id="task-attempt-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-001",
        payload={},
    )
    event_2 = ExecutionEvent(
        event_id="event-002",
        event_type=ExecutionEventType.TASK_ATTEMPT_FAILED,
        event_ts=ts_ns_from_str("2026-04-20T11:01:00.000000002Z"),
        entity_type="task_attempt",
        entity_id="task-attempt-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-001",
        payload={},
    )
    event_3 = ExecutionEvent(
        event_id="event-003",
        event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:02:00.000000003Z"),
        entity_type="task_attempt",
        entity_id="task-attempt-002",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        task_attempt_id="task-attempt-002",
        payload={},
    )

    execution_events.append(event_1)
    execution_events.append(event_2)
    execution_events.append(event_3)

    rows = execution_events.list_for_task_attempt("task-attempt-001")

    assert rows == [event_1, event_2]


def test_list_methods_return_rows_in_deterministic_order(tmp_path: Path) -> None:
    flow_runs, _, _, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(
        flow_runs,
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )

    event_3 = ExecutionEvent(
        event_id="event-003",
        event_type=ExecutionEventType.FLOW_RUN_FINISHED,
        event_ts=ts_ns_from_str("2026-04-20T11:05:00.000000003Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        payload={},
    )
    event_2b = ExecutionEvent(
        event_id="event-002b",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.000000002Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        payload={},
    )
    event_2a = ExecutionEvent(
        event_id="event-002a",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.000000002Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        payload={},
    )
    event_1 = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T10:59:59.999999999Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        payload={},
    )

    execution_events.append(event_3)
    execution_events.append(event_2b)
    execution_events.append(event_2a)
    execution_events.append(event_1)

    rows = execution_events.list_for_flow_run("flow-run-001")

    assert rows == [event_1, event_2a, event_2b, event_3]


def test_payload_and_nullable_parent_ids_round_trip(tmp_path: Path) -> None:
    flow_runs, _, _, execution_events = _make_stores(tmp_path)

    _insert_parent_flow(
        flow_runs,
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )

    payload: JSONMap = {
        "outer": {
            "rows": 123,
            "ok": True,
        },
        "name": "demo",
    }

    event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=ts_ns_from_str("2026-04-20T11:00:00.123456789Z"),
        entity_type="flow_run",
        entity_id="flow-run-001",
        flow_run_id="flow-run-001",
        task_run_id=None,
        task_attempt_id=None,
        payload=payload,
    )

    execution_events.append(event)
    restored = execution_events.get("event-001")

    assert restored == event
    assert restored is not None
    assert restored.flow_run_id == "flow-run-001"
    assert restored.task_run_id is None
    assert restored.task_attempt_id is None
    assert restored.payload == payload
