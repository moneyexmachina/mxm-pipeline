from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import (
    FlowRun,
    SemanticEvent,
    TaskAttempt,
    TaskRun,
)
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import (
    FlowRunsStore,
    SemanticEventsStore,
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
    SemanticEventsStore,
]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    return (
        FlowRunsStore(backend=backend),
        TaskRunsStore(backend=backend),
        TaskAttemptsStore(backend=backend),
        SemanticEventsStore(backend=backend),
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
    flow_runs, task_runs, task_attempts, semantic_events = _make_stores(tmp_path)

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

    event = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.123456789Z"),
        domain_key="demo.asset",
        payload=payload,
    )

    semantic_events.append(event)
    restored = semantic_events.get("event-001")

    assert restored == event


def test_get_missing_returns_none(tmp_path: Path) -> None:
    _, _, _, semantic_events = _make_stores(tmp_path)

    assert semantic_events.get("missing-event") is None


def test_append_is_idempotent_by_event_id(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts, semantic_events = _make_stores(tmp_path)

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

    event = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.123456789Z"),
        domain_key="demo.asset",
        payload={},
    )

    semantic_events.append(event)
    semantic_events.append(event)

    rows = semantic_events.list_for_task_attempt("task-attempt-001")

    assert rows == [event]


def test_list_for_task_attempt_returns_only_matching_rows(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts, semantic_events = _make_stores(tmp_path)

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

    event_1 = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000001Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2 = SemanticEvent(
        event_id="event-002",
        task_attempt_id="task-attempt-001",
        event_type="validated",
        event_ts=ts_ns_from_str("2026-04-20T12:01:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_3 = SemanticEvent(
        event_id="event-003",
        task_attempt_id="task-attempt-002",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:02:00.000000003Z"),
        domain_key="other.asset",
        payload={},
    )

    semantic_events.append(event_1)
    semantic_events.append(event_2)
    semantic_events.append(event_3)

    rows = semantic_events.list_for_task_attempt("task-attempt-001")

    assert rows == [event_1, event_2]


def test_list_for_task_attempt_returns_rows_in_deterministic_order(
    tmp_path: Path,
) -> None:
    flow_runs, task_runs, task_attempts, semantic_events = _make_stores(tmp_path)

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

    event_3 = SemanticEvent(
        event_id="event-003",
        task_attempt_id="task-attempt-001",
        event_type="published",
        event_ts=ts_ns_from_str("2026-04-20T12:05:00.000000003Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2b = SemanticEvent(
        event_id="event-002b",
        task_attempt_id="task-attempt-001",
        event_type="validated",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2a = SemanticEvent(
        event_id="event-002a",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_1 = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="started",
        event_ts=ts_ns_from_str("2026-04-20T11:59:59.999999999Z"),
        domain_key="demo.asset",
        payload={},
    )

    semantic_events.append(event_3)
    semantic_events.append(event_2b)
    semantic_events.append(event_2a)
    semantic_events.append(event_1)

    rows = semantic_events.list_for_task_attempt("task-attempt-001")

    assert rows == [event_1, event_2a, event_2b, event_3]


def test_domain_key_and_nested_payload_round_trip(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts, semantic_events = _make_stores(tmp_path)

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
        "outer": {
            "rows": 123,
            "ok": True,
        },
        "name": "demo",
    }

    event = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.123456789Z"),
        domain_key="demo.asset",
        payload=payload,
    )

    semantic_events.append(event)
    restored = semantic_events.get("event-001")

    assert restored == event
    assert restored is not None
    assert restored.task_attempt_id == "task-attempt-001"
    assert restored.domain_key == "demo.asset"
    assert restored.payload == payload
