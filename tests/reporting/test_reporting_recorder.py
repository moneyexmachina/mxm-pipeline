from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.events import ExecutionEventType
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import RunStatus, SemanticEvent
from mxm.pipeline.reporting.recorder import ReportingRecorder
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import ReportingStore
from mxm.types.timestamps import is_ts_ns, ts_ns_from_str


def _make_recorder(tmp_path: Path) -> tuple[ReportingRecorder, ReportingStore]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    store = ReportingStore.from_backend(backend)
    recorder = ReportingRecorder(store=store)
    return recorder, store


def test_record_flow_started_persists_flow_run_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    started_at = ts_ns_from_str("2026-04-21T08:00:00.123456789Z")

    flow_run = recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=started_at,
    )

    assert flow_run.flow_run_id == "flow-run-001"
    assert flow_run.flow_name == "demo-flow"
    assert flow_run.status == RunStatus.RUNNING
    assert flow_run.started_at == started_at
    assert flow_run.finished_at is None

    assert store.flow_runs.get("flow-run-001") == flow_run

    events = store.execution_events.list_for_flow_run("flow-run-001")
    assert len(events) == 1

    event = events[0]
    assert event.event_type == ExecutionEventType.FLOW_RUN_STARTED
    assert event.event_ts == started_at
    assert event.entity_type == "flow_run"
    assert event.entity_id == "flow-run-001"
    assert event.flow_run_id == "flow-run-001"
    assert event.task_run_id is None
    assert event.task_attempt_id is None
    assert event.payload == {}


def test_record_flow_finished_persists_final_flow_run_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    started_at = ts_ns_from_str("2026-04-21T08:00:00.123456789Z")
    finished_at = ts_ns_from_str("2026-04-21T08:05:00.987654321Z")

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=started_at,
    )
    flow_run = recorder.record_flow_finished(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.SUCCEEDED,
        started_at=started_at,
        finished_at=finished_at,
    )

    assert flow_run.status == RunStatus.SUCCEEDED
    assert flow_run.started_at == started_at
    assert flow_run.finished_at == finished_at

    assert store.flow_runs.get("flow-run-001") == flow_run

    events = store.execution_events.list_for_flow_run("flow-run-001")
    assert len(events) == 2

    event = events[-1]
    assert event.event_type == ExecutionEventType.FLOW_RUN_FINISHED
    assert event.event_ts == finished_at
    assert event.entity_type == "flow_run"
    assert event.entity_id == "flow-run-001"
    assert event.flow_run_id == "flow-run-001"
    assert event.payload == {"status": "succeeded"}


def test_record_task_run_created_persists_task_run_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )

    task_run = recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=0,
    )

    assert task_run.task_run_id == "task-run-001"
    assert task_run.flow_run_id == "flow-run-001"
    assert task_run.task_name == "demo-task"
    assert task_run.status == RunStatus.PENDING
    assert task_run.started_at is None
    assert task_run.finished_at is None
    assert task_run.attempt_count == 0

    assert store.task_runs.get("task-run-001") == task_run

    events = store.execution_events.list_for_task_run("task-run-001")
    assert len(events) == 1

    event = events[0]
    assert event.event_type == ExecutionEventType.TASK_RUN_CREATED
    assert event.entity_type == "task_run"
    assert event.entity_id == "task-run-001"
    assert event.flow_run_id == "flow-run-001"
    assert event.task_run_id == "task-run-001"
    assert event.task_attempt_id is None
    assert event.payload == {}


def test_record_task_run_blocked_persists_task_run_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )

    blocked_at = ts_ns_from_str("2026-04-21T08:03:00.000000002Z")

    task_run = recorder.record_task_run_blocked(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=0,
        event_ts=blocked_at,
    )

    assert task_run.status == RunStatus.BLOCKED
    assert task_run.started_at is None
    assert task_run.finished_at == blocked_at
    assert task_run.attempt_count == 0

    assert store.task_runs.get("task-run-001") == task_run

    events = store.execution_events.list_for_task_run("task-run-001")
    assert len(events) == 1

    event = events[0]
    assert event.event_type == ExecutionEventType.TASK_RUN_BLOCKED
    assert event.event_ts == blocked_at


def test_record_task_run_updated_persists_state_without_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )
    recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=0,
    )

    started_at = ts_ns_from_str("2026-04-21T08:01:00.000000002Z")
    finished_at = ts_ns_from_str("2026-04-21T08:02:00.000000003Z")

    task_run = recorder.record_task_run_updated(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.SUCCEEDED,
        attempt_count=1,
        started_at=started_at,
        finished_at=finished_at,
    )

    assert task_run.status == RunStatus.SUCCEEDED
    assert task_run.started_at == started_at
    assert task_run.finished_at == finished_at
    assert task_run.attempt_count == 1

    assert store.task_runs.get("task-run-001") == task_run

    events = store.execution_events.list_for_task_run("task-run-001")
    assert len(events) == 1
    assert events[0].event_type == ExecutionEventType.TASK_RUN_CREATED


def test_record_task_attempt_started_persists_attempt_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )
    recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=0,
    )

    started_at = ts_ns_from_str("2026-04-21T08:01:00.123456789Z")

    task_attempt = recorder.record_task_attempt_started(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        attempt_number=1,
        started_at=started_at,
    )

    assert task_attempt.task_attempt_id == "task-attempt-001"
    assert task_attempt.task_run_id == "task-run-001"
    assert task_attempt.attempt_number == 1
    assert task_attempt.status == RunStatus.RUNNING
    assert task_attempt.started_at == started_at
    assert task_attempt.finished_at is None
    assert task_attempt.error_summary is None
    assert task_attempt.log_ref is None

    assert store.task_attempts.get("task-attempt-001") == task_attempt

    events = store.execution_events.list_for_task_attempt("task-attempt-001")
    assert len(events) == 1

    event = events[0]
    assert event.event_type == ExecutionEventType.TASK_ATTEMPT_STARTED
    assert event.event_ts == started_at
    assert event.entity_type == "task_attempt"
    assert event.entity_id == "task-attempt-001"
    assert event.flow_run_id == "flow-run-001"
    assert event.task_run_id == "task-run-001"
    assert event.task_attempt_id == "task-attempt-001"
    assert event.payload == {}


def test_record_task_attempt_succeeded_persists_attempt_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )
    recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=1,
    )

    started_at = ts_ns_from_str("2026-04-21T08:01:00.123456789Z")
    finished_at = ts_ns_from_str("2026-04-21T08:02:00.987654321Z")

    task_attempt = recorder.record_task_attempt_succeeded(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        attempt_number=1,
        started_at=started_at,
        finished_at=finished_at,
        log_ref="logs/task-attempt-001.log",
    )

    assert task_attempt.status == RunStatus.SUCCEEDED
    assert task_attempt.started_at == started_at
    assert task_attempt.finished_at == finished_at
    assert task_attempt.error_summary is None
    assert task_attempt.log_ref == "logs/task-attempt-001.log"

    assert store.task_attempts.get("task-attempt-001") == task_attempt

    events = store.execution_events.list_for_task_attempt("task-attempt-001")
    assert len(events) == 1

    event = events[0]
    assert event.event_type == ExecutionEventType.TASK_ATTEMPT_SUCCEEDED
    assert event.event_ts == finished_at
    assert event.payload == {}


def test_record_task_attempt_failed_persists_attempt_and_execution_event(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )
    recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=1,
    )

    started_at = ts_ns_from_str("2026-04-21T08:01:00.123456789Z")
    finished_at = ts_ns_from_str("2026-04-21T08:02:00.987654321Z")

    task_attempt = recorder.record_task_attempt_failed(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        attempt_number=1,
        started_at=started_at,
        finished_at=finished_at,
        error_summary="boom",
        log_ref="logs/task-attempt-001.log",
    )

    assert task_attempt.status == RunStatus.FAILED
    assert task_attempt.started_at == started_at
    assert task_attempt.finished_at == finished_at
    assert task_attempt.error_summary == "boom"
    assert task_attempt.log_ref == "logs/task-attempt-001.log"

    assert store.task_attempts.get("task-attempt-001") == task_attempt

    events = store.execution_events.list_for_task_attempt("task-attempt-001")
    assert len(events) == 1

    event = events[0]
    assert event.event_type == ExecutionEventType.TASK_ATTEMPT_FAILED
    assert event.event_ts == finished_at
    assert event.payload == {
        "error_summary": "boom",
        "log_ref": "logs/task-attempt-001.log",
    }


def test_record_semantic_event_persists_semantic_and_execution_trace(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        started_at=ts_ns_from_str("2026-04-21T08:00:00.000000001Z"),
    )
    recorder.record_task_run_created(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        attempt_count=1,
    )
    recorder.record_task_attempt_started(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        attempt_number=1,
        started_at=ts_ns_from_str("2026-04-21T08:01:00.000000002Z"),
    )

    event = SemanticEvent(
        event_id="semantic-event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-21T08:02:00.123456789Z"),
        domain_key="demo.asset",
        payload={"rows": 123},
    )

    returned = recorder.record_semantic_event(event)

    assert returned == event
    assert store.semantic_events.get("semantic-event-001") == event

    events = store.execution_events.list_for_task_attempt("task-attempt-001")
    assert len(events) == 2

    execution_event = events[-1]
    assert execution_event.event_type == ExecutionEventType.SEMANTIC_EVENT_EMITTED
    assert execution_event.event_ts == event.event_ts
    assert execution_event.entity_type == "task_attempt"
    assert execution_event.entity_id == "task-attempt-001"
    assert execution_event.flow_run_id is None
    assert execution_event.task_run_id is None
    assert execution_event.task_attempt_id == "task-attempt-001"
    assert execution_event.payload == {
        "semantic_event_id": "semantic-event-001",
        "semantic_event_type": "materialized",
        "domain_key": "demo.asset",
    }


def test_record_flow_started_uses_default_timestamp_when_not_provided(
    tmp_path: Path,
) -> None:
    recorder, store = _make_recorder(tmp_path)

    flow_run = recorder.record_flow_started(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )

    assert flow_run.started_at is not None
    assert is_ts_ns(flow_run.started_at)

    stored = store.flow_runs.get("flow-run-001")
    assert stored is not None
    assert stored.started_at == flow_run.started_at

    events = store.execution_events.list_for_flow_run("flow-run-001")
    assert len(events) == 1
    assert events[0].event_ts == flow_run.started_at
