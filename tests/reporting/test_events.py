from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from mxm.pipeline.reporting.events import ExecutionEvent, ExecutionEventType
from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.types.timestamps import assert_ts_ns, is_ts_ns


def test_execution_event_type_values_are_stable() -> None:
    assert ExecutionEventType.FLOW_RUN_STARTED.value == "flow_run_started"
    assert ExecutionEventType.FLOW_RUN_FINISHED.value == "flow_run_finished"
    assert ExecutionEventType.TASK_RUN_CREATED.value == "task_run_created"
    assert ExecutionEventType.TASK_RUN_BLOCKED.value == "task_run_blocked"
    assert ExecutionEventType.TASK_RUN_SKIPPED.value == "task_run_skipped"
    assert ExecutionEventType.TASK_ATTEMPT_STARTED.value == "task_attempt_started"
    assert ExecutionEventType.TASK_ATTEMPT_SUCCEEDED.value == "task_attempt_succeeded"
    assert ExecutionEventType.TASK_ATTEMPT_FAILED.value == "task_attempt_failed"
    assert ExecutionEventType.SEMANTIC_EVENT_EMITTED.value == "semantic_event_emitted"


def test_execution_event_defaults() -> None:
    event_ts = utc_now_ts_ns()
    event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_ATTEMPT_STARTED,
        event_ts=event_ts,
        entity_type="task_attempt",
        entity_id="task-attempt-001",
    )

    assert event.event_id == "event-001"
    assert event.event_type == ExecutionEventType.TASK_ATTEMPT_STARTED
    assert event.event_ts == event_ts
    assert event.entity_type == "task_attempt"
    assert event.entity_id == "task-attempt-001"
    assert event.flow_run_id is None
    assert event.task_run_id is None
    assert event.task_attempt_id is None
    assert event.payload == {}


def test_execution_event_timestamp_is_canonical() -> None:
    event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=utc_now_ts_ns(),
        entity_type="flow_run",
        entity_id="flow-run-001",
    )

    assert is_ts_ns(event.event_ts)
    assert_ts_ns(event.event_ts)


def test_execution_event_payload_default_is_not_shared() -> None:
    event_1 = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.FLOW_RUN_STARTED,
        event_ts=utc_now_ts_ns(),
        entity_type="flow_run",
        entity_id="flow-run-001",
    )
    event_2 = ExecutionEvent(
        event_id="event-002",
        event_type=ExecutionEventType.TASK_RUN_CREATED,
        event_ts=utc_now_ts_ns(),
        entity_type="task_run",
        entity_id="task-run-001",
    )

    assert event_1.payload == {}
    assert event_2.payload == {}
    assert event_1.payload is not event_2.payload


def test_execution_event_is_frozen() -> None:
    event = ExecutionEvent(
        event_id="event-001",
        event_type=ExecutionEventType.TASK_ATTEMPT_FAILED,
        event_ts=utc_now_ts_ns(),
        entity_type="task_attempt",
        entity_id="task-attempt-001",
    )

    with pytest.raises(FrozenInstanceError):
        event.entity_id = "task-attempt-002"  # type: ignore[misc]
