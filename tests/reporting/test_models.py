from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from mxm.pipeline.reporting.models import (
    FlowRun,
    RunStatus,
    SemanticEvent,
    TaskAttempt,
    TaskRun,
)
from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.types import is_ts_ns


def test_run_status_values_are_stable() -> None:
    assert RunStatus.PENDING.value == "pending"
    assert RunStatus.RUNNING.value == "running"
    assert RunStatus.SUCCEEDED.value == "succeeded"
    assert RunStatus.FAILED.value == "failed"
    assert RunStatus.SKIPPED.value == "skipped"
    assert RunStatus.BLOCKED.value == "blocked"


def test_flow_run_defaults() -> None:
    flow_run = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )

    assert flow_run.flow_run_id == "flow-run-001"
    assert flow_run.flow_name == "demo-flow"
    assert flow_run.status == RunStatus.PENDING
    assert flow_run.started_at is None
    assert flow_run.finished_at is None


def test_task_run_defaults() -> None:
    task_run = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
    )

    assert task_run.task_run_id == "task-run-001"
    assert task_run.flow_run_id == "flow-run-001"
    assert task_run.task_name == "demo-task"
    assert task_run.status == RunStatus.PENDING
    assert task_run.started_at is None
    assert task_run.finished_at is None
    assert task_run.attempt_count == 0


def test_task_attempt_defaults() -> None:
    task_attempt = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )

    assert task_attempt.task_attempt_id == "task-attempt-001"
    assert task_attempt.task_run_id == "task-run-001"
    assert task_attempt.attempt_number == 1
    assert task_attempt.status == RunStatus.PENDING
    assert task_attempt.started_at is None
    assert task_attempt.finished_at is None
    assert task_attempt.error_summary is None
    assert task_attempt.log_ref is None


def test_semantic_event_payload_default_is_not_shared() -> None:
    event_1 = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )
    event_2 = SemanticEvent(
        event_id="event-002",
        task_attempt_id="task-attempt-002",
        event_type="observed",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )

    assert event_1.payload == {}
    assert event_2.payload == {}
    assert event_1.payload is not event_2.payload


def test_semantic_event_timestamp_is_canonical() -> None:
    event = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )

    assert is_ts_ns(event.event_ts)


def test_models_are_frozen() -> None:
    flow_run = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
    )
    task_run = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
    )
    task_attempt = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )
    semantic_event = SemanticEvent(
        event_id="event-001",
        task_attempt_id="task-attempt-001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )

    with pytest.raises(FrozenInstanceError):
        flow_run.status = RunStatus.RUNNING  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        task_run.attempt_count = 1  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        task_attempt.error_summary = "boom"  # type: ignore[misc]

    with pytest.raises(FrozenInstanceError):
        semantic_event.domain_key = "other.asset"  # type: ignore[misc]
