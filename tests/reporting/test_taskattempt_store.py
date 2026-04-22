from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import (
    FlowRun,
    RunStatus,
    TaskAttempt,
    TaskRun,
)
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import (
    FlowRunsStore,
    TaskAttemptsStore,
    TaskRunsStore,
)
from mxm.types.timestamps import ts_ns_from_str


def _make_stores(
    tmp_path: Path,
) -> tuple[FlowRunsStore, TaskRunsStore, TaskAttemptsStore]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    return (
        FlowRunsStore(backend=backend),
        TaskRunsStore(backend=backend),
        TaskAttemptsStore(backend=backend),
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


def test_upsert_and_get_round_trip(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts = _make_stores(tmp_path)

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

    task_attempt = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
        status=RunStatus.PENDING,
    )

    task_attempts.upsert(task_attempt)
    restored = task_attempts.get("task-attempt-001")

    assert restored == task_attempt


def test_get_missing_returns_none(tmp_path: Path) -> None:
    _, _, task_attempts = _make_stores(tmp_path)

    assert task_attempts.get("missing-task-attempt") is None


def test_upsert_replaces_existing_task_attempt_state(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts = _make_stores(tmp_path)

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

    initial = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
        status=RunStatus.PENDING,
    )
    running = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T10:00:00.123456789Z"),
    )
    failed = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
        status=RunStatus.FAILED,
        started_at=ts_ns_from_str("2026-04-20T10:00:00.123456789Z"),
        finished_at=ts_ns_from_str("2026-04-20T10:01:30.987654321Z"),
        error_summary="boom",
        log_ref="logs/task-attempt-001.log",
    )

    task_attempts.upsert(initial)
    task_attempts.upsert(running)
    assert task_attempts.get("task-attempt-001") == running

    task_attempts.upsert(failed)
    assert task_attempts.get("task-attempt-001") == failed


def test_list_for_task_run_returns_only_rows_for_that_task_run(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts = _make_stores(tmp_path)

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

    attempt_a1 = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )
    attempt_a2 = TaskAttempt(
        task_attempt_id="task-attempt-002",
        task_run_id="task-run-001",
        attempt_number=2,
    )
    attempt_b1 = TaskAttempt(
        task_attempt_id="task-attempt-003",
        task_run_id="task-run-002",
        attempt_number=1,
    )

    task_attempts.upsert(attempt_a1)
    task_attempts.upsert(attempt_a2)
    task_attempts.upsert(attempt_b1)

    rows = task_attempts.list_for_task_run("task-run-001")

    assert rows == [attempt_a1, attempt_a2]


def test_list_for_task_run_returns_rows_in_deterministic_order(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts = _make_stores(tmp_path)

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

    attempt_3 = TaskAttempt(
        task_attempt_id="task-attempt-003",
        task_run_id="task-run-001",
        attempt_number=3,
    )
    attempt_2b = TaskAttempt(
        task_attempt_id="task-attempt-002b",
        task_run_id="task-run-001",
        attempt_number=2,
    )
    attempt_2a = TaskAttempt(
        task_attempt_id="task-attempt-002a",
        task_run_id="task-run-001",
        attempt_number=2,
    )
    attempt_1 = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=1,
    )

    task_attempts.upsert(attempt_3)
    task_attempts.upsert(attempt_2b)
    task_attempts.upsert(attempt_2a)
    task_attempts.upsert(attempt_1)

    rows = task_attempts.list_for_task_run("task-run-001")

    assert rows == [attempt_1, attempt_2a, attempt_2b, attempt_3]


def test_timestamps_and_optional_fields_round_trip(tmp_path: Path) -> None:
    flow_runs, task_runs, task_attempts = _make_stores(tmp_path)

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

    task_attempt = TaskAttempt(
        task_attempt_id="task-attempt-001",
        task_run_id="task-run-001",
        attempt_number=2,
        status=RunStatus.FAILED,
        started_at=ts_ns_from_str("2026-04-20T10:00:00.123456789Z"),
        finished_at=ts_ns_from_str("2026-04-20T10:01:30.987654321Z"),
        error_summary="network timeout",
        log_ref="logs/task-attempt-001.log",
    )

    task_attempts.upsert(task_attempt)
    restored = task_attempts.get("task-attempt-001")

    assert restored == task_attempt
    assert restored is not None
    assert restored.started_at == ts_ns_from_str("2026-04-20T10:00:00.123456789Z")
    assert restored.finished_at == ts_ns_from_str("2026-04-20T10:01:30.987654321Z")
    assert restored.error_summary == "network timeout"
    assert restored.log_ref == "logs/task-attempt-001.log"
