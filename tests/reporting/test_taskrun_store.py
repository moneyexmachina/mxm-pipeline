from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import FlowRun, RunStatus, TaskRun
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import FlowRunsStore, TaskRunsStore
from mxm.types.timestamps import ts_ns_from_str


def _make_stores(tmp_path: Path) -> tuple[FlowRunsStore, TaskRunsStore]:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    return (
        FlowRunsStore(backend=backend),
        TaskRunsStore(backend=backend),
    )


def _insert_parent_flow(
    flow_runs: FlowRunsStore, *, flow_run_id: str, flow_name: str
) -> None:
    flow_runs.upsert(
        FlowRun(
            flow_run_id=flow_run_id,
            flow_name=flow_name,
        )
    )


def test_upsert_and_get_round_trip(tmp_path: Path) -> None:
    flow_runs, task_runs = _make_stores(tmp_path)
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-001", flow_name="demo-flow")

    task_run = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.PENDING,
    )

    task_runs.upsert(task_run)
    restored = task_runs.get("task-run-001")

    assert restored == task_run


def test_get_missing_returns_none(tmp_path: Path) -> None:
    _, task_runs = _make_stores(tmp_path)

    assert task_runs.get("missing-task-run") is None


def test_upsert_replaces_existing_task_run_state(tmp_path: Path) -> None:
    flow_runs, task_runs = _make_stores(tmp_path)
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-001", flow_name="demo-flow")

    initial = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.PENDING,
        attempt_count=0,
    )
    running = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T09:00:00.123456789Z"),
        attempt_count=1,
    )
    finished = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.SUCCEEDED,
        started_at=ts_ns_from_str("2026-04-20T09:00:00.123456789Z"),
        finished_at=ts_ns_from_str("2026-04-20T09:02:30.987654321Z"),
        attempt_count=1,
    )

    task_runs.upsert(initial)
    task_runs.upsert(running)
    assert task_runs.get("task-run-001") == running

    task_runs.upsert(finished)
    assert task_runs.get("task-run-001") == finished


def test_list_for_flow_run_returns_only_rows_for_that_flow(tmp_path: Path) -> None:
    flow_runs, task_runs = _make_stores(tmp_path)
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-001", flow_name="alpha-flow")
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-002", flow_name="beta-flow")

    task_a = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="extract",
    )
    task_b = TaskRun(
        task_run_id="task-run-002",
        flow_run_id="flow-run-001",
        task_name="transform",
    )
    task_c = TaskRun(
        task_run_id="task-run-003",
        flow_run_id="flow-run-002",
        task_name="load",
    )

    task_runs.upsert(task_a)
    task_runs.upsert(task_b)
    task_runs.upsert(task_c)

    rows = task_runs.list_for_flow_run("flow-run-001")

    assert rows == [task_a, task_b]


def test_list_for_flow_run_returns_rows_in_deterministic_order(tmp_path: Path) -> None:
    flow_runs, task_runs = _make_stores(tmp_path)
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-001", flow_name="demo-flow")

    task_b = TaskRun(
        task_run_id="task-run-003",
        flow_run_id="flow-run-001",
        task_name="beta-task",
    )
    task_a2 = TaskRun(
        task_run_id="task-run-002",
        flow_run_id="flow-run-001",
        task_name="alpha-task",
    )
    task_a1 = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="alpha-task",
    )

    task_runs.upsert(task_b)
    task_runs.upsert(task_a2)
    task_runs.upsert(task_a1)

    rows = task_runs.list_for_flow_run("flow-run-001")

    assert rows == [task_a1, task_a2, task_b]


def test_timestamps_and_attempt_count_round_trip(tmp_path: Path) -> None:
    flow_runs, task_runs = _make_stores(tmp_path)
    _insert_parent_flow(flow_runs, flow_run_id="flow-run-001", flow_name="demo-flow")

    task_run = TaskRun(
        task_run_id="task-run-001",
        flow_run_id="flow-run-001",
        task_name="demo-task",
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T09:00:00.123456789Z"),
        finished_at=ts_ns_from_str("2026-04-20T09:02:30.987654321Z"),
        attempt_count=3,
    )

    task_runs.upsert(task_run)
    restored = task_runs.get("task-run-001")

    assert restored == task_run
    assert restored is not None
    assert restored.started_at == ts_ns_from_str("2026-04-20T09:00:00.123456789Z")
    assert restored.finished_at == ts_ns_from_str("2026-04-20T09:02:30.987654321Z")
    assert restored.attempt_count == 3
