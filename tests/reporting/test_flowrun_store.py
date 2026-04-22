from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import FlowRun, RunStatus
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import FlowRunsStore
from mxm.types.timestamps import ts_ns_from_str


def _make_store(tmp_path: Path) -> FlowRunsStore:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    return FlowRunsStore(backend=backend)


def test_upsert_and_get_round_trip(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    flow_run = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.PENDING,
    )

    store.upsert(flow_run)
    restored = store.get("flow-run-001")

    assert restored == flow_run


def test_get_missing_returns_none(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    assert store.get("missing-flow-run") is None


def test_upsert_replaces_existing_flow_run_state(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    initial = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.PENDING,
    )
    running = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T08:00:00.123456789Z"),
    )
    finished = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.SUCCEEDED,
        started_at=ts_ns_from_str("2026-04-20T08:00:00.123456789Z"),
        finished_at=ts_ns_from_str("2026-04-20T08:05:00.987654321Z"),
    )

    store.upsert(initial)
    store.upsert(running)
    assert store.get("flow-run-001") == running

    store.upsert(finished)
    assert store.get("flow-run-001") == finished


def test_list_all_returns_rows_in_deterministic_order(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    flow_b = FlowRun(
        flow_run_id="flow-run-002",
        flow_name="beta-flow",
    )
    flow_a2 = FlowRun(
        flow_run_id="flow-run-003",
        flow_name="alpha-flow",
    )
    flow_a1 = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="alpha-flow",
    )

    store.upsert(flow_b)
    store.upsert(flow_a2)
    store.upsert(flow_a1)

    rows = store.list_all()

    assert rows == [flow_a1, flow_a2, flow_b]


def test_timestamps_round_trip_with_nanosecond_precision(tmp_path: Path) -> None:
    store = _make_store(tmp_path)

    flow_run = FlowRun(
        flow_run_id="flow-run-001",
        flow_name="demo-flow",
        status=RunStatus.RUNNING,
        started_at=ts_ns_from_str("2026-04-20T08:00:00.123456789Z"),
        finished_at=ts_ns_from_str("2026-04-20T08:05:00.987654321Z"),
    )

    store.upsert(flow_run)
    restored = store.get("flow-run-001")

    assert restored == flow_run
    assert restored is not None
    assert restored.started_at == ts_ns_from_str("2026-04-20T08:00:00.123456789Z")
    assert restored.finished_at == ts_ns_from_str("2026-04-20T08:05:00.987654321Z")
