from __future__ import annotations

from pathlib import Path

from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import SemanticEventsStore
from mxm.types import JSONMap
from mxm.types.timestamps import ts_ns_from_str


def _make_store(tmp_path: Path) -> SemanticEventsStore:
    layout = ReportingLayout(root=tmp_path)
    backend = SQLiteBackend(layout=layout)
    return SemanticEventsStore(backend=backend)


def test_append_and_get_round_trip(tmp_path: Path) -> None:
    semantic_events = _make_store(tmp_path)

    payload: JSONMap = {
        "rows": 123,
        "status": "ok",
    }

    event = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.123456789Z"),
        domain_key="demo.asset",
        payload=payload,
    )

    semantic_events.append(event)
    restored = semantic_events.get("event-001")

    assert restored == event


def test_get_missing_returns_none(tmp_path: Path) -> None:
    semantic_events = _make_store(tmp_path)

    assert semantic_events.get("missing-event") is None


def test_append_is_idempotent_by_event_id(tmp_path: Path) -> None:
    semantic_events = _make_store(tmp_path)

    event = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.123456789Z"),
        domain_key="demo.asset",
        payload={},
    )

    semantic_events.append(event)
    semantic_events.append(event)

    rows = semantic_events.list_for_task_run("task-run-001")

    assert rows == [event]


def test_list_for_task_run_returns_only_matching_rows(tmp_path: Path) -> None:
    semantic_events = _make_store(tmp_path)

    event_1 = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000001Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2 = SemanticEvent(
        event_id="event-002",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="validated",
        event_ts=ts_ns_from_str("2026-04-20T12:01:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_3 = SemanticEvent(
        event_id="event-003",
        flow_run_id="flow-run-001",
        task_run_id="task-run-002",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:02:00.000000003Z"),
        domain_key="other.asset",
        payload={},
    )

    semantic_events.append(event_1)
    semantic_events.append(event_2)
    semantic_events.append(event_3)

    rows = semantic_events.list_for_task_run("task-run-001")

    assert rows == [event_1, event_2]


def test_list_for_task_run_returns_rows_in_deterministic_order(
    tmp_path: Path,
) -> None:
    semantic_events = _make_store(tmp_path)

    event_3 = SemanticEvent(
        event_id="event-003",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="published",
        event_ts=ts_ns_from_str("2026-04-20T12:05:00.000000003Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2b = SemanticEvent(
        event_id="event-002b",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="validated",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2a = SemanticEvent(
        event_id="event-002a",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_1 = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="started",
        event_ts=ts_ns_from_str("2026-04-20T11:59:59.999999999Z"),
        domain_key="demo.asset",
        payload={},
    )

    semantic_events.append(event_3)
    semantic_events.append(event_2b)
    semantic_events.append(event_2a)
    semantic_events.append(event_1)

    rows = semantic_events.list_for_task_run("task-run-001")

    assert rows == [event_1, event_2a, event_2b, event_3]


def test_list_for_flow_run_returns_only_matching_rows(tmp_path: Path) -> None:
    semantic_events = _make_store(tmp_path)

    event_1 = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.000000001Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_2 = SemanticEvent(
        event_id="event-002",
        flow_run_id="flow-run-001",
        task_run_id="task-run-002",
        event_type="validated",
        event_ts=ts_ns_from_str("2026-04-20T12:01:00.000000002Z"),
        domain_key="demo.asset",
        payload={},
    )
    event_3 = SemanticEvent(
        event_id="event-003",
        flow_run_id="flow-run-002",
        task_run_id="task-run-003",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:02:00.000000003Z"),
        domain_key="other.asset",
        payload={},
    )

    semantic_events.append(event_1)
    semantic_events.append(event_2)
    semantic_events.append(event_3)

    rows = semantic_events.list_for_flow_run("flow-run-001")

    assert rows == [event_1, event_2]


def test_domain_key_and_nested_payload_round_trip(tmp_path: Path) -> None:
    semantic_events = _make_store(tmp_path)

    payload: JSONMap = {
        "outer": {
            "rows": 123,
            "ok": True,
        },
        "name": "demo",
    }

    event = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=ts_ns_from_str("2026-04-20T12:00:00.123456789Z"),
        domain_key="demo.asset",
        payload=payload,
    )

    semantic_events.append(event)
    restored = semantic_events.get("event-001")

    assert restored == event
    assert restored is not None
    assert restored.flow_run_id == "flow-run-001"
    assert restored.task_run_id == "task-run-001"
    assert restored.domain_key == "demo.asset"
    assert restored.payload == payload
