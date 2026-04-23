from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.types import is_ts_ns


def test_semantic_event_fields_are_assigned() -> None:
    event_ts = utc_now_ts_ns()

    event = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=event_ts,
        domain_key="demo.asset",
        payload={"rows": 42},
    )

    assert event.event_id == "event-001"
    assert event.flow_run_id == "flow-run-001"
    assert event.task_run_id == "task-run-001"
    assert event.event_type == "materialized"
    assert event.event_ts == event_ts
    assert event.domain_key == "demo.asset"
    assert event.payload == {"rows": 42}


def test_semantic_event_payload_default_is_not_shared() -> None:
    event_1 = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )
    event_2 = SemanticEvent(
        event_id="event-002",
        flow_run_id="flow-run-002",
        task_run_id="task-run-002",
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
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )

    assert is_ts_ns(event.event_ts)


def test_semantic_event_is_frozen() -> None:
    event = SemanticEvent(
        event_id="event-001",
        flow_run_id="flow-run-001",
        task_run_id="task-run-001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )

    with pytest.raises(FrozenInstanceError):
        event.domain_key = "other.asset"  # type: ignore[misc]
