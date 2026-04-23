from __future__ import annotations

from dataclasses import FrozenInstanceError
import logging

import pytest

from mxm.pipeline.execution.context import (
    ExecutionContext,
    InMemorySemanticEventSink,
)
from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.types.timestamps import assert_ts_ns, is_ts_ns


def _make_context(
    *,
    sink: InMemorySemanticEventSink | None = None,
) -> tuple[ExecutionContext, InMemorySemanticEventSink]:
    actual_sink = sink or InMemorySemanticEventSink()
    ctx = ExecutionContext(
        flow_run_id="flowrun_001",
        task_run_id="taskrun_001",
        flow_name="demo-flow",
        task_name="demo-task",
        logger=logging.getLogger("mxm.pipeline.test"),
        semantic_event_sink=actual_sink,
    )
    return ctx, actual_sink


def test_emit_semantic_event_creates_and_returns_event() -> None:
    ctx, _ = _make_context()

    event = ctx.emit_semantic_event(
        event_type="materialized",
        domain_key="demo.asset",
        payload={"rows": 123},
    )

    assert isinstance(event, SemanticEvent)
    assert isinstance(event.event_id, str)
    assert event.event_id != ""
    assert event.flow_run_id == "flowrun_001"
    assert event.task_run_id == "taskrun_001"
    assert event.event_type == "materialized"
    assert event.domain_key == "demo.asset"
    assert event.payload == {"rows": 123}
    assert is_ts_ns(event.event_ts)
    assert_ts_ns(event.event_ts)


def test_emit_semantic_event_appends_to_sink() -> None:
    ctx, sink = _make_context()

    event = ctx.emit_semantic_event(
        event_type="observed",
        domain_key="demo.asset",
        payload={"ok": True},
    )

    assert len(sink.events) == 1
    assert sink.events[0] == event


def test_emit_semantic_event_multiple_calls_append_in_order() -> None:
    ctx, sink = _make_context()

    event_1 = ctx.emit_semantic_event(
        event_type="started",
        domain_key="demo.asset",
    )
    event_2 = ctx.emit_semantic_event(
        event_type="finished",
        domain_key="demo.asset",
        payload={"status": "ok"},
    )

    assert len(sink.events) == 2
    assert sink.events[0] == event_1
    assert sink.events[1] == event_2
    assert event_1.event_id != event_2.event_id


def test_emit_semantic_event_default_payload_is_empty_dict() -> None:
    ctx, sink = _make_context()

    event_1 = ctx.emit_semantic_event(
        event_type="materialized",
        domain_key="demo.asset",
    )
    event_2 = ctx.emit_semantic_event(
        event_type="observed",
        domain_key="demo.asset",
    )

    assert event_1.payload == {}
    assert event_2.payload == {}
    assert event_1.payload is not event_2.payload
    assert sink.events[0].payload is not sink.events[1].payload


def test_execution_context_metadata_default_is_not_shared() -> None:
    ctx_1, _ = _make_context()
    ctx_2, _ = _make_context()

    assert ctx_1.metadata == {}
    assert ctx_2.metadata == {}
    assert ctx_1.metadata is not ctx_2.metadata


def test_execution_context_is_frozen() -> None:
    ctx, _ = _make_context()

    with pytest.raises(FrozenInstanceError):
        ctx.task_name = "other-task"  # type: ignore[misc]


def test_in_memory_semantic_event_sink_appends_events() -> None:
    sink = InMemorySemanticEventSink()

    event_1 = SemanticEvent(
        event_id="semevt_001",
        flow_run_id="flowrun_001",
        task_run_id="taskrun_001",
        event_type="materialized",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )
    event_2 = SemanticEvent(
        event_id="semevt_002",
        flow_run_id="flowrun_001",
        task_run_id="taskrun_001",
        event_type="observed",
        event_ts=utc_now_ts_ns(),
        domain_key="demo.asset",
    )

    sink.emit(event_1)
    sink.emit(event_2)

    assert sink.events == [event_1, event_2]
