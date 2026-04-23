from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Protocol, runtime_checkable

from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.pipeline.utils.ids import new_semantic_event_id
from mxm.types import JSONObj


def _empty_jsonobj() -> JSONObj:
    return {}


@runtime_checkable
class SemanticEventSink(Protocol):
    def emit(self, event: SemanticEvent) -> None: ...


def _empty_semantic_events() -> list[SemanticEvent]:
    return []


@dataclass
class InMemorySemanticEventSink:
    events: list[SemanticEvent] = field(default_factory=_empty_semantic_events)

    def emit(self, event: SemanticEvent) -> None:
        self.events.append(event)


@dataclass(frozen=True)
class ExecutionContext:
    flow_run_id: str
    task_run_id: str
    flow_name: str
    task_name: str
    logger: logging.Logger
    semantic_event_sink: SemanticEventSink
    metadata: JSONObj = field(default_factory=_empty_jsonobj)

    def emit_semantic_event(
        self,
        *,
        event_type: str,
        domain_key: str,
        payload: JSONObj | None = None,
    ) -> SemanticEvent:
        event_payload = _empty_jsonobj() if payload is None else payload
        event = SemanticEvent(
            event_id=new_semantic_event_id(),
            flow_run_id=self.flow_run_id,
            task_run_id=self.task_run_id,
            event_type=event_type,
            event_ts=utc_now_ts_ns(),
            domain_key=domain_key,
            payload=event_payload,
        )
        self.semantic_event_sink.emit(event)
        return event
