from __future__ import annotations

from dataclasses import dataclass

from mxm.pipeline.execution.context import SemanticEventSink
from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.reporting.stores import SemanticEventsStore


@dataclass(frozen=True)
class ReportingSemanticEventSink(SemanticEventSink):
    store: SemanticEventsStore

    def emit(self, event: SemanticEvent) -> None:
        self.store.append(event)
