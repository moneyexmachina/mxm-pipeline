from __future__ import annotations

from dataclasses import dataclass

from mxm.pipeline.execution.context import SemanticEventSink
from mxm.pipeline.reporting.models import SemanticEvent
from mxm.pipeline.reporting.recorder import ReportingRecorder


@dataclass(frozen=True)
class ReportingSemanticEventSink(SemanticEventSink):
    recorder: ReportingRecorder

    def emit(self, event: SemanticEvent) -> None:
        self.recorder.record_semantic_event(event)
