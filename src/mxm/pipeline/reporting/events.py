from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from mxm.types import JSONObj
from mxm.types.timestamps import TSNSScalar


def _empty_jsonobj() -> JSONObj:
    return {}


class ExecutionEventType(StrEnum):
    FLOW_RUN_STARTED = "flow_run_started"
    FLOW_RUN_FINISHED = "flow_run_finished"
    TASK_RUN_CREATED = "task_run_created"
    TASK_RUN_BLOCKED = "task_run_blocked"
    TASK_RUN_SKIPPED = "task_run_skipped"
    TASK_ATTEMPT_STARTED = "task_attempt_started"
    TASK_ATTEMPT_SUCCEEDED = "task_attempt_succeeded"
    TASK_ATTEMPT_FAILED = "task_attempt_failed"
    SEMANTIC_EVENT_EMITTED = "semantic_event_emitted"


@dataclass(frozen=True)
class ExecutionEvent:
    event_id: str
    event_type: ExecutionEventType
    event_ts: TSNSScalar
    entity_type: str
    entity_id: str
    flow_run_id: str | None = None
    task_run_id: str | None = None
    task_attempt_id: str | None = None
    payload: JSONObj = field(default_factory=_empty_jsonobj)
