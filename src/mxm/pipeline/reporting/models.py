from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from mxm.types import JSONObj
from mxm.types.timestamps import TSNSScalar


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class FlowRun:
    flow_run_id: str
    flow_name: str
    status: RunStatus = RunStatus.PENDING
    started_at: TSNSScalar | None = None
    finished_at: TSNSScalar | None = None


@dataclass(frozen=True)
class TaskRun:
    task_run_id: str
    flow_run_id: str
    task_name: str
    status: RunStatus = RunStatus.PENDING
    started_at: TSNSScalar | None = None
    finished_at: TSNSScalar | None = None
    attempt_count: int = 0


@dataclass(frozen=True)
class TaskAttempt:
    task_attempt_id: str
    task_run_id: str
    attempt_number: int
    status: RunStatus = RunStatus.PENDING
    started_at: TSNSScalar | None = None
    finished_at: TSNSScalar | None = None
    error_summary: str | None = None
    log_ref: str | None = None


def _empty_jsonobj() -> JSONObj:
    return {}


@dataclass(frozen=True)
class SemanticEvent:
    event_id: str
    task_attempt_id: str
    event_type: str
    event_ts: TSNSScalar
    domain_key: str
    payload: JSONObj = field(default_factory=_empty_jsonobj)
