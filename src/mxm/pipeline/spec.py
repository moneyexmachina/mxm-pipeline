from __future__ import annotations

from dataclasses import dataclass, field

from mxm.pipeline.types import TaskFn
from mxm.types import JSONObj

__all__ = ["FlowSpec", "TaskSpec"]


# --- typed default factories (avoid Unknown from dict/list) -----------------
def _empty_params() -> JSONObj:
    return {}


def _empty_str_list() -> list[str]:
    return []


def _empty_task_list() -> list[TaskSpec]:
    return []


# --- spec dataclasses -------------------------------------------------------
@dataclass
class TaskSpec:
    name: str
    fn: TaskFn
    retries: int = 2
    retry_delay_s: int = 30
    params: JSONObj = field(default_factory=_empty_params)
    upstream: list[str] = field(default_factory=_empty_str_list)


@dataclass
class FlowSpec:
    name: str
    schedule_cron: str | None = None
    params: JSONObj = field(default_factory=_empty_params)
    tasks: list[TaskSpec] = field(default_factory=_empty_task_list)
