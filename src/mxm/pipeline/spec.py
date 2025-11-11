from __future__ import annotations

from dataclasses import dataclass, field

from .types import JSONValue, TaskFn


# --- typed default factories (avoid Unknown from dict/list) -----------------
def _empty_params() -> dict[str, JSONValue]:
    return {}


def _empty_str_list() -> list[str]:
    return []


def _empty_task_list() -> list[TaskSpec]:
    return []


# --- spec dataclasses -------------------------------------------------------
@dataclass
class AssetDecl:
    id: str
    partition_key: str | None = None  # e.g. "as_of"
    format: str = "parquet"
    path_template: str | None = None  # e.g. ".../{partition}/file.parquet"


@dataclass
class TaskSpec:
    name: str
    fn: TaskFn
    retries: int = 2
    retry_delay_s: int = 30
    params: dict[str, JSONValue] = field(default_factory=_empty_params)
    upstream: list[str] = field(default_factory=_empty_str_list)
    produces: AssetDecl | None = None  # optional asset sidecar


@dataclass
class FlowSpec:
    name: str
    schedule_cron: str | None = None
    params: dict[str, JSONValue] = field(default_factory=_empty_params)
    tasks: list[TaskSpec] = field(default_factory=_empty_task_list)
