from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class AssetDecl:
    id: str
    partition_key: str | None = None
    format: str = "parquet"
    path_template: str | None = None


@dataclass
class TaskSpec:
    name: str
    fn: Callable
    retries: int = 2
    retry_delay_s: int = 30
    params: dict[str, object] = field(default_factory=dict)
    upstream: list[str] = field(default_factory=list)
    produces: AssetDecl | None = None


@dataclass
class FlowSpec:
    name: str
    schedule_cron: str | None = None
    params: dict[str, object] = field(default_factory=dict)
    tasks: list[TaskSpec] = field(default_factory=list)
