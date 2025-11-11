from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

@dataclass
class AssetDecl:
    id: str
    partition_key: Optional[str] = None
    format: str = "parquet"
    path_template: Optional[str] = None

@dataclass
class TaskSpec:
    name: str
    fn: Callable
    retries: int = 2
    retry_delay_s: int = 30
    params: Dict[str, object] = field(default_factory=dict)
    upstream: List[str] = field(default_factory=list)
    produces: Optional[AssetDecl] = None

@dataclass
class FlowSpec:
    name: str
    schedule_cron: Optional[str] = None
    params: Dict[str, object] = field(default_factory=dict)
    tasks: List[TaskSpec] = field(default_factory=list)
