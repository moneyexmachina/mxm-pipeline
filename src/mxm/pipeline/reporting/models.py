from __future__ import annotations

from dataclasses import dataclass, field

from mxm.types import JSONObj
from mxm.types.timestamps import TSNSScalar


def _empty_jsonobj() -> JSONObj:
    return {}


@dataclass(frozen=True)
class SemanticEvent:
    event_id: str
    flow_run_id: str
    task_run_id: str
    event_type: str
    event_ts: TSNSScalar
    domain_key: str
    payload: JSONObj = field(default_factory=_empty_jsonobj)
