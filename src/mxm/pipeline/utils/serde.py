from __future__ import annotations

import json
from typing import cast

from mxm.types import JSONObj
from mxm.types.timestamps import TSNSScalar, ts_ns_from_str, ts_ns_to_str


def ts_to_sql(value: TSNSScalar | None) -> str | None:
    """
    Convert a canonical MXM timestamp scalar to SQLite text.

    SQLite persistence uses the canonical MXM UTC string format:

        YYYY-MM-DDTHH:MM:SS.fffffffffZ
    """
    if value is None:
        return None
    return ts_ns_to_str(value)


def ts_from_sql(value: str | None) -> TSNSScalar | None:
    """
    Parse SQLite timestamp text into a canonical MXM timestamp scalar.
    """
    if value is None:
        return None
    return ts_ns_from_str(value)


def json_to_sql(value: JSONObj) -> str:
    """
    Serialize a JSON object to canonical compact JSON text for SQLite persistence.
    """
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def json_from_sql(value: str) -> JSONObj:
    """
    Parse canonical JSON text from SQLite into a JSON object.
    """
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("Expected JSON object at top level.")
    return cast(JSONObj, parsed)
