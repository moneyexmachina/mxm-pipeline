from __future__ import annotations

from mxm.pipeline.reporting.models import RunStatus
from mxm.pipeline.utils.serde import (
    json_from_sql,
    json_to_sql,
    status_from_sql,
    status_to_sql,
    ts_from_sql,
    ts_to_sql,
)
from mxm.types import JSONMap
from mxm.types.timestamps import ts_ns_from_str


def test_ts_sql_round_trip() -> None:
    ts = ts_ns_from_str("2026-04-20T12:34:56.123456789Z")

    stored = ts_to_sql(ts)
    restored = ts_from_sql(stored)

    assert stored == "2026-04-20T12:34:56.123456789Z"
    assert restored == ts


def test_ts_sql_none_round_trip() -> None:
    assert ts_to_sql(None) is None
    assert ts_from_sql(None) is None


def test_status_sql_round_trip() -> None:
    status = RunStatus.RUNNING

    stored = status_to_sql(status)
    restored = status_from_sql(stored)

    assert stored == "running"
    assert restored is RunStatus.RUNNING


def test_json_sql_round_trip() -> None:
    payload: JSONMap = {
        "b": 2,
        "a": {"y": 2, "x": 1},
    }

    stored = json_to_sql(payload)
    restored = json_from_sql(stored)

    assert restored == payload


def test_json_to_sql_is_canonical_and_compact() -> None:
    payload = {
        "b": 2,
        "a": 1,
    }

    stored = json_to_sql(payload)

    assert stored == '{"a":1,"b":2}'


def test_json_from_sql_rejects_non_object_top_level() -> None:
    try:
        json_from_sql('["not","an","object"]')
    except ValueError as exc:
        assert "Expected JSON object at top level." in str(exc)
    else:
        raise AssertionError("Expected ValueError for non-object JSON.")
