# tests/test_utils_clock.py

from __future__ import annotations

from mxm.pipeline.utils.clock import utc_now_ts_ns
from mxm.types.timestamps import is_nat, is_ts_ns


def test_utc_now_ts_ns_returns_canonical_timestamp() -> None:
    ts = utc_now_ts_ns()

    assert is_ts_ns(ts)
    assert not is_nat(ts)


def test_utc_now_ts_ns_is_monotonic_non_decreasing() -> None:
    t1 = utc_now_ts_ns()
    t2 = utc_now_ts_ns()

    assert t2 >= t1
