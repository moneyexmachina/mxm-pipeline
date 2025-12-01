# tests/test_prefect_adapter_execute.py

from __future__ import annotations

from mxm.pipeline.adapters import prefect_adapter
from mxm.types import JSONMap, JSONObj, JSONValue


def test_execute_prefect_flow_passes_params_and_returns_result() -> None:
    captured: dict[str, JSONObj] = {}

    def fake_flow(**kwargs: JSONValue) -> JSONMap:
        captured["params"] = kwargs
        return {"ok": True}

    result = prefect_adapter.execute_prefect_flow(fake_flow, params={"x": 1})

    assert result == {"ok": True}
    assert captured["params"] == {"x": 1}
