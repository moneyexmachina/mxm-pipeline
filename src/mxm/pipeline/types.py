from __future__ import annotations

from collections.abc import Callable

JSONScalar = str | int | float | bool | None

type JSONValue = JSONScalar | list[JSONValue] | dict[str, JSONValue]

TaskFn = Callable[..., object]

__all__ = ["JSONScalar", "JSONValue", "TaskFn"]
