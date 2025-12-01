"""
Lightweight types and protocols for mxm-pipeline.

This module intentionally contains *only* typing constructs (aliases,
TypedDicts, Protocols) to avoid circular imports between api.py and
adapter implementations.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal, Protocol, TypedDict, runtime_checkable

from mxm.types import JSONMap, JSONObj

TaskFn = Callable[..., object]

# Extendable set of known backends. Add "dagster", "airflow", etc. later.
BackendName = Literal["prefect"]


__all__ = ["BackendName", "MXMFlow", "RunOptions", "TaskFn"]


class RunOptions(TypedDict, total=False):
    """
    Execution options passed to a compiled flow.

    Attributes
    ----------
    quiet : bool
        If True, suppress backend UI/logging where supported.
        Adapters may interpret additional options over time.
    """

    quiet: bool


@runtime_checkable
class MXMFlow(Protocol):
    """
    Opaque, compiled flow artifact produced by an adapter.

    Implementations must hide any vendor-specific types and expose a
    single `execute` method that returns a normalized task result map.
    """

    backend: BackendName
    name: str

    def execute(
        self,
        params: JSONObj | None = None,
        options: RunOptions | None = None,
    ) -> JSONMap:
        """
        Execute the compiled flow.

        Parameters
        ----------
        params : JSONObj | None
            Runtime parameters for the flow (adapter-defined semantics).
        options : RunOptions | None
            Execution options (e.g., `quiet`).

        Returns
        -------
        JSONMap
            Mapping of task-name to result value.
        """
        ...
