"""
Public API for compiling and executing MXM flows.

This module defines a small, backend-agnostic façade used by callers
(e.g., CLI, tests) to turn a declarative :class:`FlowSpec` into an
executable object and run it without importing vendor types (Prefect, etc.).

Layering:
- mxm.pipeline.spec   : Declarative specs (TaskSpec, FlowSpec, ...)
- mxm.pipeline.api    : Public façade (this module)
- mxm.pipeline.adapters.* : Backend implementations (e.g., Prefect)
"""

from __future__ import annotations

from mxm.pipeline.spec import FlowSpec
from mxm.pipeline.types import BackendName, MXMFlow, RunOptions
from mxm.types import JSONMap, JSONObj

__all__ = ["BackendName", "MXMFlow", "RunOptions", "compile_flow", "execute_flow"]


def compile_flow(
    spec: FlowSpec,
    backend: BackendName = "prefect",
) -> MXMFlow:
    """
    Compile a declarative :class:`FlowSpec` into an executable flow.

    This function dispatches to the selected adapter and returns an
    :class:`MXMFlow` that can be executed without importing vendor types.

    Parameters
    ----------
    spec : FlowSpec
        Declarative flow description (backend-agnostic).
    backend : BackendName, default "prefect"
        Target execution backend.

    Returns
    -------
    MXMFlow
        Compiled, executable flow object.

    Raises
    ------
    ValueError
        If the backend is unknown or unsupported.
    """
    if backend == "prefect":
        # Lazy import to avoid pulling adapter/vendor deps unless needed.
        from mxm.pipeline.adapters.prefect_adapter import build_mxm_flow_for_prefect

        return build_mxm_flow_for_prefect(spec)

    raise ValueError(f"Unsupported backend: {backend!r}")


def execute_flow(
    flow: MXMFlow,
    params: JSONObj | None = None,
    options: RunOptions | None = None,
) -> JSONMap:
    """
    Execute a compiled :class:`MXMFlow`.

    This is a thin convenience wrapper that forwards to `flow.execute`.
    It normalizes `None` inputs to empty dicts.

    Parameters
    ----------
    flow : MXMFlow
        Compiled flow produced by :func:`compile_flow`.
    params : JSONObj | None
        Runtime parameters for the flow (optional).
    options : RunOptions | None
        Execution options (e.g., `quiet`) (optional).

    Returns
    -------
    JSONMap
        Mapping of task-name to result value.
    """
    return flow.execute(params=params or {}, options=options or {})
