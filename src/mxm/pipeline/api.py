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

from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.spec import FlowSpec
from mxm.pipeline.types import BackendName, MXMFlow, RunOptions
from mxm.types import JSONMap, JSONObj

__all__ = ["BackendName", "MXMFlow", "RunOptions", "compile_flow", "execute_flow"]


def compile_flow(
    spec: FlowSpec,
    *,
    backend: BackendName = "prefect",
    reporting_layout: ReportingLayout,
) -> MXMFlow:
    """
    Compile a declarative FlowSpec into an executable flow.

    Parameters
    ----------
    spec : FlowSpec
    backend : BackendName
    reporting_layout : ReportingLayout
        Defines where semantic events are persisted.

    Returns
    -------
    MXMFlow
    """
    if backend == "prefect":
        from mxm.pipeline.adapters.prefect_adapter import build_mxm_flow_for_prefect

        return build_mxm_flow_for_prefect(
            spec,
            reporting_layout=reporting_layout,
        )

    raise ValueError(f"Unsupported backend: {backend!r}")


def execute_flow(
    flow: MXMFlow,
    params: JSONObj | None = None,
    options: RunOptions | None = None,
) -> JSONMap:
    return flow.execute(params=params or {}, options=options or {})
