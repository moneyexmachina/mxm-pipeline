"""
ASCII DAG utilities for mxm-pipeline.

This module renders a flow's edges as newline-separated strings in the form
`U -> V`, suitable for quick CLI visualization and test assertions.
"""

from __future__ import annotations

from mxm.pipeline.spec import FlowSpec

__all__ = ["ascii_edges"]


def ascii_edges(flow: FlowSpec) -> str:
    """
    Render the directed edges of a flow as sorted ASCII lines.

    Each task contributes one line per upstream dependency, formatted as
    `"<upstream> -> <task>"`. Tasks without upstream dependencies produce
    no lines. The output is sorted lexicographically for determinism.

    Parameters
    ----------
    flow : FlowSpec
        The flow whose task dependencies should be printed.

    Returns
    -------
    str
        Newline-separated, lexicographically sorted edges.
    """
    lines: list[str] = []
    for task in flow.tasks:
        for up in task.upstream:
            lines.append(f"{up} -> {task.name}")
    return "\n".join(sorted(lines))
