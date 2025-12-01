"""
Demo Prefect flow for mxm-pipeline.

This module defines a simple DAG A -> {B, C} -> D using the project's
`FlowSpec` and `TaskSpec`. It is intended for CLI smoke testing and examples.
"""

from __future__ import annotations

from mxm.pipeline.spec import FlowSpec, TaskSpec

__all__ = ["build_demo_flow"]


def _task_A() -> int:
    """Compute value for task A (no inputs)."""
    # Example pure function; stable return for deterministic tests.
    return 2


def _task_B(a: int) -> int:
    """Compute B = A + 3."""
    return a + 3


def _task_C(a: int) -> int:
    """Compute C = A + 5."""
    return a + 5


def _task_D(b: int, c: int) -> int:
    """Compute D = B + C."""
    return b + c


def build_demo_flow() -> FlowSpec:
    """Build the demo flow (A -> {B, C} -> D)."""
    tasks = [
        TaskSpec(name="A", fn=_task_A, upstream=[]),
        TaskSpec(name="B", fn=_task_B, upstream=["A"]),
        TaskSpec(name="C", fn=_task_C, upstream=["A"]),
        TaskSpec(name="D", fn=_task_D, upstream=["B", "C"]),
    ]
    return FlowSpec(name="demo", tasks=tasks)
