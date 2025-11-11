from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Protocol,
    Set,
    TypeVar,
    runtime_checkable,
)

from mxm.pipeline.spec import FlowSpec, TaskSpec
from mxm.pipeline.types import JSONValue

__all__ = ["build_prefect_flow", "run_prefect_flow"]


# --- Minimal typing for Prefect's @flow decorator ----------------------------

F = TypeVar("F", bound=Callable[..., Any])


@runtime_checkable
class _FlowDecorator(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Callable[[F], F]: ...


def _require_prefect_flow() -> _FlowDecorator:
    """
    Import guard that returns a callable compatible with Prefect's `@flow`.
    Kept narrow so we don't pull Prefect types into our public surface.
    """
    try:
        from prefect import flow  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Prefect adapter requires the 'orchestration' extra. "
            "Install with: `poetry install -E orchestration`."
        ) from exc
    # We trust Prefect to satisfy the protocol at runtime.
    return flow  # type: ignore[return-value]


# --- Build-time validation helpers ------------------------------------------


def _index_tasks(tasks: Iterable[TaskSpec]) -> Dict[str, TaskSpec]:
    by_name: Dict[str, TaskSpec] = {}
    for t in tasks:
        if t.name in by_name:
            raise ValueError(f"Duplicate task name: {t.name!r}")
        by_name[t.name] = t
    return by_name


def _validate_dependencies(by_name: Mapping[str, TaskSpec]) -> None:
    # Unknown upstreams
    for t in by_name.values():
        for u in t.upstream:
            if u not in by_name:
                raise ValueError(
                    f"Unknown upstream {u!r} referenced by task {t.name!r}"
                )

    # Cycle detection (Kahn's algorithm)
    indeg: Dict[str, int] = {n: 0 for n in by_name}
    children: Dict[str, Set[str]] = {n: set() for n in by_name}

    for t in by_name.values():
        for u in t.upstream:
            indeg[t.name] += 1
            children[u].add(t.name)

    frontier: Set[str] = {n for n, d in indeg.items() if d == 0}
    removed = 0

    while frontier:
        n = frontier.pop()
        removed += 1
        for v in list(children[n]):
            indeg[v] -= 1
            if indeg[v] == 0:
                frontier.add(v)

    if removed != len(by_name):
        raise ValueError("Cycle detected in task dependencies")


# --- Public API --------------------------------------------------------------


def build_prefect_flow(flow_spec: FlowSpec) -> Callable[..., Dict[str, Any]]:
    """
    Build and return a Prefect @flow callable from a FlowSpec.

    Validates (at build time):
      - duplicate task names
      - unknown upstream references
      - dependency cycles

    This function does NOT execute any user task code; it only prepares
    a Prefect flow wrapper. Execution wiring is added in run_prefect_flow.
    """
    flow = _require_prefect_flow()  # helpful error if Prefect missing
    by_name = _index_tasks(flow_spec.tasks)  # duplicate names
    _validate_dependencies(by_name)  # unknowns + cycles

    @flow(name=flow_spec.name)
    def _mxm_flow(**params: JSONValue) -> Dict[str, Any]:
        # Part A: build-only; return empty mapping to keep tests simple.
        _ = params
        return {}

    return _mxm_flow


def run_prefect_flow(
    flow_spec: FlowSpec,
    params: Mapping[str, JSONValue],
) -> Dict[str, Any]:
    """
    Placeholder for Part B. For now it simply builds and calls the flow.
    Returns the flow's result mapping (empty for Part A).
    """
    flow_fn = build_prefect_flow(flow_spec)
    return flow_fn(**dict(params))
