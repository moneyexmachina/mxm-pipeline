from __future__ import annotations

import os as _os

# Disable Prefect's console logging/UI/API globally for test & local runs
_os.environ.setdefault("PREFECT_LOGGING_TO_CONSOLE", "false")
_os.environ.setdefault("PREFECT_UI_ENABLED", "false")
_os.environ.setdefault("PREFECT_API_ENABLE", "false")
import inspect
import logging
from typing import (
    Any,
    Protocol,
    TypeVar,
    cast,
    runtime_checkable,
)
from collections.abc import Callable, Iterable, Mapping

from mxm.pipeline.spec import AssetDecl, FlowSpec, TaskSpec
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


def _index_tasks(tasks: Iterable[TaskSpec]) -> dict[str, TaskSpec]:
    by_name: dict[str, TaskSpec] = {}
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
    indeg: dict[str, int] = {n: 0 for n in by_name}
    children: dict[str, set[str]] = {n: set() for n in by_name}

    for t in by_name.values():
        for u in t.upstream:
            indeg[t.name] += 1
            children[u].add(t.name)

    frontier: set[str] = {n for n, d in indeg.items() if d == 0}
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


@runtime_checkable
class _TaskDecorator(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Callable[[F], F]: ...


def _require_prefect_task() -> _TaskDecorator:
    try:
        from prefect import task  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Prefect adapter requires the 'orchestration' extra. "
            "Install with: `poetry install -E orchestration`."
        ) from exc
    return task  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Small utilities


def _toposort(by_name: Mapping[str, TaskSpec]) -> list[str]:
    """Return a deterministic topological order or raise on cycle."""
    # indegree and children graph
    indeg: dict[str, int] = {n: 0 for n in by_name}
    children: dict[str, set[str]] = {n: set() for n in by_name}
    for t in by_name.values():
        for u in t.upstream:
            indeg[t.name] += 1
            children[u].add(t.name)

    # stable order: process frontier in sorted name order
    frontier: list[str] = sorted(n for n, d in indeg.items() if d == 0)
    order: list[str] = []

    while frontier:
        n = frontier.pop(0)
        order.append(n)
        for v in sorted(children[n]):
            indeg[v] -= 1
            if indeg[v] == 0:
                frontier.append(v)

    if len(order) != len(by_name):
        raise ValueError("Cycle detected in task dependencies")
    return order


def _merge_params(
    task: TaskSpec, flow: FlowSpec, runtime: Mapping[str, JSONValue]
) -> dict[str, JSONValue]:
    """Precedence: runtime > flow.params > task.params."""
    merged: dict[str, JSONValue] = {}
    merged.update(task.params or {})
    merged.update(flow.params or {})
    merged.update(dict(runtime))
    return merged


def _filter_kwargs(
    fn: Callable[..., Any], kwargs: Mapping[str, JSONValue]
) -> dict[str, JSONValue]:
    """Only pass kwargs that the function can accept (avoids unexpected keyword errors)."""
    sig = inspect.signature(fn)
    accepted: dict[str, JSONValue] = {}
    for name, val in kwargs.items():
        p = sig.parameters.get(name)
        if p is None:
            continue
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY, p.VAR_KEYWORD):
            accepted[name] = val
    return accepted


def _log_asset_write(
    logger: logging.Logger, asset: AssetDecl, merged_params: Mapping[str, JSONValue]
) -> None:
    part_k = asset.partition_key
    part_repr = ""
    if part_k is not None:
        v = merged_params.get(part_k, None)
        if v is not None:
            part_repr = f" partition={part_k}:{v}"
    logger.info(f"ASSET write {asset.id}{part_repr}")


def _resolve_value(maybe_future: Any) -> Any:
    """Prefect returns plain values in flows, but if we ever get a future, resolve it."""
    # Simple duck-typing: PrefectFuture has .result()
    res_attr = getattr(maybe_future, "result", None)
    if callable(res_attr):
        try:
            return res_attr()  # type: ignore[misc]
        except Exception:
            # If not a Prefect future or failed, just return as-is and let caller raise later
            return maybe_future
    return maybe_future


# --- Logging silencer for Prefect/Rich console --------------------------------
from contextlib import contextmanager


@contextmanager
def _silence_prefect_console() -> Any:
    """
    Temporarily disable Prefect's console handlers (Rich) to avoid writes to
    closed files under pytest capture. Restores handlers afterwards.
    """
    targets = [
        logging.getLogger("prefect"),
        logging.getLogger("prefect.flow_runs"),
        logging.getLogger("prefect.task_runs"),
        logging.getLogger("prefect.server"),
        logging.getLogger("prefect.server.api.server"),
    ]
    saved: list[tuple[logging.Logger, list[logging.Handler], int, bool]] = []
    try:
        for lg in targets:
            saved.append((lg, list(lg.handlers), lg.level, lg.propagate))
            lg.handlers = [logging.NullHandler()]
            lg.setLevel(logging.ERROR)
            lg.propagate = False
        yield
    finally:
        for lg, handlers, level, propagate in saved:
            lg.handlers = handlers
            lg.setLevel(level)
            lg.propagate = propagate


# --- Public API --------------------------------------------------------------


def build_prefect_flow(flow_spec: FlowSpec) -> Callable[..., dict[str, Any]]:
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
    def _mxm_flow(**params: JSONValue) -> dict[str, Any]:
        # Part A: build-only; return empty mapping to keep tests simple.
        _ = params
        return {}

    return _mxm_flow


def run_prefect_flow(
    flow_spec: FlowSpec,
    params: Mapping[str, JSONValue],
) -> dict[str, Any]:
    # Validate spec (dup/unknown/cycle)
    by_name_validate = _index_tasks(flow_spec.tasks)
    _validate_dependencies(by_name_validate)

    # Require Prefect and get decorators (this is what your test monkeypatches)
    flow_dec = _require_prefect_flow()
    task_dec = _require_prefect_task()

    # Quiet Prefect’s API/UI + reduce logging noise (best-effort)
    try:
        from prefect.settings import (
            PREFECT_API_ENABLE,
            PREFECT_LOGGING_LEVEL,
            PREFECT_LOGGING_TO_CONSOLE,
            PREFECT_UI_ENABLED,
            temporary_settings,
        )

        settings_ctx = temporary_settings(
            {
                PREFECT_API_ENABLE: False,
                PREFECT_UI_ENABLED: False,
                PREFECT_LOGGING_LEVEL: "WARNING",
                PREFECT_LOGGING_TO_CONSOLE: False,
            }
        )

    except Exception:

        class _NullCtx:
            def __enter__(self):  # type: ignore[no-redef]
                return None

            def __exit__(self, *args: object) -> None:
                return None

        settings_ctx = _NullCtx()  # type: ignore[assignment]

    logger = logging.getLogger("mxm.pipeline.adapters.prefect")

    by_name: dict[str, TaskSpec] = {t.name: t for t in flow_spec.tasks}
    order = _toposort(by_name)

    wrapped: dict[str, Callable[..., Any]] = {}
    for name in order:
        spec = by_name[name]
        wrapped[name] = task_dec(
            name=spec.name,
            retries=max(0, int(spec.retries)),
            retry_delay_seconds=max(0, int(spec.retry_delay_s)),
        )(spec.fn)  # type: ignore[misc]

    @flow_dec(name=flow_spec.name)
    def _mxm_run(**runtime_params: dict[str, JSONValue]) -> dict[str, Any]:
        results: dict[str, Any] = {}
        for name in order:
            spec = by_name[name]
            pos_args: list[Any] = (
                [results[u] for u in spec.upstream] if spec.upstream else []
            )
            merged = _merge_params(spec, flow_spec, runtime_params)
            call_kwargs = _filter_kwargs(spec.fn, merged)
            out = wrapped[name](*pos_args, **call_kwargs)
            results[name] = _resolve_value(out)
            if spec.produces is not None:
                _log_asset_write(logger, spec.produces, merged)
        return results

    run_fn: Callable[..., dict[str, Any]] = cast(
        Callable[..., dict[str, Any]], _mxm_run
    )
    with settings_ctx, _silence_prefect_console():
        return run_fn(**dict(params))
