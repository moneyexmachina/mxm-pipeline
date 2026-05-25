from __future__ import annotations

import os as _os
from contextlib import contextmanager

# Disable Prefect's console logging/UI/API globally for test & local runs
_os.environ.setdefault("PREFECT_LOGGING_TO_CONSOLE", "false")
_os.environ.setdefault("PREFECT_UI_ENABLED", "false")
_os.environ.setdefault("PREFECT_API_ENABLE", "false")

import inspect
import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Protocol, TypeVar, runtime_checkable

from mxm.pipeline.execution.context import ExecutionContext, SemanticEventSink
from mxm.pipeline.reporting.layout import ReportingLayout
from mxm.pipeline.reporting.sinks import ReportingSemanticEventSink
from mxm.pipeline.reporting.sqlite.backend import SQLiteBackend
from mxm.pipeline.reporting.stores import SemanticEventsStore
from mxm.pipeline.spec import FlowSpec, TaskSpec
from mxm.pipeline.types import BackendName, MXMFlow, RunOptions
from mxm.types import JSONMap, JSONObj, JSONValue

__all__ = [
    "PrefectMXMFlow",
    "build_mxm_flow_for_prefect",
]


# --- Minimal typing for Prefect decorators -----------------------------------

F = TypeVar("F", bound=Callable[..., Any])


@runtime_checkable
class _FlowDecorator(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Callable[[F], F]: ...


@runtime_checkable
class _TaskDecorator(Protocol):
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
    return flow  # type: ignore[return-value]


def _require_prefect_task() -> _TaskDecorator:
    try:
        from prefect import task  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Prefect adapter requires the 'orchestration' extra. "
            "Install with: `poetry install -E orchestration`."
        ) from exc
    return task  # type: ignore[return-value]


# --- Build-time validation helpers -------------------------------------------
def _accepts_execution_context(fn: Callable[..., object]) -> bool:
    sig = inspect.signature(fn)
    param = sig.parameters.get("execution_context")
    if param is None:
        return False
    return param.kind in (
        param.POSITIONAL_OR_KEYWORD,
        param.KEYWORD_ONLY,
    )


def _index_tasks(tasks: Iterable[TaskSpec]) -> dict[str, TaskSpec]:
    by_name: dict[str, TaskSpec] = {}
    for t in tasks:
        if t.name in by_name:
            raise ValueError(f"Duplicate task name: {t.name!r}")
        by_name[t.name] = t
    return by_name


def _validate_dependencies(by_name: Mapping[str, TaskSpec]) -> None:
    for t in by_name.values():
        for u in t.upstream:
            if u not in by_name:
                raise ValueError(
                    f"Unknown upstream {u!r} referenced by task {t.name!r}"
                )

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


# --- Small utilities ---------------------------------------------------------


def _toposort(by_name: Mapping[str, TaskSpec]) -> list[str]:
    """Return a deterministic topological order or raise on cycle."""
    indeg: dict[str, int] = {n: 0 for n in by_name}
    children: dict[str, set[str]] = {n: set() for n in by_name}
    for t in by_name.values():
        for u in t.upstream:
            indeg[t.name] += 1
            children[u].add(t.name)

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
    task: TaskSpec,
    flow: FlowSpec,
    runtime: JSONObj,
) -> JSONMap:
    """Precedence: runtime > flow.params > task.params."""
    merged: JSONMap = {}
    merged.update(task.params or {})
    merged.update(flow.params or {})
    merged.update(runtime)
    return merged


def _filter_kwargs(
    fn: Callable[..., Any],
    kwargs: JSONObj,
) -> JSONMap:
    """
    Only pass kwargs that the function can accept.
    This keeps `execution_context` injection optional.
    """
    sig = inspect.signature(fn)
    accepted: JSONObj = {}
    for name, val in kwargs.items():
        p = sig.parameters.get(name)
        if p is None:
            continue
        if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY, p.VAR_KEYWORD):
            accepted[name] = val
    return accepted


def _resolve_value(maybe_future: Any) -> Any:
    """
    Prefect returns plain values in flows, but if we ever get a future, resolve it.
    """
    res_attr = getattr(maybe_future, "result", None)
    if callable(res_attr):
        try:
            return res_attr()  # type: ignore[misc]
        except Exception:
            return maybe_future
    return maybe_future


def _build_execution_context(
    *,
    flow_name: str,
    task_name: str,
    semantic_event_sink: SemanticEventSink,
) -> ExecutionContext:
    try:
        from prefect.runtime import flow_run, task_run  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Prefect runtime context is unavailable.") from exc

    flow_run_id = getattr(flow_run, "id", None)
    task_run_id = getattr(task_run, "id", None)

    if flow_run_id is None:
        raise RuntimeError("Missing Prefect flow_run.id in runtime context.")
    if task_run_id is None:
        raise RuntimeError("Missing Prefect task_run.id in runtime context.")

    logger = logging.getLogger(f"mxm.pipeline.prefect.{flow_name}.{task_name}")

    return ExecutionContext(
        flow_run_id=str(flow_run_id),
        task_run_id=str(task_run_id),
        flow_name=flow_name,
        task_name=task_name,
        logger=logger,
        semantic_event_sink=semantic_event_sink,
    )


def _call_task_with_optional_execution_context(
    fn: Callable[..., object],
    args: tuple[object, ...],
    kwargs: JSONObj,
    execution_context: ExecutionContext,
) -> object:
    call_kwargs = _filter_kwargs(fn, kwargs)

    if _accepts_execution_context(fn):
        return fn(*args, **call_kwargs, execution_context=execution_context)

    return fn(*args, **call_kwargs)


def _make_prefect_task_runtime_wrapper(
    *,
    flow_name: str,
    task_spec: TaskSpec,
    semantic_event_sink: SemanticEventSink,
) -> Callable[..., Any]:
    def _runtime_wrapper(*args: object, **kwargs: JSONValue) -> object:
        execution_context = _build_execution_context(
            flow_name=flow_name,
            task_name=task_spec.name,
            semantic_event_sink=semantic_event_sink,
        )

        return _call_task_with_optional_execution_context(
            fn=task_spec.fn,
            args=args,
            kwargs=dict(kwargs),
            execution_context=execution_context,
        )

    _runtime_wrapper.__name__ = task_spec.fn.__name__
    _runtime_wrapper.__qualname__ = task_spec.fn.__qualname__
    _runtime_wrapper.__doc__ = task_spec.fn.__doc__

    return _runtime_wrapper


# --- Logging silencer for Prefect/Rich console -------------------------------


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


class PrefectMXMFlow:
    backend: BackendName = "prefect"

    def __init__(self, name: str, pf_flow: Callable[..., JSONMap]):
        self.name = name
        self._pf_flow = pf_flow

    def execute(
        self,
        params: JSONObj | None = None,
        options: RunOptions | None = None,
    ) -> JSONMap:
        _ = options
        return execute_prefect_flow(self._pf_flow, params=params)


def build_mxm_flow_for_prefect(
    flow_spec: FlowSpec, reporting_layout: ReportingLayout
) -> MXMFlow:
    pf_flow = build_prefect_flow(flow_spec, reporting_layout)
    return PrefectMXMFlow(name=flow_spec.name, pf_flow=pf_flow)


def execute_mxm_flow_for_prefect(
    flow: MXMFlow,
    params: JSONObj | None = None,
    options: RunOptions | None = None,
) -> JSONMap:
    return flow.execute(params=params, options=options)


def run_mxm_flow_for_prefect(
    flow_spec: FlowSpec,
    reporting_layout: ReportingLayout,
    params: JSONObj | None = None,
    options: RunOptions | None = None,
) -> JSONMap:
    flow = build_mxm_flow_for_prefect(flow_spec, reporting_layout)
    return flow.execute(params=params, options=options)


def build_prefect_flow(
    flow_spec: FlowSpec, reporting_layout: ReportingLayout
) -> Callable[..., JSONMap]:
    """
    Build and return a Prefect @flow callable from a FlowSpec.

    This callable, when invoked with runtime params, will:
    - run tasks in topological order
    - merge task/flow/runtime params
    - resolve any futures
    - inject MXM ExecutionContext into tasks that accept `execution_context`

    It does *not* apply any global Prefect settings or logging silencing;
    that is handled by `execute_prefect_flow`.
    """
    backend = SQLiteBackend(layout=reporting_layout)
    semantic_events_store = SemanticEventsStore(backend=backend)
    semantic_event_sink = ReportingSemanticEventSink(store=semantic_events_store)
    flow_dec = _require_prefect_flow()
    task_dec = _require_prefect_task()

    by_name = _index_tasks(flow_spec.tasks)
    _validate_dependencies(by_name)

    by_name_map: dict[str, TaskSpec] = {t.name: t for t in flow_spec.tasks}
    order = _toposort(by_name_map)

    wrapped: dict[str, Callable[..., Any]] = {}
    for name in order:
        spec = by_name_map[name]
        runtime_fn = _make_prefect_task_runtime_wrapper(
            flow_name=flow_spec.name,
            task_spec=spec,
            semantic_event_sink=semantic_event_sink,
        )
        wrapped[name] = task_dec(
            name=spec.name,
            retries=max(0, int(spec.retries)),
            retry_delay_seconds=max(0, int(spec.retry_delay_s)),
        )(runtime_fn)

    @flow_dec(name=flow_spec.name)
    def _mxm_run(**runtime_params: JSONValue) -> JSONMap:
        results: JSONMap = {}
        runtime: JSONObj = runtime_params
        for name in order:
            spec = by_name_map[name]
            pos_args: list[Any] = (
                [results[u] for u in spec.upstream] if spec.upstream else []
            )
            merged = _merge_params(spec, flow_spec, runtime)
            call_kwargs = _filter_kwargs(spec.fn, merged)
            out = wrapped[name](*pos_args, **call_kwargs)
            results[name] = _resolve_value(out)
        return results

    return _mxm_run


def execute_prefect_flow(
    pf_flow: Callable[..., JSONMap],
    params: JSONObj | None = None,
) -> JSONMap:
    """
    Execute a Prefect @flow callable under MXM's default local settings.

    - Disables Prefect API/UI where possible
    - Reduces console logging
    - Silences noisy loggers for pytest/local runs
    """
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

    with settings_ctx, _silence_prefect_console():
        return pf_flow(**dict(params or {}))
