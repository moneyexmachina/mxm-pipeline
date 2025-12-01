"""
mxm-pipeline CLI (M2.1): demo registry + Prefect-local execution.

Commands
--------
- list   : print available flows
- graph  : print DAG edges
- run    : execute the flow locally and print results

Global options
--------------
--format {plain,rich,json}  Select output format (default: plain)
--quiet / --no-quiet        Suppress Prefect API/UI/console logging (default: --quiet)
--config PATH               (Reserved) Path to mxm-config file (future use)

Exit codes
----------
0 = success
1 = runtime error (only for `run`)
2 = unknown flow
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Annotated, Literal

import typer

from mxm.pipeline.api import compile_flow, execute_flow
from mxm.pipeline.graph_ascii import ascii_edges
from mxm.pipeline.registry import get_flow, get_flows
from mxm.pipeline.spec import FlowSpec

try:
    # Rich is optional for plain/json modes; only used when --format=rich
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
except Exception:  # pragma: no cover - rich is expected in dev, but guard anyway
    Console = None  # type: ignore[assignment]
    Panel = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]

app = typer.Typer(help="mxm-pipeline — operator CLI (demo registry; Prefect local)")

Format = Literal["plain", "rich", "json"]


def _parse_params(param_kv: list[str]) -> dict[str, str]:
    """
    Parse repeated --param k=v options into a dict (last write wins).
    """
    out: dict[str, str] = {}
    for raw in param_kv:
        if "=" not in raw:
            raise ValueError(f"--param requires k=v, got: {raw!r}")
        k, v = raw.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Empty key in --param {raw!r}")
        out[k] = v
    return out


def _edges_of_flow(spec: FlowSpec) -> list[tuple[str, str]]:
    """
    Compute edge list [(u, v), ...] from a FlowSpec.
    """
    edges: list[tuple[str, str]] = []
    for task in spec.tasks:
        for up in task.upstream:
            edges.append((up, task.name))
    edges.sort(key=lambda e: (e[0], e[1]))
    return edges


@app.callback()
def _main_options(  # pyright: ignore[reportUnusedFunction]
    ctx: typer.Context,
    format: Annotated[
        Format,
        typer.Option(
            "--format",
            "-f",
            case_sensitive=False,
            help="Output format: plain, rich, or json (default: plain).",
        ),
    ] = "plain",
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet/--no-quiet",
            help="Suppress Prefect API/UI/logging (default: --quiet).",
        ),
    ] = True,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            "-c",
            help="(Reserved) Path to mxm-config file for defaults (future use).",
        ),
    ] = None,
) -> None:
    """
    Capture global CLI options and stash in the Typer context.
    """
    ctx.obj = {"format": format, "quiet": quiet, "config": config}


@app.command("list")
def list_flows(ctx: typer.Context) -> None:
    """
    List available flows.
    """
    fmt: Format = ctx.obj["format"]
    flows = sorted(get_flows().keys())

    if fmt == "json":
        typer.echo(json.dumps({"flows": flows}, separators=(",", ":")))
        return

    if fmt == "rich":
        if Console is None or Table is None:
            typer.echo("\n".join(flows))
            return
        console = Console()
        table = Table(title="Registered Flows")
        table.add_column("Flow", style="bold")
        for name in flows:
            table.add_row(name)
        console.print(table)
        return

    # plain
    for name in flows:
        typer.echo(name)


@app.command("graph")
def graph(
    ctx: typer.Context, flow: str = typer.Argument(..., help="Flow name")
) -> None:
    """
    Print flow DAG edges.
    - plain : newline-separated 'U -> V'
    - rich  : panel containing the same text
    - json  : { "flow": ..., "edges": [{"u": "...", "v": "..."}] }
    """
    fmt: Format = ctx.obj["format"]
    try:
        spec = get_flow(flow)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2)  # noqa: B904

    if fmt == "json":
        edges_json = [{"u": u, "v": v} for (u, v) in _edges_of_flow(spec)]
        typer.echo(
            json.dumps({"flow": spec.name, "edges": edges_json}, separators=(",", ":"))
        )
        return

    text = ascii_edges(spec)

    if fmt == "rich":
        if Console is None or Panel is None:
            typer.echo(text)
            return
        console = Console()
        console.print(Panel.fit(text, title=f"DAG: {spec.name}", border_style="cyan"))
        return

    # plain
    typer.echo(text)


@app.command("run")
def run(
    ctx: typer.Context,
    flow: Annotated[str, typer.Argument(..., help="Flow name")],
    param: Annotated[
        list[str] | None,
        typer.Option(
            "--param",
            "-p",
            help="Flow parameter in k=v form (repeatable; later overrides earlier).",
        ),
    ] = None,
) -> None:
    """
    Execute the flow locally via the Prefect adapter.

    Output by format:
    - plain : minified JSON result on stdout
    - rich  : pretty JSON result
    - json  : minified JSON result (identical content to plain)

    Exit codes:
    - 0 on success
    - 1 on runtime error
    - 2 if the flow is unknown
    """
    fmt: Format = ctx.obj["format"]

    try:
        spec = get_flow(flow)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2)  # noqa: B904

    try:
        params = _parse_params(param or [])
        mxm_flow = compile_flow(spec)
        result = execute_flow(mxm_flow, params=params)

        if fmt == "json":
            typer.echo(json.dumps(result, separators=(",", ":")))
            return

        if fmt == "rich" and Console is not None:
            console = Console()
            console.print_json(data=result)
            return

        # plain
        typer.echo(json.dumps(result, separators=(",", ":")))
    except Exception as exc:
        typer.echo(f"Runtime error: {exc}", err=True)
        raise typer.Exit(code=1)  # noqa: B904


def main(argv: list[str] | None = None) -> int:
    """
    Entry point wrapper that returns an exit code (useful for script wiring/tests).
    """
    try:
        app(standalone_mode=False)
        return 0
    except typer.Exit as e:
        return e.exit_code
    except Exception as exc:  # Safety net
        typer.echo(f"Unexpected error: {exc}", err=True)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
