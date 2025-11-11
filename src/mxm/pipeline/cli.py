from __future__ import annotations
import json
from typing import Optional, Dict
import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="mxm-pipeline — operator CLI for Money Ex Machina")
console = Console()

# In v0.1.0 we’ll swap these for real registry/adapters
_FAKE_FLOWS = ["justetf"]

@app.command("list")
def list_flows() -> None:
    table = Table(title="Registered Flows")
    table.add_column("Flow", style="bold")
    for f in _FAKE_FLOWS:
        table.add_row(f)
    console.print(table)

@app.command("graph")
def graph(flow: str = typer.Argument(..., help="Flow name")) -> None:
    if flow not in _FAKE_FLOWS:
        typer.echo(f"Unknown flow: {flow}", err=True)
        raise typer.Exit(code=1)
    console.print(f"[bold]DAG[/] for [italic]{flow}[/] (stub)")

@app.command("run")
def run(
    flow: str = typer.Argument(..., help="Flow name"),
    params: Optional[str] = typer.Option(None, help='JSON of params, e.g. \'{"as_of":"2025-11-11"}\''),
) -> None:
    if flow not in _FAKE_FLOWS:
        typer.echo(f"Unknown flow: {flow}", err=True)
        raise typer.Exit(code=1)
    parsed: Dict[str, object] = {}
    if params:
        parsed = json.loads(params)
    console.print(f"Running [bold]{flow}[/] with params: {parsed} (stub)")

@app.command("status")
def status(flow: str = typer.Argument(..., help="Flow name")) -> None:
    if flow not in _FAKE_FLOWS:
        typer.echo(f"Unknown flow: {flow}", err=True)
        raise typer.Exit(code=1)
    console.print(f"Status for [bold]{flow}[/]: OK (stub)")
