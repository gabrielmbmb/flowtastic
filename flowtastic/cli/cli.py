from __future__ import annotations

import typer
from flowtastic import __version__
from flowtastic.cli import local_environment

app = typer.Typer(add_completion=False, name="FlowTastic")

app.add_typer(
    local_environment.app,
    name="local-environment",
    help="Manage Kafka Broker local deployment using Docker containers.",
)


def version_callback(value: bool) -> None:
    """Prints FlowTastic version and exits."""
    if value:
        typer.echo(f"flowtastic v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Prints the FlowTastic version and exit.",
    )
) -> None:
    ...
