from __future__ import annotations

import typer
from flowtastic.cli import local_environment

app = typer.Typer()

app.add_typer(
    local_environment.app,
    name="local-environment",
    help="manage Kafka Broker local deployment using Docker containers.",
)


def main() -> None:
    """Main entrypoint for the FlowTastic CLI."""
    app()
