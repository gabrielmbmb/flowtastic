from __future__ import annotations

from flowtastic import FlowTastic


def test_flowtastic_init() -> None:
    """Tests `flowtastic.FlowTastic.__init__` method."""
    app = FlowTastic(name="unit-tests", broker="localhost:9092")
    assert app.name == "unit-tests"
    assert app.broker == "localhost:9092"
