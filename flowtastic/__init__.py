from __future__ import annotations

from flowtastic.app import FlowTastic
from flowtastic.logger import configure_logger
from flowtastic.message import JSONMessage, Message

__version__ = "0.0.1"

__all__ = [
    "FlowTastic",
    "JSONMessage",
    "Message",
]

configure_logger()
