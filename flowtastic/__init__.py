from __future__ import annotations

from flowtastic.app import FlowTastic
from flowtastic.logger import configure_logger
from flowtastic.message import JSONMessage, Message, YAMLMessage
from flowtastic.publish import Publish

__version__ = "0.0.1a0"

__all__ = ["FlowTastic", "JSONMessage", "Message", "YAMLMessage", "Publish"]

configure_logger()
