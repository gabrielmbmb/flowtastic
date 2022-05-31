from __future__ import annotations

from flowtastic.message.base import Message
from flowtastic.message.exceptions import DeserializationError
from flowtastic.message.json_message import JSONMessage

__all__ = ["Message", "DeserializationError", "JSONMessage"]
