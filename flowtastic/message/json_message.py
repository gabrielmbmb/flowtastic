from __future__ import annotations

try:
    import orjson as json
except ImportError:
    import json  # type: ignore

from typing import Any

from flowtastic.message.base import Message


class JSONMessage(Message):
    """A message that is encoded as JSON."""

    def _deserialize(self, message: str) -> Any:
        """Deserialize the message from a JSON string to a Python object.

        Args:
            message: The JSON string message to deserialize.

        Returns:
            The deserialized message.
        """
        return json.loads(message)
