from __future__ import annotations

try:
    import orjson as json
except ImportError:
    import json  # type: ignore

from typing import Any

from flowtastic.message.base import Message
from pydantic import BaseModel


class JSONMessage(Message):
    """A message that is encoded as JSON."""

    def _serialize(self, python_object: BaseModel | Any) -> str:
        """Serialize the message from a Python object to a JSON string.

        Args:
            python_object: The Python object to serialize.

        Returns:
            The serialized message.
        """
        if isinstance(python_object, BaseModel):
            return python_object.json()
        return json.dumps(python_object)  # type: ignore

    def _deserialize(self, message: str) -> Any:
        """Deserialize the message from a JSON string to a Python object.

        Args:
            message: The JSON string message to deserialize.

        Returns:
            The deserialized message.
        """
        return json.loads(message)
