from __future__ import annotations

try:
    import yaml
except ImportError:
    raise ImportError(
        "YAMLMessage requires the PyYAML package. Please install it using 'pip install flowtastic[yaml]'."
    )

try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader, Dumper  # type: ignore

from typing import Any

from flowtastic.message.base import Message
from pydantic import BaseModel


class YAMLMessage(Message):
    """A message that is encoded as YAML."""

    def _serialize(self, python_object: BaseModel | Any) -> str:
        """Serialize the message from a Python object to a YAML string.

        Args:
            python_object: The Python object to serialize.

        Returns:
            The serialized message.
        """
        if isinstance(python_object, BaseModel):
            return yaml.dump(python_object.dict(), Dumper=Dumper)  # type: ignore
        return yaml.dump(python_object, Dumper=Dumper)  # type: ignore

    def _deserialize(self, message: str) -> Any:
        """Deserialize the message from a YAML string to a Python object.

        Args:
            message: The YAML string message to deserialize.

        Returns:
            The deserialized message.
        """
        return yaml.load(message, Loader=Loader)
