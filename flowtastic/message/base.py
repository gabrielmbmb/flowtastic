from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from flowtastic.message.exceptions import DeserializationError

_DEFAULT_ENCODING = "utf-8"


class Message(ABC):
    """Base class for messages.

    Args:
        encoding: The encoding to use when decoding or encoding the message.
    """

    encoding: str

    def __init__(self, encoding: str = _DEFAULT_ENCODING) -> None:
        self.encoding = encoding

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Message):
            raise NotImplementedError
        return self.encoding == other.encoding

    def decode(self, message: bytes) -> str:
        """Decode the message using the encoding specified in the class.

        Args:
            message: The message to decode.

        Returns:
            The decoded message.
        """
        return message.decode(self.encoding)

    @abstractmethod
    def _deserialize(self, message: str) -> Any:
        pass

    def deserialize(self, message: bytes) -> Any:
        """Deserialize the message decoding the message from bytes to string using the specified
        encoding in the class. Then, it calls the `_deserialize` abstract method that should
        be implemented by the subclass to generate a Python object from the message.

        Args:
            message: The message to deserialize.

        Returns:
            The deserialized message.

        Raises:
            DeserializationError: If the message cannot be deserialized.
        """
        try:
            decode = self.decode(message)
            deserialized = self._deserialize(decode)
            return deserialized
        except ValueError as e:
            raise DeserializationError(e)
