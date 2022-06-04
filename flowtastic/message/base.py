from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from flowtastic.message.exceptions import DeserializationError

if TYPE_CHECKING:
    from pydantic import BaseModel


_DEFAULT_ENCODING = "utf-8"


class Message(ABC):
    """Base class for messages that can be serialized and deserialized.

    Args:
        encoding: The encoding to use when decoding or encoding the message.
    """

    encoding: str

    def __init__(self, encoding: str = _DEFAULT_ENCODING) -> None:
        """Inits `Message` class.

        Args:
            encoding: The encoding to use when decoding or encoding the message.
        """
        self.encoding = encoding

    def __eq__(self, other: Any) -> bool:
        """Compares the `Message` class with another `Message` class. They are equal if the
        encoding is the same.

        Args:
            other: The other `Message` class.

        Return:
            `True` if the `Message` classes are equal, `False` otherwise.
        """
        if isinstance(other, Message):
            return self.encoding == other.encoding
        raise NotImplementedError

    def __hash__(self) -> int:
        """Returns the hash of the `Message` class."""
        return hash(self.encoding)

    def decode(self, message: bytes) -> str:
        """Decode the message using the encoding specified in the class.

        Args:
            message: The message to decode.

        Returns:
            The decoded message.
        """
        return message.decode(self.encoding)

    @abstractmethod
    def _serialize(self, python_object: BaseModel | Any) -> str:
        """Serializes `python_object` to string. This method should be implemented by the
        subclass to serialize a Python object to a string."""
        ...

    def serialize(self, python_object: BaseModel | Any) -> bytes:
        """Serializes `python_object` to bytes using the specified encoding in the class.

        Args:
            python_object: The Python object to serialize.

        Returns:
            The serialized object.
        """
        return self._serialize(python_object).encode(self.encoding)

    @abstractmethod
    def _deserialize(self, message: str) -> Any:
        """Deserialize the message. This method should be implemented by the subclass to
        generate a Python object from the message."""
        ...

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
