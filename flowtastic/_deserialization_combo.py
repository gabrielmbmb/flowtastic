from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowtastic.message import Message

if TYPE_CHECKING:
    from pydantic import BaseModel


class _DeserializationCombo:
    """A combination of a `Message` class and a `BaseModel` class. This class is only used
    internally by the `FlowTastic` class to compare the combination of a `Message` class
    and a `BaseModel` class.

    Attributes:
        message: The `Message` class instance.
        pydantic_base_model: The `BaseModel` class type.
    """

    def __init__(self, message: Message, pydantic_base_model: type[BaseModel]) -> None:
        """Inits `_DeserializationCombo` class.

        Args:
            message: The `Message` class instance.
            pydantic_base_model: The `BaseModel` class type.
        """
        self.message = message
        self.pydantic_base_model = pydantic_base_model

    def __eq__(self, other: Any) -> bool:
        """Compares the `_DeserializationCombo` class with another `_DeserializationCombo`.
        They are equal if the `Message` class and the `BaseModel` class are the same (type).

        Args:
            other: The other `_DeserializationCombo` class.

        Returns:
            True if the `_DeserializationCombo` classes are equal, `False` otherwise.
        """
        if isinstance(other, Message):
            return False
        if isinstance(other, _DeserializationCombo):
            return (
                self.message == other.message
                and self.pydantic_base_model == other.pydantic_base_model
            )
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash(
            (
                self.message.__class__.__name__,
                self.message.encoding,
                self.pydantic_base_model,
            )
        )
