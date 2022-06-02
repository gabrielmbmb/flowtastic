from __future__ import annotations

from typing import Any, Awaitable, Callable, Union

from flowtastic.message import DeserializationError
from flowtastic.publish import Publish
from pydantic import BaseModel, ValidationError

SubscriberFunc = Callable[
    [Union[BaseModel, dict[str, Any]]], Awaitable[Union[Publish, None]]
]
"""The signature of the function that is decorated using `flowtastic.FlowTastic.subscriber`
decorator."""

DeserializationErrorFunc = Callable[[DeserializationError], Awaitable[None]]
"""The signature of the function that will get executed when a `flowtastic.message.DeserializationError`
occurs."""

ValidationErrorFunc = Callable[[ValidationError], Awaitable[None]]
"""The signature of the function that will get executed when a `pydantic.ValidationError`
occurs."""
