from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional, Union

from pydantic import BaseModel

SubscriberFunc = Callable[
    [Union[BaseModel, dict[str, Any]]],
    Awaitable[Optional[Union[BaseModel, dict[str, Any]]]],
]
