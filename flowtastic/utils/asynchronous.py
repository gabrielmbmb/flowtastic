from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable


def create_gathering(
    funcs: list[Callable[..., Awaitable[Any]]], *args: Any, **kwargs: Any
) -> asyncio.futures.Future:  # type: ignore
    """Create a gathering from a list of coroutines. The functions are called with the provided
    arguments and keyword arguments.

    Args:
        funcs: List of coroutines.
        *args: Positional arguments to pass to the coroutines.
        **kwargs: Keyword arguments to pass to the coroutines.

    Returns:
        A gathering coroutine.
    """
    return asyncio.gather(*[func(*args, **kwargs) for func in funcs])
