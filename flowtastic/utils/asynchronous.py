from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable


def create_gathering(
    funcs: list[Callable[..., Awaitable[Any]]],
    wrapper_func: Callable[..., Awaitable[None]] | None = None,
    *args: Any,
    **kwargs: Any,
) -> asyncio.futures.Future:  # type: ignore
    """Create a gathering from a list of coroutines. The functions are called with the provided
    arguments and keyword arguments.

    Args:
        funcs: List of coroutines.
        wrapper_func: Wrapper function that will wrap each of the coroutines in `funcs`.
        *args: Positional arguments to pass to the coroutines.
        **kwargs: Keyword arguments to pass to the coroutines.

    Returns:
        A gathering coroutine.
    """
    if wrapper_func is not None:
        return asyncio.gather(*[wrapper_func(func, *args, **kwargs) for func in funcs])
    return asyncio.gather(*[func(*args, **kwargs) for func in funcs])
