from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def common(a: list[T], b: list[T]) -> list[T]:
    """Returns the common elements of two lists.

    Args:
        a: The first list.
        b: The second list.

    Returns:
        The common elements of the two lists.
    """
    return [x for x in a if x in b]
