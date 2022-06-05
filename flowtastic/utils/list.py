from __future__ import annotations

from typing import List, TypeVar

T = TypeVar("T")


def common(a: List[T], b: List[T]) -> List[T]:
    """Returns the common elements of two lists.

    Args:
        a: The first list.
        b: The second list.

    Returns:
        The common elements of the two lists.
    """
    return [x for x in a if x in b]
