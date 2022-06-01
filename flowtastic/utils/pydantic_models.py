from __future__ import annotations

from typing import Any

from pydantic import BaseModel


def is_base_model_subclass(cls: Any) -> bool:
    """Check if a class is a subclass of `BaseModel`. If the `issubclass` function raises
    an exception, it means that `cls` is probably a type hint from `typing` module.

    Args:
        cls: The class to check.

    Returns:
        `True` if `cls` is a subclass of `BaseModel`, `False` otherwise.
    """
    try:
        return issubclass(cls, BaseModel)
    except Exception:
        return False
