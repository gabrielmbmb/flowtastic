from __future__ import annotations

from enum import Enum

from pydantic import BaseSettings, validator


class LoggingLevels(str, Enum):
    """Enum for the logging levels."""

    NOTSET = "NOTSET"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def build_logging_level(v: str) -> LoggingLevels:
    """Builds a logging level from a string.

    Args:
        v: The string to build the logging level from.

    Returns:
        The logging level.
    """
    return LoggingLevels(v)


class FlowTasticConfig(BaseSettings):
    """FlowTastic configuration attributes."""

    # Logging and tracebacks
    LOGGING_LEVEL: LoggingLevels = LoggingLevels.INFO
    SUPPRESS_LOGGERS_NAMES: list[str] = ["asyncio", "aiokafka"]
    SUPPRESS_LOGGERS_LEVEL: LoggingLevels = LoggingLevels.WARNING

    _build_logging_level = validator("LOGGING_LEVEL", pre=True, allow_reuse=True)(
        build_logging_level
    )
    _build_suppress_loggers_level = validator(
        "SUPPRESS_LOGGERS_LEVEL", pre=True, allow_reuse=True
    )(build_logging_level)

    ENABLE_RICH_TRACEBACK: bool = True

    class Config:
        case_sensitive = True
        env_prefix = "FLOWTASTIC_"


_config = FlowTasticConfig()
