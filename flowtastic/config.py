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


class FlowTasticConfig(BaseSettings):
    """FlowTastic configuration attributes."""

    # Logging and tracebacks
    LOGGING_LEVEL: LoggingLevels = LoggingLevels.INFO

    @validator("LOGGING_LEVEL", pre=True)
    def build_logging_level(cls, v: str) -> LoggingLevels:
        return LoggingLevels(v)

    ENABLE_RICH_TRACEBACK: bool = True

    class Config:
        case_sensitive = True
        env_prefix = "FLOWTASTIC_"


_config = FlowTasticConfig()
