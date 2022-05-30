from __future__ import annotations

import logging

from flowtastic.config import LoggingLevels, _config
from rich.traceback import install as install_rich_traceback


def get_logging_level() -> str:
    """Returns the logging level."""
    return _config.LOGGING_LEVEL.value


def create_console_handler() -> logging.StreamHandler:  # type: ignore
    """Creates a console handler for the logger.

    Returns:
        The console handler for the logger.
    """
    console_handler = logging.StreamHandler()
    console_handler.setLevel(get_logging_level())
    return console_handler


def configure_root_logger() -> None:
    """Configures the root logger."""
    logging.basicConfig(level=get_logging_level())


def configure_error_tracebacks() -> None:
    """Configures `rich` error tracebacks."""
    install_rich_traceback(show_locals=_config.LOGGING_LEVEL == LoggingLevels.DEBUG)


def configure_logger() -> None:
    """Creates the app logger, sets the level and adds the console handler."""
    configure_root_logger()
    if _config.ENABLE_RICH_TRACEBACK:
        configure_error_tracebacks()


def get_logger(name: str) -> logging.Logger:
    """Creates a logger with the given name.

    Args:
        name: The name of the logger.

    Returns:
        The logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(get_logging_level())
    logger.addHandler(create_console_handler())
    return logger
