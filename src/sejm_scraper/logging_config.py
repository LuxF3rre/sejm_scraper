"""Structured logging configuration for sejm_scraper."""

import logging
from enum import StrEnum

import structlog


class LogFormat(StrEnum):
    """Rendering format for log output."""

    CONSOLE = "console"
    JSON = "json"


def configure_logging(*, log_format: LogFormat = LogFormat.CONSOLE) -> None:
    """Configure how structlog renders log records.

    Args:
        log_format: ``console`` for human-readable, coloured output (the
            default) or ``json`` for one JSON object per line, suitable
            for log aggregation when running the pipeline unattended.
    """
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer()
        if log_format is LogFormat.JSON
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        # Do not cache bound loggers: loggers obtained via get_logger()
        # rebind on every call, so calling configure_logging() always takes
        # effect immediately regardless of when it runs relative to the
        # first log call. The log volume here makes caching irrelevant.
        cache_logger_on_first_use=False,
    )
