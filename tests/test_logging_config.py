import json
from collections.abc import Iterator

import pytest
import structlog

from sejm_scraper import logging_config


@pytest.fixture(autouse=True)
def _reset_structlog() -> Iterator[None]:
    """Restore structlog defaults so config does not leak across tests."""
    yield
    structlog.reset_defaults()


def test_json_log_format_emits_json(capsys: pytest.CaptureFixture[str]) -> None:
    logging_config.configure_logging(log_format=logging_config.LogFormat.JSON)

    structlog.get_logger().info("scraped terms", count=5)

    record = json.loads(capsys.readouterr().out.strip())
    assert record["event"] == "scraped terms"
    assert record["count"] == 5
    assert record["level"] == "info"
    assert "timestamp" in record


def test_console_log_format_is_not_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    logging_config.configure_logging(
        log_format=logging_config.LogFormat.CONSOLE
    )

    structlog.get_logger().info("scraped terms", count=5)

    out = capsys.readouterr().out
    assert "scraped terms" in out
    with pytest.raises(json.JSONDecodeError):
        json.loads(out)


def test_reconfiguring_applies_to_existing_loggers(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A logger bound before reconfiguration must pick up the new format.

    Guards against re-enabling logger caching, which would freeze the
    format chosen at the first log call.
    """
    logging_config.configure_logging(
        log_format=logging_config.LogFormat.CONSOLE
    )
    logger = structlog.get_logger()
    logger.info("before reconfigure")

    logging_config.configure_logging(log_format=logging_config.LogFormat.JSON)
    logger.info("after reconfigure", count=1)

    last_line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(last_line)
    assert record["event"] == "after reconfigure"
    assert record["count"] == 1
