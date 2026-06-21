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
