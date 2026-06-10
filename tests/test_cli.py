from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

from sejm_scraper import cli, pipeline

runner = CliRunner()


def test_prepare_database_creates_db_file(tmp_path: Path) -> None:
    db_file = tmp_path / "test.duckdb"

    result = runner.invoke(
        cli.app, ["prepare-database", "--db-path", str(db_file)]
    )

    assert result.exit_code == 0
    assert db_file.exists()


def test_scrape_invokes_pipeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mock_pipeline = AsyncMock()
    monkeypatch.setattr(pipeline, "pipeline", mock_pipeline)
    db_file = tmp_path / "test.duckdb"

    result = runner.invoke(
        cli.app,
        [
            "scrape",
            "--from-term",
            "10",
            "--from-sitting",
            "39",
            "--from-voting",
            "205",
            "--db-path",
            str(db_file),
        ],
    )

    assert result.exit_code == 0
    mock_pipeline.assert_called_once()
    call_kwargs = mock_pipeline.call_args.kwargs
    assert call_kwargs["from_term"] == 10
    assert call_kwargs["from_sitting"] == 39
    assert call_kwargs["from_voting"] == 205
    assert call_kwargs["engine"] is not None


def test_resume_invokes_resume_pipeline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mock_resume = AsyncMock()
    monkeypatch.setattr(pipeline, "resume_pipeline", mock_resume)
    db_file = tmp_path / "test.duckdb"

    result = runner.invoke(cli.app, ["resume", "--db-path", str(db_file)])

    assert result.exit_code == 0
    mock_resume.assert_called_once()
    assert mock_resume.call_args.kwargs["engine"] is not None
