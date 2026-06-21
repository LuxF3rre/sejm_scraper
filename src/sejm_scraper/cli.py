"""CLI entrypoint for sejm_scraper."""

import anyio
import typer
from sqlalchemy import Engine

from sejm_scraper import database, logging_config, pipeline

app = typer.Typer(help="Scrape Polish Sejm parliamentary data.")

DEFAULT_DB_PATH = "sejm_scraper.duckdb"

_DB_PATH_HELP = "Path to the DuckDB database file."
_LOG_FORMAT_HELP = (
    "Log output format: 'console' for human-readable output or "
    "'json' for one JSON object per line."
)
_DEFAULT_LOG_FORMAT = logging_config.LogFormat.CONSOLE


def _engine_from_path(db_path: str) -> Engine:
    return database.get_engine(url=f"duckdb:///{db_path}")


@app.callback()
def main(
    *,
    log_format: logging_config.LogFormat = typer.Option(
        _DEFAULT_LOG_FORMAT, help=_LOG_FORMAT_HELP
    ),
) -> None:
    """Scrape Polish Sejm parliamentary data."""
    logging_config.configure_logging(log_format=log_format)


@app.command()
def prepare_database(
    *,
    db_path: str = typer.Option(DEFAULT_DB_PATH, help=_DB_PATH_HELP),
) -> None:
    """Create all database tables."""
    database.create_db_and_tables(engine=_engine_from_path(db_path))


@app.command()
def scrape(
    *,
    from_term: int | None = typer.Option(
        None, help="Start from this term number."
    ),
    from_sitting: int | None = typer.Option(
        None, help="Start from this sitting number (requires --from-term)."
    ),
    from_voting: int | None = typer.Option(
        None,
        help=(
            "Start from this voting number "
            "(requires --from-term and --from-sitting)."
        ),
    ),
    db_path: str = typer.Option(DEFAULT_DB_PATH, help=_DB_PATH_HELP),
) -> None:
    """Run the full scraping pipeline."""

    async def _run() -> None:
        await pipeline.pipeline(
            engine=_engine_from_path(db_path),
            from_term=from_term,
            from_sitting=from_sitting,
            from_voting=from_voting,
        )

    anyio.run(_run)


@app.command()
def resume(
    *,
    db_path: str = typer.Option(DEFAULT_DB_PATH, help=_DB_PATH_HELP),
) -> None:
    """Resume scraping from the last completed point in the database."""

    async def _run() -> None:
        await pipeline.resume_pipeline(engine=_engine_from_path(db_path))

    anyio.run(_run)
