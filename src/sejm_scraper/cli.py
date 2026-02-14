"""CLI entrypoint for sejm_scraper."""

import anyio
import typer

from sejm_scraper import database, pipeline

app = typer.Typer(help="Scrape Polish Sejm parliamentary data.")


@app.command()
def prepare_database() -> None:
    """Create all database tables."""
    database.create_db_and_tables()


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
) -> None:
    """Run the full scraping pipeline."""

    async def _run() -> None:
        await pipeline.pipeline(
            from_term=from_term,
            from_sitting=from_sitting,
            from_voting=from_voting,
        )

    anyio.run(_run)


@app.command()
def resume() -> None:
    """Resume scraping from the last completed point in the database."""

    async def _run() -> None:
        await pipeline.resume_pipeline()

    anyio.run(_run)
