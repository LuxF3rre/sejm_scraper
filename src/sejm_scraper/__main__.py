import typer

from sejm_scraper import database, pipeline

app = typer.Typer()


@app.command()
def start_pipeline(
    from_term: int = typer.Option(
        None, help="Start from a specific term number"
    ),
    from_sitting: int = typer.Option(
        None, help="Start from a specific sitting number"
    ),
    from_voting: int = typer.Option(
        None, help="Start from a specific voting number"
    ),
):
    """
    Start the scraping pipeline to collect data from the Sejm API.
    """
    pipeline.start_pipeline(from_term, from_sitting, from_voting)


@app.command()
def resume_pipeline():
    """
    Resume the scraping pipeline to collect data from the Sejm API.
    """
    pipeline.resume_pipeline()


@app.command()
def prepare_database():
    """
    Prepare the database for the scraping pipeline.
    """
    database.create_db_and_tables()


if __name__ == "__main__":
    app()
