import asyncio
import itertools
import os # Added import
from typing import Annotated, Union, Optional, List, Any, Dict

import httpx
import typer
from loguru import logger

from sejm_scraper import api_client as api
from sejm_scraper import process, schemas, utils, database # Modified import
from sejm_scraper.database import get_duckdb_connection, create_tables_if_not_exists


app = typer.Typer()

@app.callback()
def main_callback(
    ctx: typer.Context,
    db_path: Annotated[Optional[str], typer.Option(
        help="Path to the DuckDB database file. Overrides SEJM_SCRAPER_DUCKDB_FILE env var.",
        envvar="SEJM_SCRAPER_DUCKDB_FILE", # Typer can also read from env var, but we give CLI precedence
    )] = None,
):
    """
    Manage Sejm Scraper.
    The database path can be set using the --db-path option or the SEJM_SCRAPER_DUCKDB_FILE environment variable.
    CLI option takes precedence.
    """
    # database.DUCKDB_FILE is initialized in database.py using os.getenv
    # If db_path is provided via CLI, it overrides any value (from env var or default)
    if db_path:
        database.DUCKDB_FILE = db_path
        logger.info(f"Using DuckDB database file (from --db-path): {database.DUCKDB_FILE}")
    elif "SEJM_SCRAPER_DUCKDB_FILE" in os.environ:
        # If --db-path is not used, but env var is set, database.py already handled it.
        # We log it here for clarity.
        logger.info(f"Using DuckDB database file (from SEJM_SCRAPER_DUCKDB_FILE): {database.DUCKDB_FILE}")
    else:
        # If neither --db-path nor env var is set, the default from database.py is used.
        logger.info(f"Using default DuckDB database file: {database.DUCKDB_FILE}")


@app.command()
def prepare_database() -> None:
    logger.info("Preparing database...")
    conn = None
    try:
        conn = get_duckdb_connection()
        create_tables_if_not_exists(conn)
        logger.info("Database prepared successfully.")
    except Exception as e:
        logger.error(f"Error preparing database: {e}")
    finally:
        if conn:
            conn.close()


async def _fetch_and_process_votings_for_sittings(
    client: httpx.AsyncClient,
    term_schema: schemas.TermSchema,
    sittings_batch: List[schemas.SittingSchema],
    conn: Any, # DuckDB connection
    mp_id_lookup: Dict[int, str], # in_term_id -> mp_id for the current term
    chunk_size: int,
    semaphore: asyncio.Semaphore,
    from_voting_num: Optional[int],
):
    processed_votings_data = []
    processed_voting_options_data = []
    processed_votes_data = []

    voting_api_tasks = []
    sitting_map = {} # To map task result back to sitting

    for sitting_schema in sittings_batch:
        # Apply from_voting filter at the sitting level if applicable
        # This means we might fetch votings for a sitting even if only some are needed
        # More granular filtering is done after fetching.
        task = asyncio.ensure_future(
            api.get_votings(client, term_schema.number, sitting_schema.number)
        )
        voting_api_tasks.append(task)
        sitting_map[task] = sitting_schema # Associate task with its sitting

    # Gather votings for the batch of sittings
    votings_results_for_sittings_batch = []
    for i in range(0, len(voting_api_tasks), int(semaphore_value)): # Respect semaphore for gather
        tasks_chunk = voting_api_tasks[i:i+int(semaphore_value)]
        async with semaphore: # Ensure overall concurrency control
             votings_results_for_sittings_batch.extend(await asyncio.gather(*tasks_chunk))


    votes_api_tasks = []
    voting_map = {} # To map task result back to (term, sitting, voting)

    for i, raw_votings_for_sitting_list in enumerate(votings_results_for_sittings_batch):
        sitting_schema = sitting_map[voting_api_tasks[i]] # Retrieve corresponding sitting

        current_votings_to_process = raw_votings_for_sitting_list
        if from_voting_num is not None and term_schema.number == current_from_term_num and sitting_schema.number == current_from_sitting_num:
            current_votings_to_process = [v for v in current_votings_to_process if v.number >= from_voting_num]
        
        if not current_votings_to_process:
            logger.info(f"No votings to process for sitting {sitting_schema.number} in term {term_schema.number} after filtering.")
            continue

        for voting_schema in current_votings_to_process:
            processed_votings_data.append(
                process.process_voting(voting_schema, term_schema, sitting_schema)
            )
            
            # Voting Options
            current_voting_options = voting_schema.voting_options
            if current_voting_options is None:
                current_voting_options = [schemas.VotingOptionSchema(optionIndex=1, description=None)]
            
            for vo_schema in current_voting_options:
                processed_voting_options_data.append(
                    process.process_voting_option(vo_schema, term_schema, sitting_schema, voting_schema)
                )

            # Prepare to fetch votes for this voting
            task = asyncio.ensure_future(
                api.get_votes(client, term_schema.number, sitting_schema.number, voting_schema.number)
            )
            votes_api_tasks.append(task)
            voting_map[task] = (term_schema, sitting_schema, voting_schema)

    # Gather all votes for all relevant votings
    all_raw_votes_data = []
    for i in range(0, len(votes_api_tasks), int(semaphore_value)): # Respect semaphore
        tasks_chunk = votes_api_tasks[i:i+int(semaphore_value)]
        async with semaphore: # Ensure overall concurrency control
            all_raw_votes_data.extend(await asyncio.gather(*tasks_chunk))

    for i, raw_votes_schema in enumerate(all_raw_votes_data): # schemas.VotingWithMpVotesSchema
        term_s, sitting_s, voting_s = voting_map[votes_api_tasks[i]] # Retrieve context
        if not raw_votes_schema.mp_votes:
            logger.warning(f"No MP votes data for term {term_s.number}, sitting {sitting_s.number}, voting {voting_s.number}")
            continue
        for mp_vote_schema in raw_votes_schema.mp_votes:
            mp_id = mp_id_lookup.get(mp_vote_schema.mp_term_id)
            if mp_id is None:
                logger.error(
                    f"Could not find mp_id for mp_term_id {mp_vote_schema.mp_term_id} "
                    f"in term {term_s.number}. Skipping vote."
                )
                continue
            
            vote_records = process.process_vote(
                term_s, sitting_s, voting_s, mp_vote_schema, mp_id
            )
            processed_votes_data.extend(vote_records)

    # Bulk insert collected data for this batch of sittings
    if processed_votings_data:
        process.bulk_insert_data(conn, "Votings", processed_votings_data, process.VOTINGS_COLUMN_ORDER, chunk_size)
    if processed_voting_options_data:
        process.bulk_insert_data(conn, "VotingOptions", processed_voting_options_data, process.VOTING_OPTIONS_COLUMN_ORDER, chunk_size)
    if processed_votes_data:
        process.bulk_insert_data(conn, "Votes", processed_votes_data, process.VOTES_COLUMN_ORDER, chunk_size)


# Store from_point details globally for access in helper functions if needed, or pass them down.
# For simplicity here, making them global within the module for scrape.
# A class-based approach or passing context object would be cleaner for larger apps.
current_from_term_num: Optional[int] = None
current_from_sitting_num: Optional[int] = None
current_from_voting_num: Optional[int] = None
semaphore_value: int = 10 # Default, will be updated by scrape function parameter


@app.command()
async def scrape(
    from_point: Annotated[
        Union[str, None],
        typer.Option(
            help="In form of term[,sitting[,voting]], e.g. 10,13,35; 10,13; 10"
        ),
    ] = None,
    parallelism_limit: Annotated[int, typer.Option(help="Max concurrent API calls.")] = 10,
    chunk_size: Annotated[int, typer.Option(help="Rows per bulk insert chunk.")] = 100,
) -> None:
    global current_from_term_num, current_from_sitting_num, current_from_voting_num, semaphore_value
    semaphore_value = parallelism_limit
    semaphore = asyncio.Semaphore(parallelism_limit)

    if from_point is not None:
        from_elements = [int(x) for x in from_point.split(",")]
        fill = 3 - len(from_elements)
        parsed_from_elements = from_elements + [None] * fill
        current_from_term_num, current_from_sitting_num, current_from_voting_num = parsed_from_elements
        logger.info(f"Starting scrape from: Term={current_from_term_num}, Sitting={current_from_sitting_num}, Voting={current_from_voting_num}")
    else:
        current_from_term_num, current_from_sitting_num, current_from_voting_num = None, None, None
        logger.info("Starting scrape from the beginning.")

    client = httpx.AsyncClient(timeout=30.0)
    conn = get_duckdb_connection()

    try:
        # TERMS (Processed sequentially as there aren't many)
        logger.info("Fetching terms...")
        raw_terms = await api.get_terms(client)
        if not raw_terms:
            logger.warning("No terms data found.")
            return

        terms_to_process = raw_terms
        if current_from_term_num is not None:
            terms_to_process = [term for term in terms_to_process if term.number >= current_from_term_num]
        
        if not terms_to_process:
            logger.info("No terms to process after filtering by from_point.")
            return

        processed_terms_data = [process.process_term(t) for t in terms_to_process]
        process.bulk_insert_data(conn, "Terms", processed_terms_data, process.TERMS_COLUMN_ORDER, chunk_size)
        logger.info(f"Processed and inserted {len(processed_terms_data)} terms.")

        for term_schema in terms_to_process:
            term_id = utils.get_term_nk(term_schema)
            logger.info(f"Processing term {term_schema.number} (ID: {term_id})...")

            # MPs for the current term
            logger.info(f"Fetching MPs for term {term_schema.number}...")
            async with semaphore: # Limit concurrency for get_mps
                raw_mps = await api.get_mps(client, term_schema.number)
            if not raw_mps:
                logger.warning(f"No MPs data for term {term_schema.number}")
            else:
                processed_mps_data = [process.process_mp(mp) for mp in raw_mps] # Pass term_schema if needed by process_mp
                processed_mp_links_data = [process.process_mp_to_term_link(mp, term_schema) for mp in raw_mps]
                process.bulk_insert_data(conn, "MPs", processed_mps_data, process.MPS_COLUMN_ORDER, chunk_size)
                process.bulk_insert_data(conn, "MpToTermLink", processed_mp_links_data, process.MP_TO_TERM_LINK_COLUMN_ORDER, chunk_size)
                logger.info(f"Processed and inserted {len(processed_mps_data)} MPs and links for term {term_schema.number}.")

            # Create mp_id_lookup for this term (in_term_id -> mp_id)
            mp_id_lookup_rows = conn.execute(
                f"SELECT in_term_id, mp_id FROM MpToTermLink WHERE term_id = ?", (term_id,)
            ).fetchall()
            mp_id_lookup = {row[0]: row[1] for row in mp_id_lookup_rows}
            logger.debug(f"Created mp_id_lookup for term {term_schema.number} with {len(mp_id_lookup)} entries.")

            # SITTINGS for the current term
            logger.info(f"Fetching sittings for term {term_schema.number}...")
            async with semaphore: # Limit concurrency for get_sittings
                 raw_sittings_for_term = await api.get_sittings(client, term_schema.number)
            
            if not raw_sittings_for_term:
                logger.warning(f"No sittings data for term {term_schema.number}")
                continue

            sittings_to_process = [s for s in raw_sittings_for_term if s.number != 0] # Exclude planned sittings
            if current_from_sitting_num is not None and term_schema.number == current_from_term_num:
                sittings_to_process = [s for s in sittings_to_process if s.number >= current_from_sitting_num]

            if not sittings_to_process:
                logger.info(f"No sittings to process for term {term_schema.number} after filtering.")
                continue
                
            processed_sittings_data = [process.process_sitting(s, term_schema) for s in sittings_to_process if s is not None]
            # Filter out None results if process_sitting can return None (it does for planned sittings, already filtered)
            processed_sittings_data = [s_data for s_data in processed_sittings_data if s_data is not None]

            if processed_sittings_data:
                 process.bulk_insert_data(conn, "Sittings", processed_sittings_data, process.SITTINGS_COLUMN_ORDER, chunk_size)
                 logger.info(f"Processed and inserted {len(processed_sittings_data)} sittings for term {term_schema.number}.")
            
            # VOTINGS and VOTES (Processed in batches of sittings)
            # The _fetch_and_process_votings_for_sittings handles its own parallelism for API calls via semaphore
            # and accumulates data for votings, voting_options, and votes.
            # It then bulk inserts them.
            # We pass the sittings_to_process (which are schema objects)
            
            # The helper function will now handle fetching votings, then votes, and processing them.
            # It needs the list of sitting *schema objects* to iterate over.
            await _fetch_and_process_votings_for_sittings(
                client,
                term_schema,
                sittings_to_process, # Pass the filtered list of sitting schema objects
                conn,
                mp_id_lookup,
                chunk_size,
                semaphore, # Pass the original semaphore for it to manage sub-tasks
                current_from_voting_num if term_schema.number == current_from_term_num and current_from_sitting_num is not None else None,
            )
            
            # Reset from_sitting and from_voting after the first applicable term is processed
            if term_schema.number == current_from_term_num:
                current_from_sitting_num = None
                current_from_voting_num = None


        logger.info("Scraping finished.")

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e.request.url} - {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.opt(exception=True).error(f"An unexpected error occurred during scraping: {e}")
    finally:
        await client.aclose()
        if conn:
            conn.close()
        logger.info("Client and database connection closed.")


@app.command()
async def resume(
    parallelism_limit: Annotated[int, typer.Option(help="Max concurrent API calls.")] = 10,
    chunk_size: Annotated[int, typer.Option(help="Rows per bulk insert chunk.")] = 100,
) -> None:
    logger.info("Attempting to resume scraping...")
    conn = get_duckdb_connection()
    from_point_str: Optional[str] = None

    try:
        latest_term_row = conn.execute("SELECT MAX(number) FROM Terms").fetchone()
        if latest_term_row is None or latest_term_row[0] is None:
            logger.info("No existing data found. Starting full scrape.")
            await scrape(from_point=None, parallelism_limit=parallelism_limit, chunk_size=chunk_size)
            return

        latest_term_number = latest_term_row[0]

        latest_sitting_row = conn.execute(
            "SELECT MAX(S.number) FROM Sittings S JOIN Terms T ON S.term_id = T.id WHERE T.number = ?",
            (latest_term_number,)
        ).fetchone()

        if latest_sitting_row is None or latest_sitting_row[0] is None:
            from_point_str = f"{latest_term_number}"
            logger.info(f"Resuming from term {latest_term_number}.")
        else:
            latest_sitting_number = latest_sitting_row[0]
            latest_voting_row = conn.execute(
                """
                SELECT MAX(V.number) 
                FROM Votings V
                JOIN Sittings S ON V.sitting_id = S.id
                JOIN Terms T ON S.term_id = T.id
                WHERE T.number = ? AND S.number = ?
                """, (latest_term_number, latest_sitting_number)
            ).fetchone()

            if latest_voting_row is None or latest_voting_row[0] is None:
                from_point_str = f"{latest_term_number},{latest_sitting_number}"
                logger.info(f"Resuming from term {latest_term_number}, sitting {latest_sitting_number}.")
            else:
                latest_voting_number = latest_voting_row[0]
                from_point_str = f"{latest_term_number},{latest_sitting_number},{latest_voting_number}"
                logger.info(f"Resuming from term {latest_term_number}, sitting {latest_sitting_number}, voting {latest_voting_number}.")
        
        await scrape(from_point=from_point_str, parallelism_limit=parallelism_limit, chunk_size=chunk_size)

    except Exception as e:
        logger.opt(exception=True).error(f"Error during resume operation: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    app()
