import anyio
import httpx
import sqlmodel
from loguru import logger
from sqlalchemy import Engine

from sejm_scraper import database, scrape

# Cap on simultaneous requests to the Sejm API when scraping votes.
# The API is a public government service; hammering it with hundreds
# of concurrent requests risks throttling or bans.
MAX_CONCURRENT_VOTE_REQUESTS = 10


async def _scrape_voting_votes(
    client: httpx.AsyncClient,
    limiter: anyio.CapacityLimiter,
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
    mp_link_ids: dict[int, str],
    all_votes: list[database.VoteRecord],
    all_detail_options: list[database.VotingOption],
) -> None:
    async with limiter:
        result = await scrape.scrape_votes(
            client=client,
            term=term,
            sitting=sitting,
            voting=voting,
            mp_link_ids=mp_link_ids,
        )
    all_votes.extend(result.votes)
    all_detail_options.extend(result.voting_options)
    logger.info(
        "term {term}, sitting {sitting}, "
        "voting {voting}: scraped {count} votes",
        term=term.number,
        sitting=sitting.number,
        voting=voting.number,
        count=len(result.votes),
    )


async def _process_sitting(
    *,
    http_client: httpx.AsyncClient,
    database_client: sqlmodel.Session,
    limiter: anyio.CapacityLimiter,
    term: database.Term,
    sitting: database.Sitting,
    sitting_days: list[database.SittingDay],
    mp_link_ids: dict[int, str],
    from_voting: int | None,
) -> None:
    """Scrape and persist all votings and votes for a single sitting.

    The sitting row is committed before any votings are scraped, and
    votings are committed together with their votes only after all
    network I/O for the sitting has finished. This keeps the database
    consistent with the resume logic: a crash mid-sitting leaves the
    sitting without votings, so `resume_pipeline` restarts from this
    sitting instead of skipping the unfinished work.
    """
    database.bulk_upsert(
        session=database_client,
        model=database.Sitting,
        records=[sitting],
    )
    database.bulk_upsert(
        session=database_client,
        model=database.SittingDay,
        records=sitting_days,
    )
    database_client.commit()

    scraped_votings = await scrape.scrape_votings(
        client=http_client,
        term=term,
        sitting=sitting,
        from_voting=from_voting,
    )

    all_votes: list[database.VoteRecord] = []
    all_detail_options: list[database.VotingOption] = []

    async with anyio.create_task_group() as tg:
        for voting in scraped_votings.votings:
            tg.start_soon(
                _scrape_voting_votes,
                http_client,
                limiter,
                term,
                sitting,
                voting,
                mp_link_ids,
                all_votes,
                all_detail_options,
            )

    database.bulk_upsert(
        session=database_client,
        model=database.Voting,
        records=scraped_votings.votings,
    )
    database.bulk_upsert(
        session=database_client,
        model=database.VotingOption,
        records=scraped_votings.voting_options,
    )
    database.bulk_upsert(
        session=database_client,
        model=database.VotingOption,
        records=all_detail_options,
    )
    database.bulk_upsert(
        session=database_client,
        model=database.VoteRecord,
        records=all_votes,
    )
    database_client.commit()
    logger.info(
        "term {term}, sitting {sitting}: scraped {count} votings",
        term=term.number,
        sitting=sitting.number,
        count=len(scraped_votings.votings),
    )


async def pipeline(
    *,
    engine: Engine | None = None,
    from_term: int | None = None,
    from_sitting: int | None = None,
    from_voting: int | None = None,
) -> None:
    """Run the full scraping pipeline.

    Records are committed in the same order and granularity that
    `resume_pipeline` uses to infer the resume point: each term and
    sitting is committed when processing of it starts, and a sitting's
    votings are committed atomically with their votes once complete.

    Args:
        engine: SQLAlchemy engine to use. Defaults to a new engine
            with the default DuckDB URL.
        from_term: Start scraping from this term number onwards.
        from_sitting: Start scraping from this sitting number onwards
            (requires from_term).
        from_voting: Start scraping from this voting number onwards
            (requires from_term and from_sitting).

    Raises:
        ValueError: If from_voting is set without from_sitting/from_term,
            or from_sitting is set without from_term.
    """
    if from_voting is not None and (from_sitting is None or from_term is None):
        msg = (
            "from_voting can only be set if "
            "from_sitting and from_term are also set"
        )
        raise ValueError(msg)

    if from_sitting is not None and from_term is None:
        msg = "from_sitting can only be set if from_term is also set"
        raise ValueError(msg)

    if engine is None:
        engine = database.get_engine()

    database.create_db_and_tables(engine=engine)

    limiter = anyio.CapacityLimiter(MAX_CONCURRENT_VOTE_REQUESTS)

    async with httpx.AsyncClient() as http_client:
        with sqlmodel.Session(engine) as database_client:
            # Terms
            terms = await scrape.scrape_terms(
                client=http_client, from_term=from_term
            )
            # Process in ascending order so the highest committed
            # term number is always the last one started.
            terms.sort(key=lambda t: t.number)
            logger.info("scraped {count} terms", count=len(terms))

            for term in terms:
                database.bulk_upsert(
                    session=database_client,
                    model=database.Term,
                    records=[term],
                )
                database_client.commit()

                # Mps & Clubs
                scraped_mps = await scrape.scrape_mps(
                    client=http_client, term=term
                )
                database.bulk_upsert(
                    session=database_client,
                    model=database.Mp,
                    records=scraped_mps.mps,
                )
                database.bulk_upsert(
                    session=database_client,
                    model=database.MpToTermLink,
                    records=scraped_mps.mp_to_term_links,
                )
                database_client.commit()
                logger.info(
                    "term {term}: scraped {mp_count} mps",
                    term=term.number,
                    mp_count=len(scraped_mps.mps),
                )
                mp_link_ids = {
                    link.in_term_id: link.id
                    for link in scraped_mps.mp_to_term_links
                }

                scraped_clubs = await scrape.scrape_clubs(
                    client=http_client, term=term
                )
                database.bulk_upsert(
                    session=database_client,
                    model=database.Club,
                    records=scraped_clubs,
                )
                database_client.commit()
                logger.info(
                    "term {term}: scraped {club_count} clubs",
                    term=term.number,
                    club_count=len(scraped_clubs),
                )

                # Sittings
                effective_from_sitting = (
                    from_sitting if term.number == from_term else None
                )
                scraped_sittings = await scrape.scrape_sittings(
                    client=http_client,
                    term=term,
                    from_sitting=effective_from_sitting,
                )
                if not scraped_sittings.sittings:
                    scraped_sittings = (
                        await scrape.discover_sittings_from_votings(
                            client=http_client,
                            term=term,
                            from_sitting=effective_from_sitting,
                        )
                    )
                    if scraped_sittings.sittings:
                        logger.info(
                            "term {term}: discovered {count} sittings "
                            "from voting table",
                            term=term.number,
                            count=len(scraped_sittings.sittings),
                        )
                logger.info(
                    "term {term}: scraped {count} sittings",
                    term=term.number,
                    count=len(scraped_sittings.sittings),
                )

                sittings = sorted(
                    scraped_sittings.sittings, key=lambda s: s.number
                )
                days_by_sitting: dict[str, list[database.SittingDay]] = {}
                for day in scraped_sittings.sitting_days:
                    days_by_sitting.setdefault(day.sitting_id, []).append(day)

                # Votings & Votes
                for sitting in sittings:
                    await _process_sitting(
                        http_client=http_client,
                        database_client=database_client,
                        limiter=limiter,
                        term=term,
                        sitting=sitting,
                        sitting_days=days_by_sitting.get(sitting.id, []),
                        mp_link_ids=mp_link_ids,
                        from_voting=from_voting
                        if term.number == from_term
                        and sitting.number == from_sitting
                        else None,
                    )


async def resume_pipeline(*, engine: Engine | None = None) -> None:
    """Resume the scraping pipeline from the last completed point.

    Queries the database for the most recent term, sitting, and voting,
    then resumes the pipeline from that point. The most recent unit is
    re-scraped (upserts make this idempotent), since it may have been
    interrupted mid-way.

    Args:
        engine: SQLAlchemy engine to use. Defaults to a new engine
            with the default DuckDB URL.
    """
    if engine is None:
        engine = database.get_engine()

    database.create_db_and_tables(engine=engine)

    from_term: int | None = None
    from_sitting: int | None = None
    from_voting: int | None = None

    with sqlmodel.Session(engine) as database_client:
        last_term = database_client.exec(
            sqlmodel.select(database.Term).order_by(
                sqlmodel.desc(database.Term.number)
            )
        ).first()
        if last_term is not None:
            from_term = last_term.number
            last_sitting = database_client.exec(
                sqlmodel.select(database.Sitting)
                .where(database.Sitting.term_id == last_term.id)
                .order_by(sqlmodel.desc(database.Sitting.number))
            ).first()
            if last_sitting is not None:
                from_sitting = last_sitting.number
                last_voting = database_client.exec(
                    sqlmodel.select(database.Voting)
                    .where(database.Voting.sitting_id == last_sitting.id)
                    .order_by(sqlmodel.desc(database.Voting.number))
                ).first()
                if last_voting is not None:
                    from_voting = last_voting.number

    if from_term is None:
        logger.info("no existing data found, starting fresh pipeline")
        await pipeline(engine=engine)
    elif from_sitting is None:
        logger.info("resuming from term {term}", term=from_term)
        await pipeline(engine=engine, from_term=from_term)
    elif from_voting is None:
        logger.info(
            "resuming from term {term}, sitting {sitting}",
            term=from_term,
            sitting=from_sitting,
        )
        await pipeline(
            engine=engine,
            from_term=from_term,
            from_sitting=from_sitting,
        )
    else:
        logger.info(
            "resuming from term {term}, sitting {sitting}, voting {voting}",
            term=from_term,
            sitting=from_sitting,
            voting=from_voting,
        )
        await pipeline(
            engine=engine,
            from_term=from_term,
            from_sitting=from_sitting,
            from_voting=from_voting,
        )
