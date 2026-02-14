import anyio
import httpx
import sqlmodel
from loguru import logger
from sqlalchemy import Engine

from sejm_scraper import database, scrape


async def pipeline(
    *,
    engine: Engine | None = None,
    from_term: int | None = None,
    from_sitting: int | None = None,
    from_voting: int | None = None,
) -> None:
    """Run the full scraping pipeline.

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

    async with httpx.AsyncClient() as http_client:
        with sqlmodel.Session(engine) as database_client:
            # Terms
            terms = await scrape.scrape_terms(
                client=http_client, from_term=from_term
            )
            database.bulk_upsert(
                session=database_client,
                model=database.Term,
                records=terms,
            )
            database_client.commit()
            logger.info("scraped {count} terms", count=len(terms))

            # Mps, Clubs & Sittings
            for term in terms:
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
                database.bulk_upsert(
                    session=database_client,
                    model=database.Sitting,
                    records=scraped_sittings.sittings,
                )
                database.bulk_upsert(
                    session=database_client,
                    model=database.SittingDay,
                    records=scraped_sittings.sitting_days,
                )
                database_client.commit()
                logger.info(
                    "term {term}: scraped {count} sittings",
                    term=term.number,
                    count=len(scraped_sittings.sittings),
                )

                # Votings
                for sitting in scraped_sittings.sittings:
                    scraped_votings = await scrape.scrape_votings(
                        client=http_client,
                        term=term,
                        sitting=sitting,
                        from_voting=from_voting
                        if term.number == from_term
                        and sitting.number == from_sitting
                        else None,
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
                    database_client.commit()
                    logger.info(
                        "term {term}, sitting {sitting}: "
                        "scraped {count} votings",
                        term=term.number,
                        sitting=sitting.number,
                        count=len(scraped_votings.votings),
                    )

                    # Votes — scrape concurrently, single commit per sitting
                    all_votes: list[database.VoteRecord] = []
                    all_detail_options: list[database.VotingOption] = []

                    async def _scrape_voting_votes(
                        voting: database.Voting,
                        term: database.Term,
                        sitting: database.Sitting,
                        all_votes: list[database.VoteRecord],
                        all_detail_options: list[database.VotingOption],
                    ) -> None:
                        result = await scrape.scrape_votes(
                            client=http_client,
                            term=term,
                            sitting=sitting,
                            voting=voting,
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

                    async with anyio.create_task_group() as tg:
                        for voting in scraped_votings.votings:
                            tg.start_soon(
                                _scrape_voting_votes,
                                voting,
                                term,
                                sitting,
                                all_votes,
                                all_detail_options,
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


async def resume_pipeline(*, engine: Engine | None = None) -> None:
    """Resume the scraping pipeline from the last completed point.

    Queries the database for the most recent term, sitting, and voting,
    then resumes the pipeline from that point.

    Args:
        engine: SQLAlchemy engine to use. Defaults to a new engine
            with the default DuckDB URL.
    """
    if engine is None:
        engine = database.get_engine()

    with sqlmodel.Session(engine) as database_client:
        last_term = database_client.exec(
            sqlmodel.select(database.Term).order_by(
                sqlmodel.desc(database.Term.number)
            )
        ).first()
        if last_term is None:
            logger.info("no existing data found, starting fresh pipeline")
            await pipeline(engine=engine)
        else:
            last_sitting = database_client.exec(
                sqlmodel.select(database.Sitting)
                .where(database.Sitting.term_id == last_term.id)
                .order_by(sqlmodel.desc(database.Sitting.number))
            ).first()
            if last_sitting is None:
                logger.info(
                    "resuming from term {term}",
                    term=last_term.number,
                )
                await pipeline(engine=engine, from_term=last_term.number)
            else:
                last_voting = database_client.exec(
                    sqlmodel.select(database.Voting)
                    .where(database.Voting.sitting_id == last_sitting.id)
                    .order_by(sqlmodel.desc(database.Voting.number))
                ).first()
                if last_voting is None:
                    logger.info(
                        "resuming from term {term}, sitting {sitting}",
                        term=last_term.number,
                        sitting=last_sitting.number,
                    )
                    await pipeline(
                        engine=engine,
                        from_term=last_term.number,
                        from_sitting=last_sitting.number,
                    )
                else:
                    logger.info(
                        "resuming from term {term}, "
                        "sitting {sitting}, voting {voting}",
                        term=last_term.number,
                        sitting=last_sitting.number,
                        voting=last_voting.number,
                    )
                    await pipeline(
                        engine=engine,
                        from_term=last_term.number,
                        from_sitting=last_sitting.number,
                        from_voting=last_voting.number,
                    )
