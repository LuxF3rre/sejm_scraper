from typing import Optional

import httpx
import sqlmodel

from sejm_scraper import database, scrape


def pipeline(
    from_term: Optional[int] = None,
    from_sitting: Optional[int] = None,
    from_voting: Optional[int] = None,
) -> None:
    if from_voting is not None and (from_sitting is None or from_term is None):
        msg = (
            "from_voting can only be set if "
            "from_sitting and from_term are also set"
        )
        raise ValueError(msg)

    if from_sitting is not None and from_term is None:
        msg = "from_sitting can only be set if from_term is also set"
        raise ValueError(msg)

    with (
        httpx.Client() as http_client,
        sqlmodel.Session(database.ENGINE) as database_client,
    ):
        # Terms
        terms = scrape.scrape_terms(client=http_client, from_term=from_term)
        for term in terms:
            database_client.merge(term)
        database_client.commit()

        # Sittings
        for term in terms:
            scraped_mps = scrape.scrape_mps(client=http_client, term=term)
            for mp in scraped_mps.mps:
                database_client.merge(mp)
            for mp_to_term_link in scraped_mps.mp_to_term_links:
                database_client.merge(mp_to_term_link)
            database_client.commit()

            sittings = scrape.scrape_sittings(
                client=http_client,
                term=term,
                from_sitting=from_sitting if term.number == from_term else None,
            )
            for sitting in sittings:
                database_client.merge(sitting)
            database_client.commit()

            # Votings
            for sitting in sittings:
                scraped_votings = scrape.scrape_votings(
                    client=http_client,
                    term=term,
                    sitting=sitting,
                    from_voting=from_voting
                    if term.number == from_term
                    and sitting.number == from_sitting
                    else None,
                )
                for voting in scraped_votings.votings:
                    database_client.merge(voting)
                for voting_option in scraped_votings.voting_options:
                    database_client.merge(voting_option)
                database_client.commit()

                # Votes
                for voting in scraped_votings.votings:
                    votes = scrape.scrape_votes(
                        client=http_client,
                        term=term,
                        sitting=sitting,
                        voting=voting,
                    )
                    for vote in votes:
                        database_client.merge(vote)
                    database_client.commit()


def resume_pipeline() -> None:
    with sqlmodel.Session(database.ENGINE) as database_client:
        last_term = database_client.exec(
            sqlmodel.select(database.Term).order_by(
                sqlmodel.desc(database.Term.number)  # ty: ignore
            )
        ).first()
        if last_term is None:
            pipeline()
        else:
            last_sitting = database_client.exec(
                sqlmodel.select(database.Sitting)
                .where(database.Sitting.term_id == last_term.id)
                .order_by(sqlmodel.desc(database.Sitting.number))  # ty: ignore
            ).first()
            if last_sitting is None:
                pipeline(from_term=last_term.number)
            else:
                last_voting = database_client.exec(
                    sqlmodel.select(database.Voting)
                    .where(database.Voting.sitting_id == last_sitting.id)
                    .order_by(
                        sqlmodel.desc(database.Voting.number)  # ty: ignore
                    )
                ).first()
                if last_voting is None:
                    pipeline(
                        from_term=last_term.number,
                        from_sitting=last_sitting.number,
                    )
                else:
                    pipeline(
                        from_term=last_term.number,
                        from_sitting=last_sitting.number,
                        from_voting=last_voting.number,
                    )
