from typing import Optional

import httpx
import sqlmodel

from sejm_scraper import database, scrape


def start_pipeline(
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

        # Parties, Mps, Sittings
        for term in terms:
            # Parties
            scraped_parties = scrape.scrape_parties(
                client=http_client, term=term
            )
            for party in scraped_parties:
                database_client.merge(party)
            database_client.commit()

            # MPs
            scraped_mps_in_term = scrape.scrape_mps_in_term(
                client=http_client, term=term
            )
            for mp_in_term in scraped_mps_in_term.mps_in_term:
                database_client.merge(mp_in_term)
            database_client.commit()

            # Sittings
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
        most_recent_voting = database_client.exec(
            sqlmodel.select(database.Voting)
            .join(database.Sitting)
            .join(database.Term)
            .order_by(
                sqlmodel.desc(database.Term.number),
                sqlmodel.desc(database.Sitting.number),
                sqlmodel.desc(database.Voting.number),
            )
        ).first()

        if most_recent_voting is None:
            start_pipeline()
        else:
            sitting = database_client.exec(
                sqlmodel.select(database.Sitting).where(
                    database.Sitting.id == most_recent_voting.sitting_id
                )
            ).first()

            term = database_client.exec(
                sqlmodel.select(database.Term).where(
                    database.Term.id == sitting.term_id
                )
            ).first()

            start_pipeline(
                from_term=term.number,
                from_sitting=sitting.number,
                from_voting=most_recent_voting.number,
            )
