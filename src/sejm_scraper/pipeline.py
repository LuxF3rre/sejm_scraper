import httpx
import sqlmodel

from sejm_scraper import database, scrape


def start_pipeline(
    from_term: int | None = None,
    from_sitting: int | None = None,
    from_voting: int | None = None,
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
        sqlmodel.Session(database.get_engine()) as database_client,
    ):
        # Terms
        terms = scrape.scrape_terms(client=http_client, from_term=from_term)
        for term in terms:
            database_client.merge(term)
        database_client.commit()

        # Parties, Mps, Sittings
        for term in terms:
            # Parties
            scraped_parties_in_term = scrape.scrape_parties_in_term(
                client=http_client, term=term
            )
            for party_in_term in scraped_parties_in_term:
                database_client.merge(party_in_term)
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
                        sql = sqlmodel.text("""
                            INSERT OR REPLACE INTO vote (
                                id,
                                voting_option_id,
                                mp_in_term_id,
                                party_in_term_id,
                                vote
                            )
                            VALUES (
                                :id,
                                :voting_option_id,
                                :mp_in_term_id,
                                :party_in_term_id,
                                :vote
                            );
                        """)
                        database_client.exec(
                            sql,
                            params=vote.model_dump(),
                        )
                    database_client.commit()


def _get_most_recent_voting(
    database_client: sqlmodel.Session,
) -> database.Voting | None:
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
    return most_recent_voting


def _get_most_recent_sitting(
    database_client: sqlmodel.Session,
    most_recent_voting: database.Voting,
) -> database.Sitting:
    return database_client.exec(
        sqlmodel.select(database.Sitting).where(
            database.Sitting.id == most_recent_voting.sitting_id
        )
    ).first()


def _get_most_recent_term(
    database_client: sqlmodel.Session,
    most_recent_sitting: database.Sitting,
) -> database.Term:
    return database_client.exec(
        sqlmodel.select(database.Term).where(
            database.Term.id == most_recent_sitting.term_id
        )
    ).first()


def resume_pipeline() -> None:
    with sqlmodel.Session(database.get_engine()) as database_client:
        most_recent_voting = _get_most_recent_voting(database_client)

        if most_recent_voting is None:
            start_pipeline()
        else:
            most_recent_sitting = _get_most_recent_sitting(
                database_client,
                most_recent_voting,
            )

            most_recent_term = _get_most_recent_term(
                database_client,
                most_recent_sitting,
            )

            start_pipeline(
                from_term=most_recent_term.number,
                from_sitting=most_recent_sitting.number,
                from_voting=most_recent_voting.number,
            )
