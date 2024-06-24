from hashlib import sha256

import httpx
import typer  # type: ignore
from loguru import logger

from sejm_scraper import api_client as api
from sejm_scraper import database, schemas
from sejm_scraper.database import Base, SessionMaker, engine

app = typer.Typer()


def _get_surogate_key(*s: str) -> str:
    to_hash = [str(x) for x in s if x is not None]
    to_hash_bytes = "".join(to_hash).encode("utf-8")
    sha256_hash = sha256()
    sha256_hash.update(to_hash_bytes)
    hex_hash = sha256_hash.hexdigest()
    return hex_hash


@app.command()
def prepare_database() -> None:
    Base.metadata.create_all(engine)


@app.command()
def scrape() -> None:
    client = httpx.Client()
    with SessionMaker() as db:
        #
        # terms
        #
        terms = api.get_terms(client)
        if not terms:
            logger.warning("No terms data")

        for term in terms:
            logger.info(f"Scraping term {term.number}")
            term_id = _get_surogate_key(term.number)
            db_item = database.Terms(
                id=term_id,
                number=term.number,
            )
            db.add(db_item)
            db.commit()

            mps = api.get_mps(client, term.number)
            if not mps:
                logger.warning(f"No MPs data for term {term.number}")
            for mp in mps:
                logger.info(f"Scraping mp {mp.in_term_id} in {term.number}")
                mp_id = _get_surogate_key(
                    mp.first_name,
                    # mp.second_name,
                    mp.last_name,
                    mp.birth_date,
                    mp.birth_place,
                )
                mp_exists = db.query(database.MPs).filter(database.MPs.id == mp_id).first()
                if mp_exists is None:
                    db_item = database.MPs(
                        id=mp_id,
                        first_name=mp.first_name,
                        second_name=mp.second_name,
                        last_name=mp.last_name,
                        birth_date=mp.birth_date,
                        birth_place=mp.birth_place,
                    )
                    db.add(db_item)
                    db.commit()

                mp_to_term_link_id = _get_surogate_key(
                    mp.first_name,
                    # mp.second_name,
                    mp.last_name,
                    mp.birth_date,
                    mp.birth_place,
                    term.number,
                )
                db_item = database.MpToTermLink(
                    id=mp_to_term_link_id,
                    mp_id=mp_id,
                    term_id=term_id,
                    in_term_id=mp.in_term_id,
                    party=mp.party,
                    education=mp.education,
                    profession=mp.profession,
                    voivodeship=mp.voivodeship,
                    district_name=mp.district_name,
                    inactivity_cause=mp.inactivity_cause,
                    inactivity_description=mp.inactivity_description,
                    became_inactive=mp.inactivity_cause is not None or mp.inactivity_description is not None,
                )
                db.add(db_item)
                db.commit()

            #
            # sittings
            #

            sittings = api.get_sittings(client, term.number)
            if not sittings:
                logger.warning(f"No sittings data for term {term.number}")
            for sitting in sittings:
                logger.info(f"Scraping sitting {sitting.number} in {term.number}")

                if sitting.number == 0:  # skip planned sittings
                    continue

                sitting_id = _get_surogate_key(term.number, sitting.number)
                db_item = database.Sittings(
                    id=sitting_id,
                    term_id=term_id,
                    title=sitting.title,
                    number=sitting.number,
                )
                db.add(db_item)
                db.commit()

                #
                # votings
                #

                votings = api.get_votings(client, term.number, sitting.number)
                if not votings:
                    logger.warning(f"No voting data for term {term.number}, sitting {sitting.number}")
                for voting in votings:
                    logger.info(f"Scraping votting {voting.voting_number} in sitting {sitting.number} in {term.number}")
                    voting_id = _get_surogate_key(
                        term.number,
                        sitting.number,
                        voting.sitting_day,
                        voting.voting_number,
                        voting.date,
                    )
                    db_item = database.Votings(
                        id=voting_id,
                        sitting_id=sitting_id,
                        sitting_day=voting.sitting_day,
                        voting_number=voting.voting_number,
                        date=voting.date,
                        title=voting.title,
                        description=voting.description,
                        topic=voting.topic,
                    )
                    db.add(db_item)
                    db.commit()

                    #
                    # voting options
                    #

                    voting_options = voting.voting_options
                    if voting_options is None:
                        # provide "default" option
                        voting_options = [schemas.VotingOptionSchema(**{"optionIndex": 1, "description": None})]

                    for voting_option in voting_options:
                        voting_option_id = _get_surogate_key(
                            term.number,
                            sitting.number,
                            voting.sitting_day,
                            voting.voting_number,
                            voting.date,
                            voting_option.option_index,
                        )
                        db_item = database.VotingOptions(
                            id=voting_option_id,
                            voting_id=voting_id,
                            option_index=voting_option.option_index,
                            description=voting_option.description,
                        )
                        db.add(db_item)
                        db.commit()

                    #
                    # votes
                    #

                    votes = api.get_votes(client, term.number, sitting.number, voting.voting_number)
                    if not votings:
                        logger.warning(
                            f"No votes data for term {term.number} ",
                            f"sitting {sitting.number}, voting {voting.voting_number}",
                        )

                    for vote in votes.mp_votes:
                        logger.info(
                            f"Scraping vote {vote.mp_term_id} "
                            f"in votting {voting.voting_number} "
                            f"in sitting {sitting.number} in {term.number}"
                        )
                        mp_id_query = (
                            db.query(database.MpToTermLink)
                            .filter(
                                database.MpToTermLink.term_id == term_id,
                                database.MpToTermLink.in_term_id == vote.mp_term_id,
                            )
                            .first()
                        )
                        if mp_id_query is None:
                            raise ValueError
                        mp_id = mp_id_query.mp_id  # type: ignore
                        inner_votes = vote.votes
                        if inner_votes is None:
                            inner_votes = {1: vote.vote}

                        for inner_vote_index, inner_vote in inner_votes.items():
                            inner_vote_id = _get_surogate_key(
                                term.number,
                                sitting.number,
                                voting.sitting_day,
                                voting.voting_number,
                                voting.date,
                                inner_vote_index,
                                mp_id,  # type: ignore
                            )
                            voting_option_id = _get_surogate_key(
                                term.number,
                                sitting.number,
                                voting.sitting_day,
                                voting.voting_number,
                                voting.date,
                                inner_vote_index,
                            )
                            db_item = database.Votes(
                                id=inner_vote_id,
                                voting_option_id=voting_option_id,
                                mp_id=mp_id,
                                vote=inner_vote,
                            )
                            db.add(db_item)
                            db.commit()


if __name__ == "__main__":
    app()
