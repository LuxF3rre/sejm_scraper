from typing import Annotated, Union

import httpx
import typer  # type: ignore
from loguru import logger

from sejm_scraper import api_client as api
from sejm_scraper import process, schemas
from sejm_scraper.database import Base, SessionMaker, Sittings, Terms, Votings, engine

app = typer.Typer()


@app.command()
def prepare_database() -> None:
    Base.metadata.create_all(engine)


@app.command()
def scrape(
    from_point: Annotated[
        Union[str, None], typer.Option(help="In form of term[,sitting[,voting]], e.g. 10,13,35; 10,13; 10")
    ] = None,
) -> None:
    from_term, from_sitting, from_voting = None, None, None
    if from_point is not None:
        from_elements = [int(x) for x in from_point.split(",")]
        fill = 3 - len(from_elements)
        from_term, from_sitting, from_voting = from_elements + [None] * fill

    client = httpx.Client(timeout=30.0)

    #
    # terms
    #

    terms = api.get_terms(client)
    if not terms:
        logger.warning("No terms data")
    if from_term is not None:
        terms = [term for term in terms if term.number >= from_term]
    for term in terms:
        process.process_term(term=term)

        #
        # mps
        #

        mps = api.get_mps(client, term.number)
        if not mps:
            logger.warning(f"No MPs data for term {term.number}")
        for mp in mps:
            process.process_mp(mp=mp, term=term)
            process.process_mp_to_term_link(mp=mp, term=term)

        #
        # sittings
        #

        sittings = api.get_sittings(client, term.number)
        if not sittings:
            logger.warning(f"No sittings data for term {term.number}")

        if from_sitting is not None and term.number == from_term:
            # note that this also filters out planned meetings (all have number: 0)
            # hence no skip log from the process_sitting
            sittings = [sitting for sitting in sittings if sitting.number >= from_sitting]
        for sitting in sittings:
            process.process_sitting(sitting=sitting, term=term)

            #
            # votings
            #

            votings = api.get_votings(client, term.number, sitting.number)
            if not votings:
                logger.warning(f"No voting data for term {term.number}, sitting {sitting.number}")
            if from_voting is not None and sitting.number == from_sitting and term.number == from_term:
                votings = [voting for voting in votings if voting.number >= from_voting]
            for voting in votings:
                process.process_voting(voting=voting, term=term, sitting=sitting)

                #
                # voting options
                #

                voting_options = voting.voting_options
                if voting_options is None:
                    # provide "default" option
                    voting_options = [schemas.VotingOptionSchema(**{"optionIndex": 1, "description": None})]
                for voting_option in voting_options:
                    process.process_voting_option(
                        voting_option=voting_option,
                        term=term,
                        sitting=sitting,
                        voting=voting,
                    )
                #
                # votes
                #

                votes = api.get_votes(client, term.number, sitting.number, voting.number)
                if not votings:
                    logger.warning(
                        f"No votes data for term {term.number} ",
                        f"sitting {sitting.number}, voting {voting.number}",
                    )
                for vote in votes.mp_votes:
                    process.process_vote(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        vote=vote,
                    )

    logger.info("All done")


@app.command()
def resume() -> None:
    with SessionMaker() as db:
        latest_term = db.query(Terms.number).order_by(Terms.number.desc()).first()

        if latest_term is None:
            logger.info("No existing data found. Starting from the beginning")
            scrape()
            return

        latest_term_number = latest_term[0]

        latest_sitting = (
            db.query(Sittings.number)
            .join(Terms)
            .filter(Terms.number == latest_term_number)
            .order_by(Sittings.number.desc())
            .first()
        )

        if latest_sitting is None:
            from_point = f"{latest_term_number}"
            logger.info(f"Resuming from term {latest_term_number}")
            scrape(from_point=from_point)
            return

        latest_sitting_number = latest_sitting[0]

        latest_voting = (
            db.query(Votings.number)
            .join(Sittings)
            .join(Terms)
            .filter(Terms.number == latest_term_number, Sittings.number == latest_sitting_number)
            .order_by(Votings.number.desc())
            .first()
        )

        if latest_voting is None:
            from_point = f"{latest_term_number},{latest_sitting_number}"
            logger.info(f"Resuming from term {latest_term_number}, sitting {latest_sitting_number}")
            scrape(from_point=from_point)
            return

        latest_voting_number = latest_voting[0]

        from_point = f"{latest_term_number},{latest_sitting_number},{latest_voting_number}"
        logger.info(
            f"Resuming from term {latest_term_number}, sitting {latest_sitting_number}, voting {latest_voting_number}"
        )
        scrape(from_point=from_point)


if __name__ == "__main__":
    app()
