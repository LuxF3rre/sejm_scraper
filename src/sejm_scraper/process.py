from loguru import logger

from sejm_scraper import database, schemas, utils
from sejm_scraper.database import SessionMaker


def process_term(
    term: schemas.TermSchema,
) -> None:
    logger.info(f"Processing {term.number} term")
    term_id = utils.get_term_nk(term=term)
    db_item = database.Terms(
        id=term_id,
        number=term.number,
        from_date=term.from_date,
        to_date=term.to_date,
    )
    with SessionMaker() as db:
        db.merge(db_item)
        db.commit()


def process_sitting(
    sitting: schemas.SittingSchema,
    term: schemas.TermSchema,
) -> None:
    logger.info(f"Processing sitting {sitting.number} in {term.number} term")
    term_id = utils.get_term_nk(term)
    if sitting.number != 0:  # skip planned sittings
        sitting_id = utils.get_sitting_nk(sitting=sitting, term=term)
        db_item = database.Sittings(
            id=sitting_id,
            term_id=term_id,
            title=sitting.title,
            number=sitting.number,
        )
        with SessionMaker() as db:
            db.merge(db_item)
            db.commit()
    else:
        logger.info(
            f"Sitting {sitting.number} in {term.number} "
            "term is planned sitting, skipping"
        )


def process_voting(
    voting: schemas.VotingSchema,
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
) -> None:
    logger.info(
        f"Processing voting {voting.number} in sitting {sitting.number} "
        f"in {term.number} term"
    )
    voting_id = utils.get_voting_nk(voting=voting, term=term, sitting=sitting)
    sitting_id = utils.get_sitting_nk(sitting=sitting, term=term)
    db_item = database.Votings(
        id=voting_id,
        sitting_id=sitting_id,
        sitting_day=voting.sitting_day,
        number=voting.number,
        date=voting.date,
        title=voting.title,
        description=voting.description,
        topic=voting.topic,
    )
    with SessionMaker() as db:
        db.merge(db_item)
        db.commit()


def process_voting_option(
    voting_option: schemas.VotingOptionSchema,
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
    voting: schemas.VotingSchema,
) -> None:
    logger.info(
        f"Processing voting option {voting_option.index} "
        f"in voting {voting.number} "
        f"in sitting {sitting.number} in {term.number} term"
    )
    voting_id = utils.get_voting_nk(voting=voting, term=term, sitting=sitting)
    voting_option_id = utils.get_voting_option_nk(
        voting_option_index=voting_option.index,
        term=term,
        sitting=sitting,
        voting=voting,
    )
    db_item = database.VotingOptions(
        id=voting_option_id,
        voting_id=voting_id,
        index=voting_option.index,
        description=voting_option.description,
    )
    with SessionMaker() as db:
        db.merge(db_item)
        db.commit()


def process_vote(
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
    voting: schemas.VotingSchema,
    vote: schemas.MpVoteSchema,
) -> None:
    term_id = utils.get_term_nk(term)

    with SessionMaker() as db:
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

        mp_id = mp_id_query.mp_id

        inner_votes = vote.votes
        if inner_votes is None:
            if vote.vote == "VOTE_VALID":
                raise TypeError
            inner_votes = {schemas.OptionIndex(1): vote.vote}

        for inner_vote_index, inner_vote in inner_votes.items():
            logger.info(
                f"Processing vote {vote.mp_term_id} "
                f"for voting option {inner_vote_index} "
                f"in voting {voting.number} "
                f"in sitting {sitting.number} in {term.number} term"
            )
            inner_vote_id = utils.get_vote_nk(
                term=term,
                sitting=sitting,
                voting=voting,
                voting_option_index=inner_vote_index,
                mp_id=mp_id,
            )

            voting_option_id = utils.get_voting_option_nk(
                voting_option_index=inner_vote_index,
                term=term,
                sitting=sitting,
                voting=voting,
            )

            db_item = database.Votes(
                id=inner_vote_id,
                voting_option_id=voting_option_id,
                mp_id=mp_id,
                vote=inner_vote,
                party=vote.party,
            )
            db.merge(db_item)
            db.commit()


def process_mp(
    mp: schemas.MpSchema,
    term: schemas.TermSchema,
) -> None:
    logger.info(
        f"Processing mp of in term id {mp.in_term_id} in {term.number} term"
    )
    mp_id = utils.get_mp_nk(mp=mp)
    with SessionMaker() as db:
        db_item = database.MPs(
            id=mp_id,
            first_name=mp.first_name,
            second_name=mp.second_name,
            last_name=mp.last_name,
            birth_date=mp.birth_date,
            birth_place=mp.birth_place,
        )
        db.merge(db_item)
        db.commit()


def process_mp_to_term_link(
    mp: schemas.MpSchema,
    term: schemas.TermSchema,
) -> None:
    logger.info(
        f"Processing mp to term link of mp of in term id {mp.in_term_id} "
        f"in {term.number} term"
    )
    mp_to_term_link_id = utils.get_mp_to_term_link_nk(mp=mp, term=term)
    term_id = utils.get_term_nk(term)
    mp_id = utils.get_mp_nk(mp)
    db_item = database.MpToTermLink(
        id=mp_to_term_link_id,
        mp_id=mp_id,
        term_id=term_id,
        in_term_id=mp.in_term_id,
        education=mp.education,
        profession=mp.profession,
        voivodeship=mp.voivodeship,
        district_name=mp.district_name,
        inactivity_cause=mp.inactivity_cause,
        inactivity_description=mp.inactivity_description,
    )
    with SessionMaker() as db:
        db.merge(db_item)
        db.commit()
