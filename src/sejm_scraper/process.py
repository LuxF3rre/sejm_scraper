from typing import Any, Dict, List, Optional

from loguru import logger

from sejm_scraper import schemas, utils


# Column orders for executemany
# These must match the CREATE TABLE statements in database.py
# and the keys in the dictionaries returned by process_* functions.
TERMS_COLUMN_ORDER = ["id", "number", "from_date", "to_date"]
SITTINGS_COLUMN_ORDER = ["id", "term_id", "title", "number"]
VOTINGS_COLUMN_ORDER = [
    "id",
    "sitting_id",
    "sitting_day",
    "number",
    "date",
    "title",
    "description",
    "topic",
]
VOTING_OPTIONS_COLUMN_ORDER = ["id", "voting_id", "index", "description"]
MPS_COLUMN_ORDER = [
    "id",
    "first_name",
    "second_name",
    "last_name",
    "birth_date",
    "birth_place",
]
VOTES_COLUMN_ORDER = ["id", "voting_option_id", "mp_id", "vote", "party"]
MP_TO_TERM_LINK_COLUMN_ORDER = [
    "id",
    "mp_id",
    "term_id",
    "in_term_id",
    "education",
    "profession",
    "voivodeship",
    "district_name",
    "inactivity_cause",
    "inactivity_description",
]


def bulk_insert_data(
    conn: Any,  # duckdb connection object
    table_name: str,
    data: List[Dict[str, Any]],
    column_order: List[str],
    chunk_size: int = 100,
) -> None:
    if not data:
        return

    logger.info(f"Bulk inserting {len(data)} rows into {table_name}")
    
    # Convert list of dicts to list of tuples, ensuring correct column order
    tuples_to_insert = []
    for row_dict in data:
        try:
            tuples_to_insert.append(tuple(row_dict[col] for col in column_order))
        except KeyError as e:
            logger.error(f"Missing key {e} in row: {row_dict} for table {table_name}")
            # Potentially skip row or raise error, for now logging and skipping problematic part
            # To be safe, we'll skip the whole chunk if one row is bad to avoid partial inserts
            # or data corruption due to misaligned columns.
            logger.error(f"Skipping chunk for {table_name} due to data error.")
            return


    if not tuples_to_insert:
        logger.warning(f"No valid data to insert into {table_name} after validation.")
        return

    placeholders = ", ".join(["?"] * len(column_order))
    insert_query = f"INSERT INTO {table_name} ({', '.join(column_order)}) VALUES ({placeholders})"

    for i in range(0, len(tuples_to_insert), chunk_size):
        chunk = tuples_to_insert[i : i + chunk_size]
        try:
            conn.executemany(insert_query, chunk)
            logger.debug(f"Inserted chunk of {len(chunk)} rows into {table_name}")
        except Exception as e:
            logger.error(f"Error inserting chunk into {table_name}: {e}")
            # Depending on the error, might want to retry or handle specific exceptions
    logger.info(f"Finished bulk inserting into {table_name}")


def process_term(
    term: schemas.TermSchema,
) -> Dict[str, Any]:
    logger.info(f"Processing {term.number} term")
    term_id = utils.get_term_nk(term=term)
    return {
        "id": term_id,
        "number": term.number,
        "from_date": term.from_date,
        "to_date": term.to_date,
    }


def process_sitting(
    sitting: schemas.SittingSchema,
    term: schemas.TermSchema,
) -> Optional[Dict[str, Any]]:
    logger.info(f"Processing sitting {sitting.number} in {term.number} term")
    if sitting.number == 0:  # skip planned sittings
        logger.info(
            f"Sitting {sitting.number} in {term.number} "
            "term is planned sitting, skipping"
        )
        return None

    term_id = utils.get_term_nk(term)
    sitting_id = utils.get_sitting_nk(sitting=sitting, term=term)
    return {
        "id": sitting_id,
        "term_id": term_id,
        "title": sitting.title,
        "number": sitting.number,
    }


def process_voting(
    voting: schemas.VotingSchema,
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
) -> Dict[str, Any]:
    logger.info(
        f"Processing voting {voting.number} in sitting {sitting.number} "
        f"in {term.number} term"
    )
    voting_id = utils.get_voting_nk(voting=voting, term=term, sitting=sitting)
    sitting_id = utils.get_sitting_nk(sitting=sitting, term=term)
    return {
        "id": voting_id,
        "sitting_id": sitting_id,
        "sitting_day": voting.sitting_day,
        "number": voting.number,
        "date": voting.date,
        "title": voting.title,
        "description": voting.description,
        "topic": voting.topic,
    }


def process_voting_option(
    voting_option: schemas.VotingOptionSchema,
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
    voting: schemas.VotingSchema,
) -> Dict[str, Any]:
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
    return {
        "id": voting_option_id,
        "voting_id": voting_id,
        "index": voting_option.index,
        "description": voting_option.description,
    }


def process_vote(
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
    voting: schemas.VotingSchema,
    vote: schemas.MpVoteSchema,
    mp_id: str,  # mp_id is now passed as an argument
) -> List[Dict[str, Any]]:
    processed_votes = []

    inner_votes = vote.votes
    if inner_votes is None:
        if vote.vote == "VOTE_VALID":
            # This case was raising TypeError, indicates an invalid state.
            # Log an error and skip this vote record.
            logger.error(
                f"Invalid vote data: 'votes' is None but 'vote' is 'VOTE_VALID'. "
                f"MP Term ID: {vote.mp_term_id}, Voting: {voting.number}, "
                f"Sitting: {sitting.number}, Term: {term.number}"
            )
            return [] # Return empty list for this invalid vote
        # If 'votes' is None and 'vote' is not 'VOTE_VALID', it's a single vote
        inner_votes = {schemas.OptionIndex(1): vote.vote}

    for inner_vote_index, inner_vote_value in inner_votes.items():
        logger.info(
            f"Processing vote for MP {mp_id} (in-term ID {vote.mp_term_id}) "
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

        vote_data = {
            "id": inner_vote_id,
            "voting_option_id": voting_option_id,
            "mp_id": mp_id,
            "vote": inner_vote_value,
            "party": vote.party,
        }
        processed_votes.append(vote_data)
    return processed_votes


def process_mp(
    mp: schemas.MpSchema,
) -> Dict[str, Any]:  # Term is not needed here as mp_id is globally unique across terms based on schema
    logger.info(
        f"Processing MP {mp.first_name} {mp.last_name} (in-term ID {mp.in_term_id})"
    )
    mp_id = utils.get_mp_nk(mp=mp)
    return {
        "id": mp_id,
        "first_name": mp.first_name,
        "second_name": mp.second_name,
        "last_name": mp.last_name,
        "birth_date": mp.birth_date,
        "birth_place": mp.birth_place,
    }


def process_mp_to_term_link(
    mp: schemas.MpSchema,
    term: schemas.TermSchema,
) -> Dict[str, Any]:
    logger.info(
        f"Processing MP to term link for MP {mp.first_name} {mp.last_name} "
        f"(in-term ID {mp.in_term_id}) in {term.number} term"
    )
    mp_to_term_link_id = utils.get_mp_to_term_link_nk(mp=mp, term=term)
    term_id = utils.get_term_nk(term)
    mp_id = utils.get_mp_nk(mp)
    return {
        "id": mp_to_term_link_id,
        "mp_id": mp_id,
        "term_id": term_id,
        "in_term_id": mp.in_term_id,
        "education": mp.education,
        "profession": mp.profession,
        "voivodeship": mp.voivodeship,
        "district_name": mp.district_name,
        "inactivity_cause": mp.inactivity_cause,
        "inactivity_description": mp.inactivity_description,
    }
