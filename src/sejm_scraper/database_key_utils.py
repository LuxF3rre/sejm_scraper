from hashlib import sha256
from typing import Any

from sejm_scraper import api_schemas, database


def _generate_hash(
    *s: Any,
) -> str:
    to_hash = [str(x) for x in s if x is not None]
    to_hash_bytes = "".join(to_hash).encode("utf-8")
    sha256_hash = sha256()
    sha256_hash.update(to_hash_bytes)
    hex_hash = sha256_hash.hexdigest()
    return hex_hash


def generate_term_natural_key(
    term: api_schemas.TermSchema | database.Term,
) -> str:
    return _generate_hash(term.number)


def generate_sitting_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
) -> str:
    return _generate_hash(term.number, sitting.number)


def generate_voting_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    voting: api_schemas.VotingSchema | database.Voting,
) -> str:
    return _generate_hash(
        term.number,
        sitting.number,
        voting.day_number,
        voting.number,
        voting.date,
    )


def generate_voting_option_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    voting: api_schemas.VotingSchema | database.Voting,
    voting_option_index: api_schemas.OptionIndex,
) -> str:
    return _generate_hash(
        term.number,
        sitting.number,
        voting.day_number,
        voting.number,
        voting.date,
        voting_option_index,
    )


def generate_vote_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    voting: api_schemas.VotingSchema | database.Voting,
    voting_option_index: api_schemas.OptionIndex,
    mp_term_id: api_schemas.MpTermId,
) -> str:
    return _generate_hash(
        term.number,
        sitting.number,
        voting.day_number,
        voting.number,
        voting.date,
        voting_option_index,
        mp_term_id,
    )


def generate_mp_natural_key(
    mp: api_schemas.MpSchema | database.Mp,
) -> str:
    return _generate_hash(
        mp.first_name,
        mp.last_name,
        mp.birth_date,
        mp.birth_place,
    )


def generate_mp_to_term_link_natural_key(
    mp: api_schemas.MpSchema | database.Mp,
    term: api_schemas.TermSchema | database.Term,
) -> str:
    return _generate_hash(
        mp.first_name,
        mp.last_name,
        mp.birth_date,
        mp.birth_place,
        term.number,
    )
