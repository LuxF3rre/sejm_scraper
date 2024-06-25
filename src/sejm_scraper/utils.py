from hashlib import sha256
from typing import Any

from sejm_scraper import schemas


def _get_surogate_key(*s: Any) -> str:
    to_hash = [str(x) for x in s if x is not None]
    to_hash_bytes = "".join(to_hash).encode("utf-8")
    sha256_hash = sha256()
    sha256_hash.update(to_hash_bytes)
    hex_hash = sha256_hash.hexdigest()
    return hex_hash


def get_term_sk(
    term: schemas.TermSchema,
) -> str:
    return _get_surogate_key(term.number)


def get_sitting_sk(
    sitting: schemas.SittingSchema,
    term: schemas.TermSchema,
) -> str:
    return _get_surogate_key(term.number, sitting.number)


def get_voting_sk(
    voting: schemas.VotingSchema,
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
) -> str:
    return _get_surogate_key(
        term.number,
        sitting.number,
        voting.sitting_day,
        voting.voting_number,
        voting.date,
    )


def get_voting_option_sk(
    voting_option_index: schemas.OptionIndex,
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
    voting: schemas.VotingSchema,
) -> str:
    return _get_surogate_key(
        term.number,
        sitting.number,
        voting.sitting_day,
        voting.voting_number,
        voting.date,
        voting_option_index,
    )


def get_vote_sk(
    term: schemas.TermSchema,
    sitting: schemas.SittingSchema,
    voting: schemas.VotingSchema,
    voting_option_index: schemas.OptionIndex,
    mp_id: str,
) -> str:
    return _get_surogate_key(
        term.number,
        sitting.number,
        voting.sitting_day,
        voting.voting_number,
        voting.date,
        voting_option_index,
        mp_id,
    )


def get_mp_sk(
    mp: schemas.MpSchema,
) -> str:
    return _get_surogate_key(
        mp.first_name,
        mp.last_name,
        mp.birth_date,
        mp.birth_place,
    )


def get_mp_to_term_link_sk(
    mp: schemas.MpSchema,
    term: schemas.TermSchema,
) -> str:
    return _get_surogate_key(
        mp.first_name,
        mp.last_name,
        mp.birth_date,
        mp.birth_place,
        term.number,
    )
