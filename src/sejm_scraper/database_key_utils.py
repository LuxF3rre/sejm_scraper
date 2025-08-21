from datetime import date
from hashlib import sha256

from sejm_scraper import api_schemas, database


def _generate_hash(
    *s: str | int | float | bool | date,
) -> str:
    for x in s:
        if not str(x):
            msg = "hash elements cannot be empty"
            raise ValueError(msg)
    to_hash = [str(x) for x in s]
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
        voting.date_time,
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
        voting.date_time,
        voting_option_index,
    )


def generate_vote_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    voting: api_schemas.VotingSchema | database.Voting,
    voting_option_index: api_schemas.OptionIndex,
    mp_in_term_id: api_schemas.MpInTermId,
) -> str:
    return _generate_hash(
        term.number,
        sitting.number,
        voting.day_number,
        voting.number,
        voting.date_time,
        voting_option_index,
        mp_in_term_id,
    )


def generate_mp_in_term_natural_key(
    term: api_schemas.TermSchema | database.Term,
    mp: api_schemas.MpInTermSchema | database.MpInTerm | api_schemas.MpInTermId,
) -> str:
    if isinstance(mp, int):  # api_schemas.MpInTermId)
        return _generate_hash(
            term.number,
            mp,
        )
    return _generate_hash(
        term.number,
        mp.in_term_id,
    )


def generate_party_in_term_natural_key(
    term: api_schemas.TermSchema | database.Term,
    party_in_term: api_schemas.PartyInSchema
    | database.PartyInTerm
    | api_schemas.PartyAbbreviation,
) -> str:
    if isinstance(party_in_term, api_schemas.PartyInSchema):
        return _generate_hash(
            term.number,
            party_in_term.id,
        )
    if isinstance(party_in_term, database.PartyInTerm):
        return _generate_hash(
            term.number,
            party_in_term.abbreviation,
        )
    return _generate_hash(  # api_schemas.PartyAbbreviation
        term.number,
        party_in_term,
    )
