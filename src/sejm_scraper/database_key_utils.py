from datetime import date, datetime
from hashlib import sha256
from typing import Any

from sejm_scraper import api_schemas, database


def _normalize_value(x: Any) -> str:
    if x is None:
        return ""
    # datetime is a subclass of date, so check datetime first
    if isinstance(x, datetime):
        return str(x.date())
    return str(x)


def _generate_hash(
    *s: Any,
) -> str:
    to_hash = [_normalize_value(x) for x in s]
    to_hash_bytes = "||".join(to_hash).encode("utf-8")
    return sha256(to_hash_bytes).hexdigest()


def generate_term_natural_key(
    term: api_schemas.TermSchema | database.Term,
) -> str:
    """Generate a deterministic SHA-256 key for a term.

    Args:
        term: Term schema or database model.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(term.number)


def generate_sitting_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
) -> str:
    """Generate a deterministic SHA-256 key for a sitting.

    Args:
        term: Term schema or database model.
        sitting: Sitting schema or database model.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(term.number, sitting.number)


def generate_sitting_day_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    day_date: date,
) -> str:
    """Generate a deterministic SHA-256 key for a sitting day.

    Args:
        term: Term schema or database model.
        sitting: Sitting schema or database model.
        day_date: Date of the sitting day.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(term.number, sitting.number, day_date)


def generate_voting_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    voting: api_schemas.VotingSchema | database.Voting,
) -> str:
    """Generate a deterministic SHA-256 key for a voting.

    Args:
        term: Term schema or database model.
        sitting: Sitting schema or database model.
        voting: Voting schema or database model.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(
        term.number,
        sitting.number,
        voting.sitting_day,
        voting.number,
        voting.date,
    )


def generate_voting_option_natural_key(
    term: api_schemas.TermSchema | database.Term,
    sitting: api_schemas.SittingSchema | database.Sitting,
    voting: api_schemas.VotingSchema | database.Voting,
    voting_option_index: api_schemas.OptionIndex,
) -> str:
    """Generate a deterministic SHA-256 key for a voting option.

    Args:
        term: Term schema or database model.
        sitting: Sitting schema or database model.
        voting: Voting schema or database model.
        voting_option_index: Index of the voting option.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(
        term.number,
        sitting.number,
        voting.sitting_day,
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
    """Generate a deterministic SHA-256 key for an individual vote.

    Args:
        term: Term schema or database model.
        sitting: Sitting schema or database model.
        voting: Voting schema or database model.
        voting_option_index: Index of the voting option.
        mp_term_id: MP's term-specific identifier.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(
        term.number,
        sitting.number,
        voting.sitting_day,
        voting.number,
        voting.date,
        voting_option_index,
        mp_term_id,
    )


def generate_club_natural_key(
    club: api_schemas.ClubSchema | database.Club,
    term: api_schemas.TermSchema | database.Term,
) -> str:
    """Generate a deterministic SHA-256 key for a club.

    Args:
        club: Club schema or database model.
        term: Term schema or database model.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(term.number, club.club_id)


def generate_mp_natural_key(
    mp: api_schemas.MpSchema | database.Mp,
) -> str:
    """Generate a deterministic SHA-256 key for an MP.

    Args:
        mp: MP schema or database model.

    Returns:
        Hex-encoded SHA-256 hash.
    """
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
    """Generate a deterministic SHA-256 key for an MP-to-term link.

    Args:
        mp: MP schema or database model.
        term: Term schema or database model.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return _generate_hash(
        mp.first_name,
        mp.last_name,
        mp.birth_date,
        mp.birth_place,
        term.number,
    )
