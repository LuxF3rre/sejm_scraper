from datetime import UTC, date, datetime

from sejm_scraper import api_schemas, database, database_key_utils


def test_normalize_value_none() -> None:
    assert database_key_utils._normalize_value(None) == ""


def test_normalize_value_date() -> None:
    assert (
        database_key_utils._normalize_value(date(2025, 1, 15)) == "2025-01-15"
    )


def test_normalize_value_datetime_uses_date_part() -> None:
    dt = datetime(2025, 8, 5, 16, 32, 45, tzinfo=UTC)
    assert database_key_utils._normalize_value(dt) == "2025-08-05"


def test_normalize_value_string() -> None:
    assert database_key_utils._normalize_value("hello") == "hello"


def test_normalize_value_int() -> None:
    assert database_key_utils._normalize_value(42) == "42"


def test_generate_hash_deterministic() -> None:
    h1 = database_key_utils._generate_hash("a", "b")
    h2 = database_key_utils._generate_hash("a", "b")
    assert h1 == h2


def test_generate_hash_different_inputs() -> None:
    h1 = database_key_utils._generate_hash("a", "b")
    h2 = database_key_utils._generate_hash("b", "a")
    assert h1 != h2


def test_generate_hash_separator_prevents_collision() -> None:
    h1 = database_key_utils._generate_hash("ab", "c")
    h2 = database_key_utils._generate_hash("a", "bc")
    assert h1 != h2


def test_generate_term_natural_key() -> None:
    term = api_schemas.TermSchema.model_validate(
        {"num": 10, "from": "2023-11-13"}
    )
    key = database_key_utils.generate_term_natural_key(term=term)
    assert len(key) == 64  # SHA-256 hex


def test_generate_term_natural_key_from_db_model() -> None:
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    key = database_key_utils.generate_term_natural_key(term=term)
    assert len(key) == 64


def test_generate_term_key_schema_and_model_match() -> None:
    schema = api_schemas.TermSchema.model_validate(
        {"num": 10, "from": "2023-11-13"}
    )
    model = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    assert database_key_utils.generate_term_natural_key(
        term=schema
    ) == database_key_utils.generate_term_natural_key(term=model)


def test_generate_sitting_natural_key() -> None:
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    sitting = database.Sitting(id="", term_id="", title="Test", number=39)
    key = database_key_utils.generate_sitting_natural_key(
        term=term, sitting=sitting
    )
    assert len(key) == 64


def test_generate_sitting_day_natural_key() -> None:
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    sitting = database.Sitting(id="", term_id="", title="Test", number=39)
    key = database_key_utils.generate_sitting_day_natural_key(
        term=term, sitting=sitting, day_date=date(2025, 7, 22)
    )
    assert len(key) == 64


def test_generate_voting_natural_key() -> None:
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    sitting = database.Sitting(id="", term_id="", title="Test", number=39)
    voting = database.Voting(
        id="",
        sitting_id="",
        sitting_day=6,
        number=205,
        date=date(2025, 8, 5),
        title="Test",
        description=None,
        topic=None,
        kind="ELECTRONIC",
        yes=0,
        no=0,
        abstain=0,
        not_participating=0,
        present=0,
        total_voted=0,
        majority_type="SIMPLE_MAJORITY",
        majority_votes=0,
        against_all=None,
    )
    key = database_key_utils.generate_voting_natural_key(
        term=term, sitting=sitting, voting=voting
    )
    assert len(key) == 64


def test_generate_voting_option_natural_key() -> None:
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    sitting = database.Sitting(id="", term_id="", title="Test", number=39)
    voting = database.Voting(
        id="",
        sitting_id="",
        sitting_day=6,
        number=205,
        date=date(2025, 8, 5),
        title="Test",
        description=None,
        topic=None,
        kind="ELECTRONIC",
        yes=0,
        no=0,
        abstain=0,
        not_participating=0,
        present=0,
        total_voted=0,
        majority_type="SIMPLE_MAJORITY",
        majority_votes=0,
        against_all=None,
    )
    key = database_key_utils.generate_voting_option_natural_key(
        term=term,
        sitting=sitting,
        voting=voting,
        voting_option_index=api_schemas.OptionIndex(1),
    )
    assert len(key) == 64


def test_generate_mp_natural_key() -> None:
    mp = database.Mp(
        id="",
        first_name="Andrzej",
        second_name="Mieczysław",
        last_name="Adamczyk",
        birth_date=date(1959, 1, 4),
        birth_place="Krzeszowice",
    )
    key = database_key_utils.generate_mp_natural_key(mp=mp)
    assert len(key) == 64


def test_generate_mp_to_term_link_natural_key() -> None:
    mp = database.Mp(
        id="",
        first_name="Andrzej",
        second_name=None,
        last_name="Adamczyk",
        birth_date=date(1959, 1, 4),
        birth_place=None,
    )
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    key = database_key_utils.generate_mp_to_term_link_natural_key(
        mp=mp, term=term
    )
    assert len(key) == 64


def test_generate_club_natural_key() -> None:
    club = database.Club(
        id="",
        term_id="",
        club_id="PiS",
        name="Prawo i Sprawiedliwość",
        phone=None,
        fax=None,
        email=None,
        members_count=189,
    )
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    key = database_key_utils.generate_club_natural_key(club=club, term=term)
    assert len(key) == 64


def test_generate_vote_natural_key() -> None:
    term = database.Term(
        id="", number=10, from_date=date(2023, 11, 13), to_date=None
    )
    sitting = database.Sitting(id="", term_id="", title="Test", number=39)
    voting = database.Voting(
        id="",
        sitting_id="",
        sitting_day=6,
        number=205,
        date=date(2025, 8, 5),
        title="Test",
        description=None,
        topic=None,
        kind="ELECTRONIC",
        yes=0,
        no=0,
        abstain=0,
        not_participating=0,
        present=0,
        total_voted=0,
        majority_type="SIMPLE_MAJORITY",
        majority_votes=0,
        against_all=None,
    )
    key = database_key_utils.generate_vote_natural_key(
        term=term,
        sitting=sitting,
        voting=voting,
        voting_option_index=api_schemas.OptionIndex(1),
        mp_term_id=api_schemas.MpTermId(1),
    )
    assert len(key) == 64
