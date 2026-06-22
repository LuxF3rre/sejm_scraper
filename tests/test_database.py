from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import sqlmodel

from sejm_scraper import database
from sejm_scraper.api_schemas import Vote

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Engine


def test_json_safe_date() -> None:
    assert database._json_safe(date(2025, 1, 15)) == "2025-01-15"


def test_json_safe_string() -> None:
    assert database._json_safe("hello") == "hello"


def test_json_safe_none() -> None:
    assert database._json_safe(None) is None


def test_json_safe_int() -> None:
    assert database._json_safe(42) == 42


def test_bulk_upsert_empty_records(engine: "Engine") -> None:
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(session=session, model=database.Term, records=[])


def test_bulk_upsert_inserts_records(engine: "Engine") -> None:
    term = database.Term(
        id="abc123",
        number=10,
        from_date=date(2023, 11, 13),
        to_date=None,
    )
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term],
        )
        session.commit()

    with sqlmodel.Session(engine) as session:
        result = session.exec(sqlmodel.select(database.Term)).first()
        assert result is not None
        assert result.id == "abc123"
        assert result.number == 10


def test_bulk_upsert_stamps_loaded_at(engine: "Engine") -> None:
    """Every inserted row gets a naive UTC loaded_at timestamp."""
    before = datetime.now(UTC).replace(tzinfo=None)
    term = database.Term(
        id="abc123",
        number=10,
        from_date=date(2023, 11, 13),
        to_date=None,
    )
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term],
        )
        session.commit()
    after = datetime.now(UTC).replace(tzinfo=None)

    with sqlmodel.Session(engine) as session:
        result = session.exec(sqlmodel.select(database.Term)).first()
        assert result is not None
        assert result.loaded_at is not None
        # Stored as naive UTC wall-clock time.
        assert result.loaded_at.tzinfo is None
        assert before <= result.loaded_at <= after


def test_bulk_upsert_refreshes_loaded_at_on_merge(engine: "Engine") -> None:
    """Re-upserting a row updates loaded_at, ignoring the record value."""
    stale = datetime(2000, 1, 1)  # noqa: DTZ001  # naive UTC to match column
    term = database.Term(
        id="abc123",
        number=10,
        from_date=date(2023, 11, 13),
        to_date=None,
        loaded_at=stale,
    )
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term],
        )
        session.commit()

    with sqlmodel.Session(engine) as session:
        result = session.exec(sqlmodel.select(database.Term)).first()
        assert result is not None
        assert result.loaded_at is not None
        # The stale value carried on the record must be overridden.
        assert result.loaded_at > stale


def test_bulk_upsert_replaces_on_duplicate(engine: "Engine") -> None:
    term_v1 = database.Term(
        id="abc123",
        number=10,
        from_date=date(2023, 11, 13),
        to_date=None,
    )
    term_v2 = database.Term(
        id="abc123",
        number=10,
        from_date=date(2023, 11, 13),
        to_date=date(2027, 11, 12),
    )
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term_v1],
        )
        session.commit()

    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term_v2],
        )
        session.commit()

    with sqlmodel.Session(engine) as session:
        results = session.exec(sqlmodel.select(database.Term)).all()
        assert len(results) == 1
        assert results[0].to_date == date(2027, 11, 12)


def test_bulk_upsert_binds_enums_dates_and_nulls(
    engine: "Engine",
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    """Native parameter binding must handle StrEnum, date, and None."""
    option = database.VotingOption(
        id="opt1",
        voting_id=voting.id,
        index=1,
        option_label=None,
        description=None,
        votes=0,
    )
    vote_record = database.VoteRecord(
        id="vote1",
        voting_option_id="opt1",
        mp_to_term_link_id=None,
        mp_term_id=1,
        vote=Vote.YES,
        party=None,
    )
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session, model=database.Term, records=[term]
        )
        database.bulk_upsert(
            session=session, model=database.Sitting, records=[sitting]
        )
        database.bulk_upsert(
            session=session, model=database.Voting, records=[voting]
        )
        database.bulk_upsert(
            session=session, model=database.VotingOption, records=[option]
        )
        database.bulk_upsert(
            session=session, model=database.VoteRecord, records=[vote_record]
        )
        session.commit()

    with sqlmodel.Session(engine) as session:
        result = session.exec(sqlmodel.select(database.VoteRecord)).first()
        assert result is not None
        assert result.vote == Vote.YES
        assert result.party is None
        assert result.mp_to_term_link_id is None
        stored_voting = session.exec(sqlmodel.select(database.Voting)).first()
        assert stored_voting is not None
        assert stored_voting.date == voting.date


def test_create_db_and_tables_with_engine(engine: "Engine") -> None:
    # engine fixture already calls create_db_and_tables,
    # verify tables exist by inserting a record
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[
                database.Term(
                    id="t1",
                    number=1,
                    from_date=date(2000, 1, 1),
                    to_date=None,
                )
            ],
        )
        session.commit()

    with sqlmodel.Session(engine) as session:
        result = session.exec(sqlmodel.select(database.Term)).first()
        assert result is not None
