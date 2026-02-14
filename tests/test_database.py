from datetime import date
from typing import TYPE_CHECKING

import sqlmodel

from sejm_scraper import database

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
