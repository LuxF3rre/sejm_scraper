import json
import os
import tempfile
from collections.abc import Sequence
from datetime import date
from typing import Union

import sqlmodel
from sqlalchemy import Engine
from sqlmodel import Field, SQLModel, create_engine

from sejm_scraper.api_schemas import Vote

DEFAULT_DUCKDB_URL = "duckdb:///sejm_scraper.duckdb"


def get_engine(*, url: str = DEFAULT_DUCKDB_URL, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine for the database.

    Args:
        url: Database connection URL.
        echo: Whether to log SQL statements.

    Returns:
        A configured SQLAlchemy engine.
    """
    return create_engine(url, echo=echo)


class Term(SQLModel, table=True):
    id: str = Field(primary_key=True)
    number: int
    from_date: date
    to_date: Union[date, None]


class Sitting(SQLModel, table=True):
    id: str = Field(primary_key=True)
    term_id: str = Field(foreign_key="term.id")
    title: str
    number: int


class SittingDay(SQLModel, table=True):
    id: str = Field(primary_key=True)
    sitting_id: str = Field(foreign_key="sitting.id")
    date: date


class Voting(SQLModel, table=True):
    id: str = Field(primary_key=True)
    sitting_id: str = Field(foreign_key="sitting.id")
    sitting_day: int
    number: int
    date: date
    title: str
    description: Union[str, None]
    topic: Union[str, None]
    kind: str
    yes: int
    no: int
    abstain: int
    not_participating: int
    present: int
    total_voted: int
    majority_type: str
    majority_votes: int
    against_all: Union[int, None]


class VotingOption(SQLModel, table=True):
    id: str = Field(primary_key=True)
    voting_id: str = Field(foreign_key="voting.id")
    index: int
    option_label: Union[str, None]
    description: Union[str, None]
    votes: int


class Club(SQLModel, table=True):
    id: str = Field(primary_key=True)
    term_id: str = Field(foreign_key="term.id")
    club_id: str
    name: str
    phone: Union[str, None]
    fax: Union[str, None]
    email: Union[str, None]
    members_count: int


class Mp(SQLModel, table=True):
    id: str = Field(primary_key=True)
    first_name: str
    second_name: Union[str, None]
    last_name: str
    birth_date: date
    birth_place: Union[str, None]


class VoteRecord(SQLModel, table=True):
    id: str = Field(primary_key=True)
    voting_option_id: str = Field(foreign_key="votingoption.id")
    mp_term_id: int
    vote: Vote
    party: Union[str, None]


class MpToTermLink(SQLModel, table=True):
    id: str = Field(primary_key=True)
    mp_id: str = Field(foreign_key="mp.id")
    term_id: str = Field(foreign_key="term.id")
    in_term_id: int
    active: bool
    club: Union[str, None]
    district_num: int
    number_of_votes: int
    email: Union[str, None]
    education: Union[str, None]
    profession: Union[str, None]
    voivodeship: Union[str, None]
    district_name: str
    inactivity_cause: Union[str, None]
    inactivity_description: Union[str, None]


def bulk_upsert(
    *,
    session: sqlmodel.Session,
    model: type[SQLModel],
    records: Sequence[SQLModel],
) -> None:
    """Bulk upsert records using DuckDB's vectorized INSERT OR REPLACE.

    SQLAlchemy's ORM `session.merge()` / `session.add()` issues one
    INSERT per row, which is extremely slow for the thousands of vote
    records per sitting. This function bypasses that path entirely by
    serialising records to a JSON temp file and loading them in a single
    `INSERT OR REPLACE ... SELECT FROM read_json_auto(...)` statement,
    letting DuckDB handle the bulk load with its vectorized execution
    engine.

    Args:
        session: Active SQLModel session.
        model: The SQLModel table class.
        records: Instances to upsert.
    """
    if not records:
        return
    table = model.__table__  # type: ignore[attr-defined]  # SQLModel tables have __table__ at runtime
    columns = [col.name for col in table.columns]
    col_names = ", ".join(columns)

    rows = [
        {col: _json_safe(getattr(r, col)) for col in columns} for r in records
    ]

    # duckdb-engine's ConnectionWrapper proxies attribute access to
    # the raw DuckDBPyConnection via __getattr__, so .execute()
    # works directly against the native DuckDB connection.
    dbapi_conn = session.connection().connection.dbapi_connection

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as f:
        json.dump(rows, f)
        tmp_path = f.name

    try:
        path = tmp_path.replace("\\", "/")
        dbapi_conn.execute(  # type: ignore[union-attr]  # guaranteed non-None inside active session
            f"INSERT OR REPLACE INTO {table.name} ({col_names}) "  # noqa: S608
            f"SELECT {col_names} FROM read_json_auto('{path}')"
        )
    finally:
        os.unlink(tmp_path)


def _json_safe(value: object) -> object:
    """Convert values that `json.dump` cannot serialise natively."""
    if isinstance(value, date):
        return value.isoformat()
    return value


def create_db_and_tables(*, engine: Engine | None = None) -> None:
    """Create all database tables.

    Args:
        engine: SQLAlchemy engine to use. Defaults to a new engine
            with the default DuckDB URL.
    """
    if engine is None:
        engine = get_engine()
    SQLModel.metadata.create_all(engine)
