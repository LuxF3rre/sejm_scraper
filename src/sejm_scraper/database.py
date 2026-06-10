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
    # Nullable because a vote may reference an MP missing from the
    # term's MP list (the API is not consistent about this).
    mp_to_term_link_id: Union[str, None] = Field(
        default=None, foreign_key="mptotermlink.id"
    )
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
    """Bulk upsert records using DuckDB's INSERT OR REPLACE.

    SQLAlchemy's ORM `session.merge()` / `session.add()` issues one
    INSERT per row through the full ORM machinery, which is extremely
    slow for the thousands of vote records per sitting. This function
    bypasses that path by executing a single prepared
    `INSERT OR REPLACE` statement over all rows with `executemany` on
    the native DuckDB connection. Parameter binding also avoids the
    type-inference pitfalls of file-based loading (`read_json_auto`).

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
    placeholders = ", ".join(["?"] * len(columns))

    rows = [[getattr(r, col) for col in columns] for r in records]

    # duckdb-engine's ConnectionWrapper proxies attribute access to
    # the raw DuckDBPyConnection via __getattr__, so .executemany()
    # works directly against the native DuckDB connection.
    dbapi_conn = session.connection().connection.dbapi_connection
    dbapi_conn.executemany(  # type: ignore[union-attr]  # guaranteed non-None inside active session
        f"INSERT OR REPLACE INTO {table.name} ({col_names}) "  # noqa: S608
        f"VALUES ({placeholders})",
        rows,
    )


def create_db_and_tables(*, engine: Engine | None = None) -> None:
    """Create all database tables.

    Args:
        engine: SQLAlchemy engine to use. Defaults to a new engine
            with the default DuckDB URL.
    """
    if engine is None:
        engine = get_engine()
    SQLModel.metadata.create_all(engine)
