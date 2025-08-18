import os
from datetime import date

from loguru import logger
from sqlmodel import Field, SQLModel, create_engine

from sejm_scraper import api_schemas

DEBUG = os.getenv("SEJM_SCRAPER_DEBUG", None) == "true"

DUCKDB_URL = "duckdb:///" + os.getenv("DUCKDB_PATH", "sejm_scraper.duckdb")
ENGINE = create_engine(DUCKDB_URL, echo=DEBUG)


class Term(SQLModel, table=True):
    id: str = Field(primary_key=True)
    number: int
    from_date: date
    to_date: date | None


class Sitting(SQLModel, table=True):
    id: str = Field(primary_key=True)
    term_id: str = Field(foreign_key="term.id")
    title: str
    number: int


class Voting(SQLModel, table=True):
    id: str = Field(primary_key=True)
    sitting_id: str = Field(foreign_key="sitting.id")
    number: int
    day_number: int
    date: date
    title: str
    description: str | None
    topic: str | None


class VotingOption(SQLModel, table=True):
    id: str = Field(primary_key=True)
    voting_id: str = Field(foreign_key="voting.id")
    index: int
    description: str | None


class Vote(SQLModel, table=True):
    id: str = Field(primary_key=True)
    voting_option_id: str = Field(foreign_key="votingoption.id")
    mp_in_term_id: str = Field(foreign_key="mpinterm.id")
    party_in_term_id: str | None = Field(foreign_key="partyinterm.id")
    vote: api_schemas.Vote


class MpInTerm(SQLModel, table=True):
    id: str = Field(primary_key=True)
    term_id: str = Field(foreign_key="term.id")
    in_term_id: int
    first_name: str
    second_name: str | None
    last_name: str
    birth_date: date
    birth_place: str | None
    education: str | None
    profession: str | None
    voivodeship: str | None
    district_name: str
    inactivity_cause: str | None
    inactivity_description: str | None


class PartyInTerm(SQLModel, table=True):
    id: str = Field(primary_key=True)
    term_id: str = Field(foreign_key="term.id")
    abbreviation: str
    name: str
    phone: str | None
    fax: str | None
    email: str | None
    member_count: int


def prepare_db_and_tables() -> None:
    logger.info("Preparing database and tables")
    SQLModel.metadata.create_all(ENGINE)
    logger.debug("Successfully prepared database and tables")
