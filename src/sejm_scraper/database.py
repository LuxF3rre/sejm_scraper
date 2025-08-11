import os
from datetime import date
from typing import Union

from loguru import logger
from sqlmodel import Field, SQLModel, create_engine

from sejm_scraper import api_schemas

DEBUG = bool(os.getenv("SEJM_SCRAPER_DEBUG", None))

DUCKDB_URL = "duckdb:///sejm_scraper.duckdb"
ENGINE = create_engine(DUCKDB_URL, echo=DEBUG)


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


class Voting(SQLModel, table=True):
    id: str = Field(primary_key=True)
    sitting_id: str = Field(foreign_key="sitting.id")
    number: int
    day_number: int
    date: date
    title: str
    description: Union[str, None]
    topic: Union[str, None]


class VotingOption(SQLModel, table=True):
    id: str = Field(primary_key=True)
    voting_id: str = Field(foreign_key="voting.id")
    index: int
    description: Union[str, None]


class Vote(SQLModel, table=True):
    id: str = Field(primary_key=True)
    voting_option_id: str = Field(foreign_key="votingoption.id")
    mp_in_term_id: str = Field(foreign_key="mpinterm.id")
    vote: api_schemas.Vote
    party: Union[str, None]


class MpInTerm(SQLModel, table=True):
    id: str = Field(primary_key=True)
    term_id: str = Field(foreign_key="term.id")
    in_term_id: int
    first_name: str
    second_name: Union[str, None]
    last_name: str
    birth_date: date
    birth_place: Union[str, None]
    education: Union[str, None]
    profession: Union[str, None]
    voivodeship: Union[str, None]
    district_name: str
    inactivity_cause: Union[str, None]
    inactivity_description: Union[str, None]


def create_db_and_tables() -> None:
    logger.info("Creating database and tables")
    SQLModel.metadata.create_all(ENGINE)
    logger.debug("Successfully created database and tables")
