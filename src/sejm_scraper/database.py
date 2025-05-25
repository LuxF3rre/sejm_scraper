import duckdb
import os  # Added import
from sqlalchemy import (
    CHAR,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import declarative_base, relationship

# DUCKDB_FILE is now initialized from an environment variable, with a default.
DUCKDB_FILE = os.getenv("SEJM_SCRAPER_DUCKDB_FILE", "sejm_scraper.duckdb")

def get_duckdb_connection():
    # This function will now use the potentially overridden DUCKDB_FILE
    return duckdb.connect(DUCKDB_FILE, read_only=False)

Base = declarative_base()

def create_tables_if_not_exists(conn):
    """
    Creates tables in the DuckDB database based on the SQLAlchemy models
    if they do not already exist.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Terms (
            id VARCHAR PRIMARY KEY,
            number INTEGER NOT NULL,
            from_date DATE NOT NULL,
            to_date DATE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Sittings (
            id VARCHAR PRIMARY KEY,
            term_id VARCHAR NOT NULL REFERENCES Terms(id),
            title VARCHAR NOT NULL,
            number INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Votings (
            id VARCHAR PRIMARY KEY,
            sitting_id VARCHAR NOT NULL REFERENCES Sittings(id),
            sitting_day INTEGER NOT NULL,
            number INTEGER NOT NULL,
            date TIMESTAMP NOT NULL,
            title VARCHAR NOT NULL,
            description VARCHAR,
            topic VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS VotingOptions (
            id VARCHAR PRIMARY KEY,
            voting_id VARCHAR NOT NULL REFERENCES Votings(id),
            index INTEGER NOT NULL,
            description VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS MPs (
            id VARCHAR PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            second_name VARCHAR,
            last_name VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            birth_place VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Votes (
            id VARCHAR PRIMARY KEY,
            voting_option_id VARCHAR NOT NULL REFERENCES VotingOptions(id),
            mp_id VARCHAR NOT NULL REFERENCES MPs(id),
            vote VARCHAR NOT NULL,
            party VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS MpToTermLink (
            id VARCHAR PRIMARY KEY,
            mp_id VARCHAR NOT NULL REFERENCES MPs(id),
            term_id VARCHAR NOT NULL REFERENCES Terms(id),
            in_term_id INTEGER NOT NULL,
            education VARCHAR,
            profession VARCHAR,
            voivodeship VARCHAR,
            district_name VARCHAR NOT NULL,
            inactivity_cause VARCHAR,
            inactivity_description VARCHAR
        );
        """
    )


class Terms(Base):
    __tablename__ = "Terms"

    id = Column(CHAR(64), primary_key=True)

    number = Column(Integer, nullable=False)
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=True)

    sittings = relationship("Sittings", back_populates="term")
    mp_to_term_link = relationship("MpToTermLink", back_populates="term")


class Sittings(Base):
    __tablename__ = "Sittings"

    id = Column(CHAR(64), primary_key=True)
    term_id = Column(CHAR(64), ForeignKey("Terms.id"), nullable=False)

    title = Column(String, nullable=False)
    number = Column(Integer, nullable=False)

    term = relationship("Terms", back_populates="sittings")
    votings = relationship("Votings", back_populates="sitting")


class Votings(Base):
    __tablename__ = "Votings"

    id = Column(CHAR(64), primary_key=True)
    sitting_id = Column(CHAR(64), ForeignKey("Sittings.id"), nullable=False)

    sitting_day = Column(Integer, nullable=False)
    number = Column(Integer, nullable=False)
    date = Column(DateTime, nullable=False)
    title = Column(String, nullable=False)
    description = Column(String, nullable=True)
    topic = Column(String, nullable=True)

    sitting = relationship("Sittings", back_populates="votings")
    voting_options = relationship("VotingOptions", back_populates="voting")


class VotingOptions(Base):
    __tablename__ = "VotingOptions"

    id = Column(CHAR(64), primary_key=True)
    voting_id = Column(CHAR(64), ForeignKey("Votings.id"), nullable=False)

    index = Column(Integer, nullable=False)
    description = Column(String, nullable=True)

    voting = relationship("Votings", back_populates="voting_options")
    votes = relationship("Votes", back_populates="voting_option")


class Votes(Base):
    __tablename__ = "Votes"

    id = Column(CHAR(64), primary_key=True)
    voting_option_id = Column(
        CHAR(64), ForeignKey("VotingOptions.id"), nullable=False
    )
    mp_id = Column(CHAR(64), ForeignKey("MPs.id"), nullable=False)

    vote = Column(String, nullable=False)
    party = Column(String, nullable=True)

    voting_option = relationship("VotingOptions", back_populates="votes")
    mp = relationship("MPs", back_populates="votes")


class MPs(Base):
    __tablename__ = "MPs"

    id = Column(CHAR(64), primary_key=True)

    first_name = Column(String, nullable=False)
    second_name = Column(String, nullable=True)
    last_name = Column(String, nullable=False)
    birth_date = Column(Date, nullable=False)
    birth_place = Column(String, nullable=True)

    votes = relationship("Votes", back_populates="mp")
    mp_to_term_link = relationship("MpToTermLink", back_populates="mp")


class MpToTermLink(Base):
    __tablename__ = "MpToTermLink"

    id = Column(CHAR(64), primary_key=True)
    mp_id = Column(CHAR(64), ForeignKey("MPs.id"), nullable=False)
    term_id = Column(CHAR(64), ForeignKey("Terms.id"), nullable=False)

    in_term_id = Column(Integer, nullable=False)
    education = Column(String, nullable=True)
    profession = Column(String, nullable=True)
    voivodeship = Column(String, nullable=True)
    district_name = Column(String, nullable=False)
    inactivity_cause = Column(String, nullable=True)
    inactivity_description = Column(String, nullable=True)

    mp = relationship("MPs", back_populates="mp_to_term_link")
    term = relationship("Terms", back_populates="mp_to_term_link")
