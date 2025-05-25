import os

from sqlalchemy import (
    CHAR,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

database_name = os.getenv("SEJM_SCRAPER_DATABASE", "postgres")
user = os.getenv("SEJM_SCRAPER_USER", "postgres")
password = os.getenv("SEJM_SCRAPER_PASSWORD", "postgres")
host = os.getenv("SEJM_SCRAPER_HOST", "localhost")
port = os.getenv("SEJM_SCRAPER_PORT", "5432")

engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{database_name}",
)

SessionMaker = sessionmaker(bind=engine)

Base = declarative_base()


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
