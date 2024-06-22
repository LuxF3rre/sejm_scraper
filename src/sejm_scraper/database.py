import os

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

database_name = os.getenv("SEJM_SCRAPER_DATABASE", "postgres")
user = os.getenv("SEJM_SCRAPER_USER", "postgres")
password = os.getenv("SEJM_SCRAPER_PASSWORD", "postgres")
host = os.getenv("SEJM_SCRAPER_HOST", "localhost")
port = os.getenv("SEJM_SCRAPER_PORT", "5432")

engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{database_name}",
)

SessionMaker = sessionmaker(bind=engine)

# Need to ignore the type of declariative_base for lack of better options
# See: https://stackoverflow.com/questions/58325495/what-type-do-i-use-for-sqlalchemy-declarative-base
Base = declarative_base()  # type: ignore


class Votings(Base):  # type: ignore
    __tablename__ = "Votings"

    SittingDayId = Column(Integer)
    VotingTimestamp = Column(DateTime)
    TermNumber = Column(Integer, primary_key=True)
    SittingNumber = Column(Integer, primary_key=True)
    VotingNumber = Column(Integer, primary_key=True)
    SittingUrl = Column(String)
    VotingUrl = Column(String)
    SittingTitle = Column(String)
    VotingTopic = Column(String)


class PartyVotesLinks(Base):  # type: ignore
    __tablename__ = "PartyVotesLinks"

    Url = Column(String)
    Party = Column(String)
    VotingInternalId = Column(Integer, primary_key=True)
    TermNumber = Column(Integer)
    SittingNumber = Column(Integer)
    VotingNumber = Column(Integer)


class Votes(Base):  # type: ignore
    __tablename__ = "Votes"

    VotingInternalId = Column(Integer, primary_key=True)
    Url = Column(String)
    Person = Column(String, primary_key=True)
    Party = Column(String, primary_key=True)
    Vote = Column(String)
