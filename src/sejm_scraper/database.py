import os

from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

database_name = os.environ["POSTGRES_DB"]
user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
port = os.environ["POSTGRES_PORT"]
pool_size = int(os.environ["SCRAPINGBEE_CONCURRENT"])

engine = create_engine(
    f"postgresql://{user}:{password}@{host}:{port}/{database_name}",
    pool_size=pool_size,
)

Base = declarative_base()


class Votings(Base):
    __tablename__ = "Votings"

    SittingDayId = Column(Integer)
    VotingTimestamp = Column(DateTime)
    TermNumber = Column(Integer)
    SittingNumber = Column(Integer)
    VotingNumber = Column(Integer)
    SittingUrl = Column(String)
    VotingUrl = Column(String, primary_key=True)
    SittingTitle = Column(String)
    VotingTopic = Column(String)


class PartyVotesLinks(Base):
    __tablename__ = "PartyVotesLinks"

    Url = Column(String)
    Party = Column(String)
    VotingInternalId = Column(Integer, primary_key=True)
    TermNumber = Column(Integer)
    SittingNumber = Column(Integer)
    VotingNumber = Column(Integer)


class Votes(Base):
    __tablename__ = "Votes"

    VotingInternalId = Column(Integer, primary_key=True)
    Url = Column(String)
    Person = Column(String, primary_key=True)
    Party = Column(String)
    Vote = Column(String)


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
