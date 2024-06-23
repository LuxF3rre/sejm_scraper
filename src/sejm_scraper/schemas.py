from datetime import datetime
from enum import Enum
from typing import Literal, NewType, Union

from pydantic import BaseModel, Field


class TermSchema(BaseModel):
    number: int = Field(validation_alias="num")


class SittingSchema(BaseModel):
    title: str
    number: int


OptionIndex = NewType("OptionIndex", int)


class VotingOptionSchema(BaseModel):
    option_index: OptionIndex = Field(validation_alias="optionIndex")
    description: Union[str, None] = Field(default=None)


class VotingSchema(BaseModel):
    term: int
    sitting: int
    sitting_day: int = Field(validation_alias="sittingDay")
    voting_number: int = Field(validation_alias="votingNumber")
    date: datetime

    title: str
    description: Union[str, None] = Field(default=None)
    topic: Union[str, None] = Field(default=None)

    voting_options: Union[list[VotingOptionSchema], None] = Field(default=None, validation_alias="votingOptions")


class Vote(str, Enum):
    YES = "YES"
    NO = "NO"
    ABSTAIN = "ABSTAIN"
    ABSENT = "ABSENT"


VOTE_VALID = Literal["VOTE_VALID"]  # vote value when multiple options are present


class MpVoteSchema(BaseModel):
    mp_term_id: int = Field(validation_alias="MP")
    votes: Union[dict[OptionIndex, Vote], None] = Field(default=None, validation_alias="listVotes")
    vote: Union[Vote, VOTE_VALID]


class VotingWithMpVotesSchema(VotingSchema):
    mp_votes: list[MpVoteSchema] = Field(validation_alias="votes")


class MpSchema(BaseModel):
    in_term_id: int = Field(validation_alias="id")
    first_name: str = Field(validation_alias="firstName")
    second_name: Union[str, None] = Field(default=None, validation_alias="secondName")
    last_name: str = Field(validation_alias="lastName")
    birth_date: str = Field(validation_alias="birthDate")
    birth_place: Union[str, None] = Field(default=None, validation_alias="birthLocation")
    party: Union[str, None] = Field(default=None, validation_alias="club")
    education: Union[str, None] = Field(default=None, validation_alias="educationLevel")
    profession: Union[str, None] = Field(default=None)
    voivodeship: Union[str, None] = Field(default=None)
    district_name: str = Field(validation_alias="districtName")
    # Usualy both below are present, but sometimes only waiver_description is present
    inactivity_cause: Union[str, None] = Field(default=None, validation_alias="inactiveCause")
    inactivity_description: Union[str, None] = Field(default=None, validation_alias="waiverDesc")