from datetime import date, datetime
from enum import StrEnum
from typing import Literal, NewType, Union

from pydantic import BaseModel, Field


class TermSchema(BaseModel):
    number: int = Field(validation_alias="num")
    from_date: date = Field(validation_alias="from")
    to_date: Union[date, None] = Field(default=None, validation_alias="to")


class SittingSchema(BaseModel):
    title: str = Field()
    number: int = Field()
    dates: list[date] = Field()


OptionIndex = NewType("OptionIndex", int)


class VotingOptionSchema(BaseModel):
    index: OptionIndex = Field(validation_alias="optionIndex")
    option_label: Union[str, None] = Field(
        default=None, validation_alias="option"
    )
    description: Union[str, None] = Field(default=None)
    votes: int = Field()


class VotingSchema(BaseModel):
    term: int = Field()
    sitting: int = Field()
    sitting_day: int = Field(validation_alias="sittingDay")
    number: int = Field(validation_alias="votingNumber")
    date: datetime = Field()
    title: str = Field()
    description: Union[str, None] = Field(default=None)
    topic: Union[str, None] = Field(default=None)
    kind: str = Field()
    yes: int = Field()
    no: int = Field()
    abstain: int = Field()
    not_participating: int = Field(validation_alias="notParticipating")
    present: int = Field()
    total_voted: int = Field(validation_alias="totalVoted")
    majority_type: str = Field(validation_alias="majorityType")
    majority_votes: int = Field(validation_alias="majorityVotes")
    against_all: Union[int, None] = Field(
        default=None, validation_alias="againstAll"
    )
    voting_options: Union[list[VotingOptionSchema], None] = Field(
        default=None, validation_alias="votingOptions"
    )


class Vote(StrEnum):
    YES = "YES"
    NO = "NO"
    ABSTAIN = "ABSTAIN"
    ABSENT = "ABSENT"
    PRESENT = "PRESENT"
    NO_VOTE = "NO_VOTE"
    VOTE_INVALID = "VOTE_INVALID"


# vote value when multiple options are present
VOTE_VALID = Literal["VOTE_VALID"]

MpTermId = NewType("MpTermId", int)


class MpVoteSchema(BaseModel):
    mp_term_id: MpTermId = Field(validation_alias="MP")
    party: Union[str, None] = Field(default=None, validation_alias="club")
    multiple_option_votes: Union[dict[OptionIndex, Vote], None] = Field(
        default=None, validation_alias="listVotes"
    )
    vote: Union[Vote, VOTE_VALID]


class VotingWithMpVotesSchema(VotingSchema):
    mp_votes: list[MpVoteSchema] = Field(validation_alias="votes")


class VotingTableEntrySchema(BaseModel):
    sitting_date: date = Field(validation_alias="date")
    proceeding: int = Field()
    votings_num: int = Field(validation_alias="votingsNum")


class ClubSchema(BaseModel):
    club_id: str = Field(validation_alias="id")
    name: str = Field()
    phone: Union[str, None] = Field(default=None)
    fax: Union[str, None] = Field(default=None)
    email: Union[str, None] = Field(default=None)
    members_count: int = Field(validation_alias="membersCount")


class MpSchema(BaseModel):
    in_term_id: int = Field(validation_alias="id")
    first_name: str = Field(validation_alias="firstName")
    second_name: Union[str, None] = Field(
        default=None, validation_alias="secondName"
    )
    last_name: str = Field(validation_alias="lastName")
    birth_date: date = Field(validation_alias="birthDate")
    birth_place: Union[str, None] = Field(
        default=None, validation_alias="birthLocation"
    )
    active: bool = Field()
    club: Union[str, None] = Field(default=None)
    district_num: int = Field(validation_alias="districtNum")
    number_of_votes: int = Field(validation_alias="numberOfVotes")
    email: Union[str, None] = Field(default=None)
    education: Union[str, None] = Field(
        default=None, validation_alias="educationLevel"
    )
    profession: Union[str, None] = Field(default=None)
    voivodeship: Union[str, None] = Field(default=None)
    district_name: str = Field(validation_alias="districtName")
    # Usually both fields are present, but sometimes
    # only inactivity_description field is present
    inactivity_cause: Union[str, None] = Field(
        default=None, validation_alias="inactiveCause"
    )
    inactivity_description: Union[str, None] = Field(
        default=None, validation_alias="waiverDesc"
    )
