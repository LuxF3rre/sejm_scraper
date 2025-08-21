from datetime import date, datetime
from enum import StrEnum
from typing import Literal, NewType

from pydantic import BaseModel, Field


class MajorityType(StrEnum):
    SIMPLE_MAJORITY = "SIMPLE_MAJORITY"
    ABSOLUTE_MAJORITY = "ABSOLUTE_MAJORITY"
    STATUTORY_MAJORITY = "STATUTORY_MAJORITY"


class TermSchema(BaseModel):
    number: int = Field(validation_alias="num")
    from_date: date = Field(validation_alias="from")
    to_date: date | None = Field(default=None, validation_alias="to")


class SittingSchema(BaseModel):
    title: str = Field()
    number: int = Field()


OptionIndex = NewType("OptionIndex", int)


class VotingOptionSchema(BaseModel):
    index: OptionIndex = Field(validation_alias="optionIndex")
    description: str | None = Field(default=None)


class VotingSchema(BaseModel):
    term: int = Field()
    sitting: int = Field()
    number: int = Field(validation_alias="votingNumber")
    day_number: int = Field(validation_alias="sittingDay")
    date: datetime = Field()
    title: str = Field()
    description: str | None = Field(default=None)
    topic: str | None = Field(default=None)
    voting_options: list[VotingOptionSchema] | None = Field(
        default=None, validation_alias="votingOptions"
    )
    majority_type: MajorityType = Field(validation_alias="majorityType")
    majority_votes: int = Field(validation_alias="majorityVotes")


class Vote(StrEnum):
    YES = "YES"
    NO = "NO"
    ABSTAIN = "ABSTAIN"
    ABSENT = "ABSENT"
    PRESENT = "PRESENT"


# vote value when multiple options are present
VOTE_VALID = Literal["VOTE_VALID"]

MpInTermId = NewType("MpInTermId", int)


class MpVoteSchema(BaseModel):
    mp_term_id: MpInTermId = Field(validation_alias="MP")
    party_in_term: str | None = Field(default=None, validation_alias="club")
    multiple_option_votes: dict[OptionIndex, Vote] | None = Field(
        default=None, validation_alias="listVotes"
    )
    vote: Vote | VOTE_VALID


class VotingWithMpVotesSchema(VotingSchema):
    mp_votes: list[MpVoteSchema] = Field(validation_alias="votes")


class MpInTermSchema(BaseModel):
    in_term_id: int = Field(validation_alias="id")
    first_name: str = Field(validation_alias="firstName")
    second_name: str | None = Field(default=None, validation_alias="secondName")
    last_name: str = Field(validation_alias="lastName")
    birth_date: date = Field(validation_alias="birthDate")
    birth_place: str | None = Field(
        default=None, validation_alias="birthLocation"
    )
    education: str | None = Field(
        default=None, validation_alias="educationLevel"
    )
    profession: str | None = Field(default=None)
    voivodeship: str | None = Field(default=None)
    district_name: str = Field(validation_alias="districtName")
    # Usually both fields are present, but sometimes
    # only inactivity_description field is present
    inactivity_cause: str | None = Field(
        default=None, validation_alias="inactiveCause"
    )
    inactivity_description: str | None = Field(
        default=None, validation_alias="waiverDesc"
    )


PartyAbbreviation = NewType("PartyAbbreviation", str)


class PartyInSchema(BaseModel):
    id: PartyAbbreviation
    name: str
    phone: str | None = Field(default=None)
    fax: str | None = Field(default=None)
    email: str | None = Field(default=None)
    member_count: int = Field(validation_alias="membersCount")
