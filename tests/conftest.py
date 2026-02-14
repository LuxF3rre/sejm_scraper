from datetime import date
from typing import TYPE_CHECKING

import pytest
from sqlmodel import create_engine

from sejm_scraper import database

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Engine


@pytest.fixture
def engine() -> "Engine":
    eng = create_engine("duckdb:///:memory:", echo=False)
    database.create_db_and_tables(engine=eng)
    return eng


@pytest.fixture
def term() -> database.Term:
    return database.Term(
        id="4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
        number=10,
        from_date=date(2023, 11, 13),
        to_date=None,
    )


@pytest.fixture
def sitting() -> database.Sitting:
    return database.Sitting(
        id="00037f39cf870a1f49129f9c82d935665d352ffd25ea3296208f6f7b16fd654f",
        term_id="4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
        title=(
            "39. Posiedzenie Sejmu RP w dniach"
            " 22, 23, 24 i 25 lipca oraz 4 i 5 sierpnia 2025 r."
        ),
        number=39,
    )


@pytest.fixture
def voting() -> database.Voting:
    return database.Voting(
        id="951864298144ec8f4743a55070d31d28601962f4b17c84dcf4da9bea431161ad",
        sitting_id="00037f39cf870a1f49129f9c82d935665d352ffd25ea3296208f6f7b16fd654f",
        sitting_day=6,
        number=205,
        date=date(2025, 8, 5),
        title=(
            "Pkt. 56 Sprawozdanie z działalności"
            " Najwyższej Izby Kontroli w 2024 roku"
            " wraz z opinią Komisji (druki nr 1405 i 1417)"
        ),
        description=(
            "wniosek o przyjęcie sprawozdania"
            " z działalności Najwyższej Izby Kontroli w 2024 roku"
        ),
        topic="głosowanie nad przyjęciem sprawozdania",
        kind="ELECTRONIC",
        yes=254,
        no=180,
        abstain=1,
        not_participating=25,
        present=0,
        total_voted=435,
        majority_type="SIMPLE_MAJORITY",
        majority_votes=181,
        against_all=None,
    )


MOCK_BASE_URL = "https://sejm-mock.pl/api/v1"

TERM_RESPONSE = [
    {
        "current": "true",
        "from": "2023-11-13",
        "num": 10,
    },
]

MP_RESPONSE = [
    {
        "accusativeName": "Andrzeja Adamczyka",
        "active": True,
        "birthDate": "1959-01-04",
        "birthLocation": "Krzeszowice",
        "club": "PiS",
        "districtName": "Kraków",
        "districtNum": 13,
        "educationLevel": "wyższe",
        "email": "Andrzej.Adamczyk@sejm.pl",
        "firstLastName": "Andrzej Adamczyk",
        "firstName": "Andrzej",
        "genitiveName": "Andrzeja Adamczyka",
        "id": 1,
        "lastFirstName": "Adamczyk Andrzej",
        "lastName": "Adamczyk",
        "numberOfVotes": 45171,
        "profession": "ekonomista",
        "secondName": "Mieczysław",
        "voivodeship": "małopolskie",
    },
]

CLUB_RESPONSE = [
    {
        "email": "kp-pis@kluby.sejm.pl",
        "fax": "(22) 694-26-11",
        "id": "PiS",
        "membersCount": 189,
        "name": "Klub Parlamentarny Prawo i Sprawiedliwość",
        "phone": "",
    },
]

SITTING_RESPONSE = [
    {
        "current": "false",
        "dates": ["2025-07-22", "2025-07-23"],
        "number": 39,
        "title": (
            "39. Posiedzenie Sejmu RP w dniach"
            " 22, 23, 24 i 25 lipca"
            " oraz 4 i 5 sierpnia 2025 r."
        ),
    },
]

VOTING_LIST_RESPONSE = [
    {
        "abstain": 1,
        "date": "2025-08-05T16:32:45",
        "description": (
            "wniosek o przyjęcie sprawozdania"
            " z działalności Najwyższej Izby Kontroli"
            " w 2024 roku"
        ),
        "kind": "ELECTRONIC",
        "majorityType": "SIMPLE_MAJORITY",
        "majorityVotes": 181,
        "no": 180,
        "notParticipating": 25,
        "present": 0,
        "sitting": 39,
        "sittingDay": 6,
        "term": 10,
        "title": (
            "Pkt. 56 Sprawozdanie z działalności"
            " Najwyższej Izby Kontroli w 2024 roku"
            " wraz z opinią Komisji (druki nr 1405 i 1417)"
        ),
        "topic": "głosowanie nad przyjęciem sprawozdania",
        "totalVoted": 435,
        "votingNumber": 205,
        "yes": 254,
    }
]

VOTE_DETAIL_RESPONSE = {
    **VOTING_LIST_RESPONSE[0],
    "votes": [
        {
            "MP": 1,
            "club": "PiS",
            "firstName": "Andrzej",
            "lastName": "Adamczyk",
            "secondName": "Mieczysław",
            "vote": "NO",
        },
    ],
}

VOTE_DETAIL_MULTI_OPTION_RESPONSE = {
    **VOTING_LIST_RESPONSE[0],
    "kind": "ON_LIST",
    "votingOptions": [
        {
            "optionIndex": 1,
            "option": "Kandydat A",
            "description": "Opis A",
            "votes": 200,
        },
        {
            "optionIndex": 2,
            "option": "Kandydat B",
            "description": "Opis B",
            "votes": 235,
        },
    ],
    "votes": [
        {
            "MP": 1,
            "club": "PiS",
            "firstName": "Andrzej",
            "lastName": "Adamczyk",
            "secondName": "Mieczysław",
            "vote": "VOTE_VALID",
            "listVotes": {"1": "YES", "2": "NO"},
        },
    ],
}

VOTING_TABLE_RESPONSE = [
    {"date": "2025-01-15", "proceeding": 5, "votingsNum": 10},
    {"date": "2025-01-16", "proceeding": 5, "votingsNum": 5},
    {"date": "2025-02-01", "proceeding": 6, "votingsNum": 8},
]
