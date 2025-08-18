import httpx
import pytest
import respx
from sqlmodel import create_engine

from sejm_scraper import api_client, database


@pytest.fixture(autouse=True)
def use_in_memory_database(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        database,
        "ENGINE",
        create_engine("duckdb:///:memory:", echo=False),
    )


@pytest.fixture(autouse=True)
@respx.mock(assert_all_called=False)
def sejm_api_mock(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_base_url = "https://sejm-mock.pl/api/v1"
    monkeypatch.setattr(api_client, "BASE_URL", mock_base_url)
    respx.get(f"{mock_base_url}/term").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "current": "true",
                    "from": "2023-11-13",
                    "num": 10,
                    "prints": {
                        "count": 1762,
                        "lastChanged": "2025-08-15T04:00:37",
                        "link": "/term10/prints",
                    },
                },
            ],
        )
    )
    respx.get(f"{mock_base_url}/term10/MP").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "accusativeName": "Andrzeja Adamczyka",
                    "active": "true",
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
            ],
        )
    )
    respx.get(f"{mock_base_url}/term10/clubs").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "email": "kp-pis@kluby.sejm.pl",
                    "fax": "(22) 694-26-11",
                    "id": "PiS",
                    "membersCount": 189,
                    "name": "Klub Parlamentarny Prawo i Sprawiedliwość",
                    "phone": "",
                },
            ],
        )
    )
    respx.get(f"{mock_base_url}/term10/proceedings").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "current": "false",
                    "dates": [
                        "2025-07-22",
                        "2025-07-23",
                        "2025-07-24",
                        "2025-07-25",
                        "2025-08-04",
                        "2025-08-05",
                    ],
                    "number": 39,
                    "title": (
                        "39. Posiedzenie Sejmu RP w dniach"
                        " 22, 23, 24 i 25 lipca oraz 4 i 5 sierpnia 2025 r."
                    ),
                },
            ],
        )
    )
    respx.get(f"{mock_base_url}/term10/votings/39").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "abstain": 1,
                    "date": "2025-08-05T16:32:45",
                    "description": (
                        "wniosek o przyjęcie sprawozdania"
                        " z działalności Najwyższej Izby Kontroli"
                        " w 2024 roku"
                    ),
                    "kind": "ELECTRONIC",
                    "links": [
                        {
                            "href": "https://api.sejm.gov.pl/sejm/term10/votings/39/205/pdf",
                            "rel": "pdf",
                        }
                    ],
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
            ],
        )
    )
    respx.get(f"{mock_base_url}/term10/votings/39/205").mock(
        return_value=httpx.Response(
            200,
            json={
                "abstain": 1,
                "date": "2025-08-05T16:32:45",
                "description": (
                    "wniosek o przyjęcie sprawozdania"
                    " z działalności Najwyższej Izby Kontroli"
                    " w 2024 roku"
                ),
                "kind": "ELECTRONIC",
                "links": [
                    {
                        "href": "https://api.sejm.gov.pl/sejm/term10/votings/39/205/pdf",
                        "rel": "pdf",
                    }
                ],
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
                "votingNumber": 205,
                "yes": 254,
            },
        )
    )
