from typing import TypeVar

import httpx
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from sejm_scraper import api_schemas

RETRY_SETTINGS = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=4, max=10),
    "reraise": True,
}
BASE_URL = "https://api.sejm.gov.pl/sejm"
TIMEOUT = 30

T = TypeVar("T", bound=BaseModel)


@retry(**RETRY_SETTINGS)
def fetch_votes(
    client: httpx.Client,
    term: int,
    sitting: int,
    voting: int,
) -> api_schemas.VotingWithMpVotesSchema:
    response = client.get(
        f"{BASE_URL}/term{term}/votings/{sitting}/{voting}",
        timeout=TIMEOUT,
    ).raise_for_status()
    return api_schemas.VotingWithMpVotesSchema(**response.json())


@retry(**RETRY_SETTINGS)
def _fetch_list[T: BaseModel](
    client: httpx.Client,
    path: str,
    model: type[T],
) -> list[T]:
    response = client.get(
        f"{BASE_URL}/{path}",
        timeout=TIMEOUT,
    ).raise_for_status()
    return [model(**item) for item in response.json()]


def fetch_terms(
    client: httpx.Client,
) -> list[api_schemas.TermSchema]:
    return _fetch_list(
        client=client,
        path="term",
        model=api_schemas.TermSchema,
    )


def fetch_sittings(
    client: httpx.Client,
    term: int,
) -> list[api_schemas.SittingSchema]:
    return _fetch_list(
        client=client,
        path=f"term{term}/proceedings",
        model=api_schemas.SittingSchema,
    )


def fetch_votings(
    client: httpx.Client,
    term: int,
    sitting: int,
) -> list[api_schemas.VotingSchema]:
    return _fetch_list(
        client=client,
        path=f"term{term}/votings/{sitting}",
        model=api_schemas.VotingSchema,
    )


@retry(**RETRY_SETTINGS)
def fetch_mps_in_term(
    client: httpx.Client,
    term: int,
) -> list[api_schemas.MpInTermSchema]:
    return _fetch_list(
        client=client,
        path=f"term{term}/MP",
        model=api_schemas.MpInTermSchema,
    )
