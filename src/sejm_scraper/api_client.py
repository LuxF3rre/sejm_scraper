from typing import TypeVar

import httpx
from loguru import logger
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
    logger.debug(
        f"Fetching votes for term {term}, sitting {sitting}, voting {voting}"
    )
    response = client.get(
        f"{BASE_URL}/term{term}/votings/{sitting}/{voting}",
        timeout=TIMEOUT,
    ).raise_for_status()
    logger.debug(
        f"Successfully fetched votes for term {term},"
        f" sitting {sitting}, voting {voting}"
    )
    return api_schemas.VotingWithMpVotesSchema(**response.json())


@retry(**RETRY_SETTINGS)
def _fetch_list[T: BaseModel](
    client: httpx.Client,
    path: str,
    model: type[T],
) -> list[T]:
    logger.debug(f"Fetching list from path: {path}")
    response = client.get(
        f"{BASE_URL}/{path}",
        timeout=TIMEOUT,
    ).raise_for_status()
    result = [model(**item) for item in response.json()]
    logger.debug(f"Fetched {len(result)} items from path: {path}")
    return result


def fetch_terms(
    client: httpx.Client,
) -> list[api_schemas.TermSchema]:
    logger.debug("Fetching terms")
    return _fetch_list(
        client=client,
        path="term",
        model=api_schemas.TermSchema,
    )


def fetch_sittings(
    client: httpx.Client,
    term: int,
) -> list[api_schemas.SittingSchema]:
    logger.debug(f"Fetching sittings for term {term}")
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
    logger.debug(f"Fetching votings for term {term}, sitting {sitting}")
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
    logger.debug(f"Fetching MPs for term {term}")
    return _fetch_list(
        client=client,
        path=f"term{term}/MP",
        model=api_schemas.MpInTermSchema,
    )


@retry(**RETRY_SETTINGS)
def fetch_parties(
    client: httpx.Client,
    term: int,
) -> list[api_schemas.PartySchema]:
    logger.debug(f"Fetching parties for term {term}")
    return _fetch_list(
        client=client,
        path=f"term{term}/clubs",
        model=api_schemas.PartySchema,
    )
