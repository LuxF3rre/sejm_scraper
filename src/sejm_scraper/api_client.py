import httpx
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential

from sejm_scraper import api_schemas

RETRY_SETTINGS = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=4, max=30),
    "reraise": True,
}
BASE_URL = "https://api.sejm.gov.pl/sejm"
TIMEOUT = 30


@retry(**RETRY_SETTINGS)  # ty: ignore[no-matching-overload]
async def fetch_votes(
    *,
    client: httpx.AsyncClient,
    term: int,
    sitting: int,
    voting: int,
) -> api_schemas.VotingWithMpVotesSchema:
    """Fetch detailed voting results including individual MP votes.

    Args:
        client: HTTP client instance.
        term: Sejm term number.
        sitting: Sitting number within the term.
        voting: Voting number within the sitting.

    Returns:
        Voting data with individual MP vote records.
    """
    response = await client.get(
        f"{BASE_URL}/term{term}/votings/{sitting}/{voting}",
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return api_schemas.VotingWithMpVotesSchema(**response.json())


@retry(**RETRY_SETTINGS)  # ty: ignore[no-matching-overload]
async def _fetch_list[T: BaseModel](
    *,
    client: httpx.AsyncClient,
    path: str,
    model: type[T],
) -> list[T]:
    response = await client.get(
        f"{BASE_URL}/{path}",
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    return [model(**item) for item in response.json()]


async def fetch_terms(
    *,
    client: httpx.AsyncClient,
) -> list[api_schemas.TermSchema]:
    """Fetch all Sejm terms.

    Args:
        client: HTTP client instance.

    Returns:
        List of term schemas.
    """
    return await _fetch_list(
        client=client,
        path="term",
        model=api_schemas.TermSchema,
    )


async def fetch_sittings(
    *,
    client: httpx.AsyncClient,
    term: int,
) -> list[api_schemas.SittingSchema]:
    """Fetch all sittings for a given term.

    Args:
        client: HTTP client instance.
        term: Sejm term number.

    Returns:
        List of sitting schemas.
    """
    return await _fetch_list(
        client=client,
        path=f"term{term}/proceedings",
        model=api_schemas.SittingSchema,
    )


async def fetch_votings(
    *,
    client: httpx.AsyncClient,
    term: int,
    sitting: int,
) -> list[api_schemas.VotingSchema]:
    """Fetch all votings for a given term and sitting.

    Args:
        client: HTTP client instance.
        term: Sejm term number.
        sitting: Sitting number within the term.

    Returns:
        List of voting schemas.
    """
    return await _fetch_list(
        client=client,
        path=f"term{term}/votings/{sitting}",
        model=api_schemas.VotingSchema,
    )


async def fetch_voting_table(
    *,
    client: httpx.AsyncClient,
    term: int,
) -> list[api_schemas.VotingTableEntrySchema]:
    """Fetch the voting table for a term (flat list of sitting-day entries).

    This endpoint exists for terms that lack proceedings data (terms 3-6).
    Each entry maps a date to a proceeding (sitting) number.

    Args:
        client: HTTP client instance.
        term: Sejm term number.

    Returns:
        List of voting table entry schemas.
    """
    return await _fetch_list(
        client=client,
        path=f"term{term}/votings",
        model=api_schemas.VotingTableEntrySchema,
    )


async def fetch_clubs(
    *,
    client: httpx.AsyncClient,
    term: int,
) -> list[api_schemas.ClubSchema]:
    """Fetch all clubs for a given term.

    Args:
        client: HTTP client instance.
        term: Sejm term number.

    Returns:
        List of club schemas.
    """
    return await _fetch_list(
        client=client,
        path=f"term{term}/clubs",
        model=api_schemas.ClubSchema,
    )


async def fetch_mps(
    *,
    client: httpx.AsyncClient,
    term: int,
) -> list[api_schemas.MpSchema]:
    """Fetch all MPs for a given term.

    Args:
        client: HTTP client instance.
        term: Sejm term number.

    Returns:
        List of MP schemas.
    """
    return await _fetch_list(
        client=client,
        path=f"term{term}/MP",
        model=api_schemas.MpSchema,
    )
