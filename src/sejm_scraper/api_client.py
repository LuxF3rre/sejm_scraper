import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from sejm_scraper import schemas

retry_settings = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=4, max=10),
    "reraise": True,
}


@retry(**retry_settings)
async def get_terms(client: httpx.AsyncClient) -> list[schemas.TermSchema]:
    response = await client.get("https://api.sejm.gov.pl/sejm/term", timeout=30)
    response.raise_for_status()
    result = response.json()
    return [schemas.TermSchema(**item) for item in result]


@retry(**retry_settings)
async def get_sittings(
    client: httpx.AsyncClient, term: int
) -> list[schemas.SittingSchema]:
    response = await client.get(
        f"https://api.sejm.gov.pl/sejm/term{term}/proceedings", timeout=30
    )
    response.raise_for_status()
    result = response.json()
    return [schemas.SittingSchema(**item) for item in result]


@retry(**retry_settings)
async def get_votings(
    client: httpx.AsyncClient, term: int, sitting: int
) -> list[schemas.VotingSchema]:
    response = await client.get(
        f"https://api.sejm.gov.pl/sejm/term{term}/votings/{sitting}",
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    return [schemas.VotingSchema(**item) for item in result]


@retry(**retry_settings)
async def get_votes(
    client: httpx.AsyncClient, term: int, sitting: int, voting: int
) -> schemas.VotingWithMpVotesSchema:
    response = await client.get(
        f"https://api.sejm.gov.pl/sejm/term{term}/votings/{sitting}/{voting}",
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    return schemas.VotingWithMpVotesSchema(**result)


@retry(**retry_settings)
async def get_mps(client: httpx.AsyncClient, term: int) -> list[schemas.MpSchema]:
    response = await client.get(
        f"https://api.sejm.gov.pl/sejm/term{term}/MP", timeout=30
    )
    response.raise_for_status()
    result = response.json()
    return [schemas.MpSchema(**item) for item in result]
