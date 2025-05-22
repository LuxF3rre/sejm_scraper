import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from sejm_scraper import schemas

retry_settings = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=4, max=10),
    "reraise": True,
    # "retry": retry_if_exception_type(IOError)
}


@retry(**retry_settings)
def get_terms(client: httpx.Client) -> list[schemas.TermSchema]:
    result = (
        client.get("https://api.sejm.gov.pl/sejm/term", timeout=30)
        .raise_for_status()
        .json()
    )
    return [schemas.TermSchema(**item) for item in result]


@retry(**retry_settings)
def get_sittings(
    client: httpx.Client, term: int
) -> list[schemas.SittingSchema]:
    result = (
        client.get(
            f"https://api.sejm.gov.pl/sejm/term{term}/proceedings", timeout=30
        )
        .raise_for_status()
        .json()
    )
    return [schemas.SittingSchema(**item) for item in result]


@retry(**retry_settings)
def get_votings(
    client: httpx.Client, term: int, sitting: int
) -> list[schemas.VotingSchema]:
    result = (
        client.get(
            f"https://api.sejm.gov.pl/sejm/term{term}/votings/{sitting}",
            timeout=30,
        )
        .raise_for_status()
        .json()
    )
    return [schemas.VotingSchema(**item) for item in result]


@retry(**retry_settings)
def get_votes(
    client: httpx.Client, term: int, sitting: int, voting: int
) -> schemas.VotingWithMpVotesSchema:
    result = (
        client.get(
            f"https://api.sejm.gov.pl/sejm/term{term}/votings/{sitting}/{voting}",
            timeout=30,
        )
        .raise_for_status()
        .json()
    )
    return schemas.VotingWithMpVotesSchema(**result)


@retry(**retry_settings)
def get_mps(client: httpx.Client, term: int) -> list[schemas.MpSchema]:
    result = (
        client.get(f"https://api.sejm.gov.pl/sejm/term{term}/MP", timeout=30)
        .raise_for_status()
        .json()
    )
    return [schemas.MpSchema(**item) for item in result]
