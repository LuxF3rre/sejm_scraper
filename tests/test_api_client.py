import httpx
import pytest
import respx

from sejm_scraper import api_client, api_schemas

from .conftest import (
    CLUB_RESPONSE,
    MOCK_BASE_URL,
    MP_RESPONSE,
    SITTING_RESPONSE,
    TERM_RESPONSE,
    VOTE_DETAIL_RESPONSE,
    VOTING_LIST_RESPONSE,
    VOTING_TABLE_RESPONSE,
)


@pytest.fixture(autouse=True)
def _patch_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_client, "BASE_URL", MOCK_BASE_URL)


@pytest.mark.anyio
@respx.mock
async def test_fetch_terms() -> None:
    respx.get(f"{MOCK_BASE_URL}/term").mock(
        return_value=httpx.Response(200, json=TERM_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        terms = await api_client.fetch_terms(client=client)

    assert len(terms) == 1
    assert terms[0].number == 10


@pytest.mark.anyio
@respx.mock
async def test_fetch_sittings() -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/proceedings").mock(
        return_value=httpx.Response(200, json=SITTING_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        sittings = await api_client.fetch_sittings(client=client, term=10)

    assert len(sittings) == 1
    assert sittings[0].number == 39


@pytest.mark.anyio
@respx.mock
async def test_fetch_votings() -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39").mock(
        return_value=httpx.Response(200, json=VOTING_LIST_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        votings = await api_client.fetch_votings(
            client=client, term=10, sitting=39
        )

    assert len(votings) == 1
    assert votings[0].number == 205


@pytest.mark.anyio
@respx.mock
async def test_fetch_votes() -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39/205").mock(
        return_value=httpx.Response(200, json=VOTE_DETAIL_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await api_client.fetch_votes(
            client=client, term=10, sitting=39, voting=205
        )

    assert len(result.mp_votes) == 1
    assert result.mp_votes[0].vote == api_schemas.Vote.NO


@pytest.mark.anyio
@respx.mock
async def test_fetch_voting_table() -> None:
    respx.get(f"{MOCK_BASE_URL}/term3/votings").mock(
        return_value=httpx.Response(200, json=VOTING_TABLE_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        entries = await api_client.fetch_voting_table(client=client, term=3)

    assert len(entries) == 3
    assert entries[0].proceeding == 5


@pytest.mark.anyio
@respx.mock
async def test_fetch_clubs() -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/clubs").mock(
        return_value=httpx.Response(200, json=CLUB_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        clubs = await api_client.fetch_clubs(client=client, term=10)

    assert len(clubs) == 1
    assert clubs[0].club_id == "PiS"


@pytest.mark.anyio
@respx.mock
async def test_fetch_mps() -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/MP").mock(
        return_value=httpx.Response(200, json=MP_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        mps = await api_client.fetch_mps(client=client, term=10)

    assert len(mps) == 1
    assert mps[0].first_name == "Andrzej"
    assert mps[0].last_name == "Adamczyk"
