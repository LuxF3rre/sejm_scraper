import httpx
import pytest
import respx

from sejm_scraper import api_client, api_schemas, database, scrape

from .conftest import (
    CLUB_RESPONSE,
    MOCK_BASE_URL,
    MP_RESPONSE,
    SITTING_RESPONSE,
    TERM_RESPONSE,
    VOTE_DETAIL_MULTI_OPTION_RESPONSE,
    VOTE_DETAIL_RESPONSE,
    VOTING_LIST_RESPONSE,
    VOTING_TABLE_RESPONSE,
)


@pytest.fixture(autouse=True)
def _patch_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_client, "BASE_URL", MOCK_BASE_URL)


@pytest.mark.anyio
@respx.mock
async def test_scrape_terms() -> None:
    respx.get(f"{MOCK_BASE_URL}/term").mock(
        return_value=httpx.Response(200, json=TERM_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        terms = await scrape.scrape_terms(client=client)

    assert len(terms) == 1
    assert terms[0].number == 10
    assert terms[0].id != ""


@pytest.mark.anyio
@respx.mock
async def test_scrape_terms_with_from_term_filter() -> None:
    respx.get(f"{MOCK_BASE_URL}/term").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"num": 9, "from": "2019-11-12"},
                {"num": 10, "from": "2023-11-13"},
            ],
        )
    )
    async with httpx.AsyncClient() as client:
        terms = await scrape.scrape_terms(client=client, from_term=10)

    assert len(terms) == 1
    assert terms[0].number == 10


@pytest.mark.anyio
@respx.mock
async def test_scrape_sittings(
    term: database.Term,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/proceedings").mock(
        return_value=httpx.Response(200, json=SITTING_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_sittings(client=client, term=term)

    assert len(result.sittings) == 1
    assert result.sittings[0].number == 39
    assert len(result.sitting_days) == 2


@pytest.mark.anyio
@respx.mock
async def test_scrape_sittings_filters_planned(
    term: database.Term,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/proceedings").mock(
        return_value=httpx.Response(
            200,
            json=[
                {**SITTING_RESPONSE[0], "number": 0},
                SITTING_RESPONSE[0],
            ],
        )
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_sittings(client=client, term=term)

    assert len(result.sittings) == 1
    assert result.sittings[0].number == 39


@pytest.mark.anyio
@respx.mock
async def test_scrape_sittings_with_from_sitting(
    term: database.Term,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/proceedings").mock(
        return_value=httpx.Response(
            200,
            json=[
                {**SITTING_RESPONSE[0], "number": 38},
                SITTING_RESPONSE[0],
            ],
        )
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_sittings(
            client=client, term=term, from_sitting=39
        )

    assert len(result.sittings) == 1
    assert result.sittings[0].number == 39


@pytest.mark.anyio
@respx.mock
async def test_discover_sittings_from_votings(
    term: database.Term,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings").mock(
        return_value=httpx.Response(200, json=VOTING_TABLE_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.discover_sittings_from_votings(
            client=client, term=term
        )

    assert len(result.sittings) == 2
    assert result.sittings[0].number == 5
    assert result.sittings[1].number == 6
    # Sitting 5 has 2 unique dates, sitting 6 has 1
    assert len(result.sitting_days) == 3


@pytest.mark.anyio
@respx.mock
async def test_discover_sittings_from_votings_with_from_sitting(
    term: database.Term,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings").mock(
        return_value=httpx.Response(200, json=VOTING_TABLE_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.discover_sittings_from_votings(
            client=client, term=term, from_sitting=6
        )

    assert len(result.sittings) == 1
    assert result.sittings[0].number == 6


@pytest.mark.anyio
@respx.mock
async def test_scrape_votings_without_options(
    term: database.Term,
    sitting: database.Sitting,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39").mock(
        return_value=httpx.Response(200, json=VOTING_LIST_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_votings(
            client=client, term=term, sitting=sitting
        )

    assert len(result.votings) == 1
    # No voting_options in response → default option created
    assert len(result.voting_options) == 1
    assert (
        result.voting_options[0].option_label
        == "Default option (no options provided)"
    )


@pytest.mark.anyio
@respx.mock
async def test_scrape_votings_with_options(
    term: database.Term,
    sitting: database.Sitting,
) -> None:
    voting_with_options = {
        **VOTING_LIST_RESPONSE[0],
        "votingOptions": [
            {
                "optionIndex": 1,
                "option": "Option A",
                "description": "Desc A",
                "votes": 200,
            },
            {
                "optionIndex": 2,
                "option": "Option B",
                "description": None,
                "votes": 235,
            },
        ],
    }
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39").mock(
        return_value=httpx.Response(200, json=[voting_with_options])
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_votings(
            client=client, term=term, sitting=sitting
        )

    assert len(result.voting_options) == 2
    assert result.voting_options[0].option_label == "Option A"
    assert result.voting_options[0].description == "Desc A"


@pytest.mark.anyio
@respx.mock
async def test_scrape_votings_with_from_voting(
    term: database.Term,
    sitting: database.Sitting,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39").mock(
        return_value=httpx.Response(
            200,
            json=[
                {**VOTING_LIST_RESPONSE[0], "votingNumber": 204},
                VOTING_LIST_RESPONSE[0],
            ],
        )
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_votings(
            client=client,
            term=term,
            sitting=sitting,
            from_voting=205,
        )

    assert len(result.votings) == 1
    assert result.votings[0].number == 205


@pytest.mark.anyio
@respx.mock
async def test_scrape_clubs(term: database.Term) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/clubs").mock(
        return_value=httpx.Response(200, json=CLUB_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        clubs = await scrape.scrape_clubs(client=client, term=term)

    assert len(clubs) == 1
    assert clubs[0].club_id == "PiS"
    assert clubs[0].term_id == term.id


@pytest.mark.anyio
@respx.mock
async def test_scrape_mps(term: database.Term) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/MP").mock(
        return_value=httpx.Response(200, json=MP_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_mps(client=client, term=term)

    assert len(result.mps) == 1
    assert result.mps[0].first_name == "Andrzej"
    assert len(result.mp_to_term_links) == 1
    assert result.mp_to_term_links[0].term_id == term.id


@pytest.mark.anyio
@respx.mock
async def test_scrape_votes_single_option(
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39/205").mock(
        return_value=httpx.Response(200, json=VOTE_DETAIL_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_votes(
            client=client,
            term=term,
            sitting=sitting,
            voting=voting,
        )

    assert len(result.votes) == 1
    assert result.votes[0].vote == api_schemas.Vote.NO
    assert result.votes[0].party == "PiS"
    # Default voting option created
    assert len(result.voting_options) == 1


@pytest.mark.anyio
@respx.mock
async def test_scrape_votes_multi_option(
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39/205").mock(
        return_value=httpx.Response(200, json=VOTE_DETAIL_MULTI_OPTION_RESPONSE)
    )
    async with httpx.AsyncClient() as client:
        result = await scrape.scrape_votes(
            client=client,
            term=term,
            sitting=sitting,
            voting=voting,
        )

    # 1 MP x 2 options = 2 vote records
    assert len(result.votes) == 2
    assert len(result.voting_options) == 2


@pytest.mark.anyio
@respx.mock
async def test_scrape_votes_vote_valid_without_list_votes_raises(
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    bad_response = {
        **VOTE_DETAIL_RESPONSE,
        "votes": [
            {
                "MP": 1,
                "club": "PiS",
                "firstName": "Andrzej",
                "lastName": "Adamczyk",
                "vote": "VOTE_VALID",
            },
        ],
    }
    respx.get(f"{MOCK_BASE_URL}/term10/votings/39/205").mock(
        return_value=httpx.Response(200, json=bad_response)
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(
            ValueError,
            match="multiple_option_votes",
        ):
            await scrape.scrape_votes(
                client=client,
                term=term,
                sitting=sitting,
                voting=voting,
            )
