from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
import sqlmodel

from sejm_scraper import database, pipeline, scrape

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Engine


@pytest.fixture
def _mock_scrape(
    monkeypatch: pytest.MonkeyPatch,
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    """Patch all scrape functions with minimal return values."""
    monkeypatch.setattr(
        scrape,
        "scrape_terms",
        AsyncMock(return_value=[term]),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_mps",
        AsyncMock(
            return_value=scrape.ScrapedMpsResult(mps=[], mp_to_term_links=[])
        ),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_clubs",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_sittings",
        AsyncMock(
            return_value=scrape.ScrapedSittingsResult(
                sittings=[sitting], sitting_days=[]
            )
        ),
    )
    monkeypatch.setattr(
        scrape,
        "discover_sittings_from_votings",
        AsyncMock(
            return_value=scrape.ScrapedSittingsResult(
                sittings=[], sitting_days=[]
            )
        ),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_votings",
        AsyncMock(
            return_value=scrape.ScrapedVotingsResult(
                votings=[voting], voting_options=[]
            )
        ),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_votes",
        AsyncMock(
            return_value=scrape.ScrapedVotesResult(votes=[], voting_options=[])
        ),
    )


@pytest.mark.anyio
async def test_pipeline_validates_from_voting_without_from_sitting(
    engine: "Engine",
) -> None:
    with pytest.raises(ValueError, match="from_voting"):
        await pipeline.pipeline(
            engine=engine,
            from_term=10,
            from_voting=205,
        )


@pytest.mark.anyio
async def test_pipeline_validates_from_sitting_without_from_term(
    engine: "Engine",
) -> None:
    with pytest.raises(ValueError, match="from_sitting"):
        await pipeline.pipeline(
            engine=engine,
            from_sitting=39,
        )


@pytest.mark.anyio
@pytest.mark.usefixtures("_mock_scrape")
async def test_pipeline_full_flow(engine: "Engine") -> None:
    await pipeline.pipeline(engine=engine)

    with sqlmodel.Session(engine) as session:
        terms = session.exec(sqlmodel.select(database.Term)).all()
        assert len(terms) == 1
        assert terms[0].number == 10

        sittings = session.exec(sqlmodel.select(database.Sitting)).all()
        assert len(sittings) == 1

        votings = session.exec(sqlmodel.select(database.Voting)).all()
        assert len(votings) == 1


@pytest.mark.anyio
@pytest.mark.usefixtures("_mock_scrape")
async def test_pipeline_passes_from_term(engine: "Engine") -> None:
    await pipeline.pipeline(engine=engine, from_term=10)

    scrape.scrape_terms.assert_called_once()  # type: ignore[union-attr]
    call_kwargs = scrape.scrape_terms.call_args.kwargs  # type: ignore[union-attr]
    assert call_kwargs["from_term"] == 10


@pytest.mark.anyio
@pytest.mark.usefixtures("_mock_scrape")
async def test_pipeline_passes_from_sitting_only_for_matching_term(
    engine: "Engine",
) -> None:
    await pipeline.pipeline(engine=engine, from_term=10, from_sitting=39)

    scrape.scrape_sittings.assert_called_once()  # type: ignore[union-attr]
    call_kwargs = scrape.scrape_sittings.call_args.kwargs  # type: ignore[union-attr]
    assert call_kwargs["from_sitting"] == 39


@pytest.mark.anyio
async def test_pipeline_falls_back_to_discover_sittings(
    monkeypatch: pytest.MonkeyPatch,
    engine: "Engine",
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    monkeypatch.setattr(
        scrape,
        "scrape_terms",
        AsyncMock(return_value=[term]),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_mps",
        AsyncMock(
            return_value=scrape.ScrapedMpsResult(mps=[], mp_to_term_links=[])
        ),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_clubs",
        AsyncMock(return_value=[]),
    )
    # scrape_sittings returns empty → triggers fallback
    monkeypatch.setattr(
        scrape,
        "scrape_sittings",
        AsyncMock(
            return_value=scrape.ScrapedSittingsResult(
                sittings=[], sitting_days=[]
            )
        ),
    )
    monkeypatch.setattr(
        scrape,
        "discover_sittings_from_votings",
        AsyncMock(
            return_value=scrape.ScrapedSittingsResult(
                sittings=[sitting], sitting_days=[]
            )
        ),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_votings",
        AsyncMock(
            return_value=scrape.ScrapedVotingsResult(
                votings=[voting], voting_options=[]
            )
        ),
    )
    monkeypatch.setattr(
        scrape,
        "scrape_votes",
        AsyncMock(
            return_value=scrape.ScrapedVotesResult(votes=[], voting_options=[])
        ),
    )

    await pipeline.pipeline(engine=engine)

    scrape.discover_sittings_from_votings.assert_called_once()  # type: ignore[union-attr]

    with sqlmodel.Session(engine) as session:
        sittings = session.exec(sqlmodel.select(database.Sitting)).all()
        assert len(sittings) == 1


@pytest.mark.anyio
async def test_resume_pipeline_cold_start(
    monkeypatch: pytest.MonkeyPatch,
    engine: "Engine",
) -> None:
    """No existing data → calls pipeline with no resume args."""
    mock_pipeline = AsyncMock()
    monkeypatch.setattr(pipeline, "pipeline", mock_pipeline)

    await pipeline.resume_pipeline(engine=engine)

    mock_pipeline.assert_called_once_with(engine=engine)


@pytest.mark.anyio
async def test_resume_pipeline_with_existing_term(
    monkeypatch: pytest.MonkeyPatch,
    engine: "Engine",
    term: database.Term,
) -> None:
    """Existing term but no sitting → resumes from that term."""
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term],
        )
        session.commit()

    mock_pipeline = AsyncMock()
    monkeypatch.setattr(pipeline, "pipeline", mock_pipeline)

    await pipeline.resume_pipeline(engine=engine)

    mock_pipeline.assert_called_once_with(engine=engine, from_term=10)


@pytest.mark.anyio
async def test_resume_pipeline_with_existing_sitting(
    monkeypatch: pytest.MonkeyPatch,
    engine: "Engine",
    term: database.Term,
    sitting: database.Sitting,
) -> None:
    """Existing term + sitting but no voting → resumes from sitting."""
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term],
        )
        database.bulk_upsert(
            session=session,
            model=database.Sitting,
            records=[sitting],
        )
        session.commit()

    mock_pipeline = AsyncMock()
    monkeypatch.setattr(pipeline, "pipeline", mock_pipeline)

    await pipeline.resume_pipeline(engine=engine)

    mock_pipeline.assert_called_once_with(
        engine=engine, from_term=10, from_sitting=39
    )


@pytest.mark.anyio
async def test_resume_pipeline_with_existing_voting(
    monkeypatch: pytest.MonkeyPatch,
    engine: "Engine",
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> None:
    """Existing term + sitting + voting → resumes from voting."""
    with sqlmodel.Session(engine) as session:
        database.bulk_upsert(
            session=session,
            model=database.Term,
            records=[term],
        )
        database.bulk_upsert(
            session=session,
            model=database.Sitting,
            records=[sitting],
        )
        database.bulk_upsert(
            session=session,
            model=database.Voting,
            records=[voting],
        )
        session.commit()

    mock_pipeline = AsyncMock()
    monkeypatch.setattr(pipeline, "pipeline", mock_pipeline)

    await pipeline.resume_pipeline(engine=engine)

    mock_pipeline.assert_called_once_with(
        engine=engine,
        from_term=10,
        from_sitting=39,
        from_voting=205,
    )
