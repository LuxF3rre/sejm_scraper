from datetime import date

import pytest
import respx

from sejm_scraper import database, pipeline


@respx.mock
def test_cold_resume_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline, "_get_most_recent_voting", lambda _: None)

    database.prepare_db_and_tables()
    pipeline.resume_pipeline()


@respx.mock
def test_hot_resume_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pipeline,
        "_get_most_recent_voting",
        lambda _: database.Voting(
            id="951864298144ec8f4743a55070d31d28601962f4b17c84dcf4da9bea431161ad",
            sitting_id="00037f39cf870a1f49129f9c82d935665d352ffd25ea3296208f6f7b16fd654f",
            number=205,
            day_number=6,
            date_time=date(2025, 8, 5),
            title=(
                "Pkt. 56 Sprawozdanie z działalności"
                " Najwyższej Izby Kontroli w 2024 roku"
                " wraz z opinią Komisji (druki nr 1405 i 1417)"
            ),
            description=(
                "wniosek o przyjęcie sprawozdania"
                " z działalności Najwyższej Izby Kontroli w 2024 roku"
            ),
            topic=None,
        ),
    )

    monkeypatch.setattr(
        pipeline,
        "_get_most_recent_sitting",
        lambda _, __: database.Sitting(
            id="00037f39cf870a1f49129f9c82d935665d352ffd25ea3296208f6f7b16fd654f",
            term_id="4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
            title=(
                "39. Posiedzenie Sejmu RP w dniach"
                " 22, 23, 24 i 25 lipca oraz 4 i 5 sierpnia 2025 r."
            ),
            number=39,
        ),
    )

    monkeypatch.setattr(
        pipeline,
        "_get_most_recent_term",
        lambda _, __: database.Term(
            id="4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
            number=10,
            from_date=date(2023, 11, 13),
            to_date=None,
        ),
    )

    database.prepare_db_and_tables()
    pipeline.resume_pipeline()
