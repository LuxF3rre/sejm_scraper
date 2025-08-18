from datetime import date

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sejm_scraper import database_key_utils


def test_empty() -> None:
    msg = "hash elements cannot be empty"
    with pytest.raises(
        ValueError,
        match=msg,
    ):
        database_key_utils._generate_hash("")


multiple_types_not_null_not_empty = (
    st.text(min_size=1)
    | st.integers()
    | st.floats()
    | st.booleans()
    | st.dates()
)


@given(
    st.tuples(
        multiple_types_not_null_not_empty,
        multiple_types_not_null_not_empty,
    )
)
def test_fuzzy(
    s: tuple[
        str | int | float | bool | date,
        str | int | float | bool | date,
    ],
) -> None:
    database_key_utils._generate_hash(*s)
