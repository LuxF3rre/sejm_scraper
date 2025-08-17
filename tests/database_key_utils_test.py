import pytest
from hypothesis import given
from hypothesis import strategies as st

from sejm_scraper import database_key_utils


def test_none_and_empty():
    with pytest.raises(
        ValueError,
        match=database_key_utils.NULL_OR_EMPTY_HASH_ELEMENT,
    ):
        database_key_utils._generate_hash(None)
    with pytest.raises(
        ValueError,
        match=database_key_utils.NULL_OR_EMPTY_HASH_ELEMENT,
    ):
        database_key_utils._generate_hash("")


multiple_types_not_null_not_empty = (
    st.integers()
    | st.floats()
    | st.booleans()
    | st.text(min_size=1)
    | st.dates()
)


@given(
    st.tuples(
        multiple_types_not_null_not_empty,
        multiple_types_not_null_not_empty,
        multiple_types_not_null_not_empty,
        multiple_types_not_null_not_empty,
        multiple_types_not_null_not_empty,
    )
)
def test_fuzzy(s):
    database_key_utils._generate_hash(*s)
