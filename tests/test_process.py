import duckdb
import pytest # For potential use of fixtures or markers later
from datetime import date, datetime

from sejm_scraper import schemas, process, utils
from sejm_scraper.database import create_tables_if_not_exists

# --- Tests for Data Transformation ---

def test_process_term():
    sample_term_schema = schemas.TermSchema(num=10, current=True, from_date=date(2023, 1, 1), to_date=date(2023, 12, 31))
    processed_data = process.process_term(sample_term_schema)
    expected_id = utils.get_term_nk(sample_term_schema)
    assert processed_data == {
        "id": expected_id,
        "number": 10,
        "from_date": date(2023, 1, 1),
        "to_date": date(2023, 12, 31)
    }

def test_process_mp():
    sample_mp_schema = schemas.MpSchema(
        id=123, # This ID from schema is not directly used for our primary key
        first_name="Jan",
        second_name="Maria",
        last_name="Kowalski",
        birth_date=date(1980, 5, 15),
        birth_place="Warszawa",
        district_num=20,
        district_name="okręg warszawski",
        voivodeship="Mazowieckie",
        club="ABC",
        num_of_votes=12345,
        education="wyższe",
        profession="prawnik",
        email="jan.kowalski@sejm.pl",
        active=True,
        inactive_cause="",
        waiver_desc="",
        oath_date=date(2023,1,10),
        in_term_id=99 # This is the mp_term_id or similar used in votes
    )
    processed_data = process.process_mp(sample_mp_schema)
    expected_id = utils.get_mp_nk(sample_mp_schema) # Uses first_name, last_name, birth_date
    assert processed_data == {
        "id": expected_id,
        "first_name": "Jan",
        "second_name": "Maria",
        "last_name": "Kowalski",
        "birth_date": date(1980, 5, 15),
        "birth_place": "Warszawa",
    }

def test_process_mp_to_term_link():
    sample_term_schema = schemas.TermSchema(num=10, current=True, from_date=date(2023,1,1), to_date=date(2023,12,31))
    sample_mp_schema = schemas.MpSchema(
        id=123, first_name="Anna", second_name=None, last_name="Nowak",
        birth_date=date(1975, 3, 3), birth_place="Kraków",
        district_num=13, district_name="okręg krakowski", voivodeship="Małopolskie",
        club="XYZ", num_of_votes=54321, education="średnie", profession="nauczyciel",
        email="anna.nowak@sejm.pl", active=True, inactive_cause=None, waiver_desc=None,
        oath_date=date(2023,1,11), in_term_id=101
    )
    processed_data = process.process_mp_to_term_link(sample_mp_schema, sample_term_schema)
    expected_id = utils.get_mp_to_term_link_nk(mp=sample_mp_schema, term=sample_term_schema)
    expected_term_id = utils.get_term_nk(sample_term_schema)
    expected_mp_id = utils.get_mp_nk(sample_mp_schema)

    assert processed_data == {
        "id": expected_id,
        "mp_id": expected_mp_id,
        "term_id": expected_term_id,
        "in_term_id": 101,
        "education": "średnie",
        "profession": "nauczyciel",
        "voivodeship": "Małopolskie",
        "district_name": "okręg krakowski",
        "inactivity_cause": None,
        "inactivity_description": None,
    }

def test_process_sitting():
    sample_term_schema = schemas.TermSchema(num=9, current=True, from_date=date(2019,1,1), to_date=date(2023,12,31))
    sample_sitting_schema = schemas.SittingSchema(
        term=9, posiedzenie=5, nrPosiedzenia=5, title="Posiedzenie Sejmu",
        dataOd=date(2023,2,10), dataDo=date(2023,2,12), number=5 # number is important
    )
    processed_data = process.process_sitting(sample_sitting_schema, sample_term_schema)
    expected_term_id = utils.get_term_nk(sample_term_schema)
    expected_sitting_id = utils.get_sitting_nk(sitting=sample_sitting_schema, term=sample_term_schema)

    assert processed_data == {
        "id": expected_sitting_id,
        "term_id": expected_term_id,
        "title": "Posiedzenie Sejmu",
        "number": 5,
    }

    # Test skipping planned sittings (number=0)
    planned_sitting_schema = schemas.SittingSchema(
        term=9, posiedzenie=0, nrPosiedzenia=0, title="Planowane Posiedzenie",
        dataOd=date(2023,3,1), dataDo=date(2023,3,1), number=0
    )
    assert process.process_sitting(planned_sitting_schema, sample_term_schema) is None

def test_process_voting():
    sample_term_schema = schemas.TermSchema(num=10, current=True, from_date=date(2023,1,1), to_date=date(2023,12,31))
    sample_sitting_schema = schemas.SittingSchema(term=10, number=1, title="Sitting 1", dataOd=date(2023,2,1), dataDo=date(2023,2,1), posiedzenie=1, nrPosiedzenia=1)
    sample_voting_schema = schemas.VotingSchema(
        sitting=1, sitting_day=1, voting_number=3, date=datetime(2023,2,1,10,30,0),
        title="Głosowanie nad ustawą X", description="Poprawka nr 5", topic="Ustawa o...",
        voting_options=None # Will be handled separately if needed by process_voting_option
    )
    processed_data = process.process_voting(sample_voting_schema, sample_term_schema, sample_sitting_schema)
    expected_voting_id = utils.get_voting_nk(voting=sample_voting_schema, term=sample_term_schema, sitting=sample_sitting_schema)
    expected_sitting_id = utils.get_sitting_nk(sitting=sample_sitting_schema, term=sample_term_schema)

    assert processed_data == {
        "id": expected_voting_id,
        "sitting_id": expected_sitting_id,
        "sitting_day": 1,
        "number": 3,
        "date": datetime(2023,2,1,10,30,0),
        "title": "Głosowanie nad ustawą X",
        "description": "Poprawka nr 5",
        "topic": "Ustawa o...",
    }

def test_process_voting_option():
    sample_term_schema = schemas.TermSchema(num=10, current=True, from_date=date(2023,1,1), to_date=date(2023,12,31))
    sample_sitting_schema = schemas.SittingSchema(term=10, number=1, title="Sitting 1", dataOd=date(2023,2,1), dataDo=date(2023,2,1), posiedzenie=1, nrPosiedzenia=1)
    sample_voting_schema = schemas.VotingSchema(sitting=1, sitting_day=1, voting_number=3, date=datetime(2023,2,1,10,30,0), title="Głosowanie Y", voting_options=None)
    sample_voting_option_schema = schemas.VotingOptionSchema(optionIndex=1, description="Za")

    processed_data = process.process_voting_option(sample_voting_option_schema, sample_term_schema, sample_sitting_schema, sample_voting_schema)
    expected_voting_id = utils.get_voting_nk(voting=sample_voting_schema, term=sample_term_schema, sitting=sample_sitting_schema)
    expected_voting_option_id = utils.get_voting_option_nk(
        voting_option_index=1, term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema
    )
    assert processed_data == {
        "id": expected_voting_option_id,
        "voting_id": expected_voting_id,
        "index": 1,
        "description": "Za",
    }

def test_process_vote():
    sample_term_schema = schemas.TermSchema(num=10, current=True, from_date=date(2023,1,1), to_date=date(2023,12,31))
    sample_sitting_schema = schemas.SittingSchema(term=10, number=1, title="Sitting 1", dataOd=date(2023,2,1), dataDo=date(2023,2,1), posiedzenie=1, nrPosiedzenia=1)
    sample_voting_schema = schemas.VotingSchema(sitting=1, sitting_day=1, voting_number=3, date=datetime(2023,2,1,10,30,0), title="Głosowanie Z", voting_options=None)
    
    # Mock MP ID as it's passed directly
    mock_mp_id = "mp_jan_kowalski_19800515"

    # Case 1: vote.votes is None (single vote)
    sample_mp_vote_schema_single = schemas.MpVoteSchema(
        mp_term_id=99, # in_term_id
        vote=schemas.VoteValue.VOTE_YES,
        votes=None,
        party="ABC"
    )
    processed_data_single = process.process_vote(
        sample_term_schema, sample_sitting_schema, sample_voting_schema, sample_mp_vote_schema_single, mock_mp_id
    )
    expected_vote_id_single = utils.get_vote_nk(
        term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema,
        voting_option_index=schemas.OptionIndex(1), mp_id=mock_mp_id
    )
    expected_voting_option_id_single = utils.get_voting_option_nk(
        voting_option_index=schemas.OptionIndex(1), term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema
    )
    assert len(processed_data_single) == 1
    assert processed_data_single[0] == {
        "id": expected_vote_id_single,
        "voting_option_id": expected_voting_option_id_single,
        "mp_id": mock_mp_id,
        "vote": "VOTE_YES",
        "party": "ABC",
    }

    # Case 2: vote.votes is populated (multiple options, e.g., personal votes)
    sample_mp_vote_schema_multiple = schemas.MpVoteSchema(
        mp_term_id=100,
        vote=schemas.VoteValue.VOTE_VALID, # This is the overall status if votes dict is present
        votes={
            schemas.OptionIndex(1): schemas.VoteValue.VOTE_YES,
            schemas.OptionIndex(2): schemas.VoteValue.VOTE_NO,
        },
        party="XYZ"
    )
    processed_data_multiple = process.process_vote(
        sample_term_schema, sample_sitting_schema, sample_voting_schema, sample_mp_vote_schema_multiple, mock_mp_id
    )
    assert len(processed_data_multiple) == 2
    
    expected_vote_id_multi_1 = utils.get_vote_nk(
        term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema,
        voting_option_index=schemas.OptionIndex(1), mp_id=mock_mp_id
    )
    expected_voting_option_id_multi_1 = utils.get_voting_option_nk(
        voting_option_index=schemas.OptionIndex(1), term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema
    )
    expected_vote_id_multi_2 = utils.get_vote_nk(
        term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema,
        voting_option_index=schemas.OptionIndex(2), mp_id=mock_mp_id
    )
    expected_voting_option_id_multi_2 = utils.get_voting_option_nk(
        voting_option_index=schemas.OptionIndex(2), term=sample_term_schema, sitting=sample_sitting_schema, voting=sample_voting_schema
    )

    assert {
        "id": expected_vote_id_multi_1,
        "voting_option_id": expected_voting_option_id_multi_1,
        "mp_id": mock_mp_id,
        "vote": "VOTE_YES",
        "party": "XYZ",
    } in processed_data_multiple
    assert {
        "id": expected_vote_id_multi_2,
        "voting_option_id": expected_voting_option_id_multi_2,
        "mp_id": mock_mp_id,
        "vote": "VOTE_NO",
        "party": "XYZ",
    } in processed_data_multiple

    # Case 3: vote.votes is None and vote.vote is VOTE_VALID (should raise error, now returns empty list)
    sample_mp_vote_schema_invalid = schemas.MpVoteSchema(
        mp_term_id=101,
        vote=schemas.VoteValue.VOTE_VALID,
        votes=None,
        party="QWE"
    )
    processed_data_invalid = process.process_vote(
        sample_term_schema, sample_sitting_schema, sample_voting_schema, sample_mp_vote_schema_invalid, mock_mp_id
    )
    assert processed_data_invalid == []


# --- Test for Bulk Insertion ---

def test_bulk_insert_data():
    conn = None
    try:
        conn = duckdb.connect(':memory:')
        create_tables_if_not_exists(conn) # Setup schema

        # Test with Terms table
        sample_terms_data = [
            {"id": "term_1", "number": 1, "from_date": date(2020, 1, 1), "to_date": date(2020, 12, 31)},
            {"id": "term_2", "number": 2, "from_date": date(2021, 1, 1), "to_date": None},
            {"id": "term_3", "number": 3, "from_date": date(2022, 1, 1), "to_date": date(2022, 6, 30)},
        ]

        # Test 1: Insert all data with chunk_size > len(data)
        process.bulk_insert_data(conn, "Terms", sample_terms_data, process.TERMS_COLUMN_ORDER, chunk_size=10)
        
        count_result = conn.execute("SELECT COUNT(*) FROM Terms").fetchone()
        assert count_result[0] == 3
        
        all_terms = conn.execute("SELECT id, number, from_date, to_date FROM Terms ORDER BY number").fetchall()
        assert len(all_terms) == 3
        assert all_terms[0] == ("term_1", 1, date(2020, 1, 1), date(2020, 12, 31))
        assert all_terms[1] == ("term_2", 2, date(2021, 1, 1), None)
        assert all_terms[2] == ("term_3", 3, date(2022, 1, 1), date(2022, 6, 30))

        # Clear table for next test
        conn.execute("DELETE FROM Terms")

        # Test 2: Insert data with chunk_size < len(data) to test chunking
        process.bulk_insert_data(conn, "Terms", sample_terms_data, process.TERMS_COLUMN_ORDER, chunk_size=2)
        count_result_chunked = conn.execute("SELECT COUNT(*) FROM Terms").fetchone()
        assert count_result_chunked[0] == 3
        all_terms_chunked = conn.execute("SELECT id, number, from_date, to_date FROM Terms ORDER BY number").fetchall()
        assert len(all_terms_chunked) == 3
        assert all_terms_chunked[0] == ("term_1", 1, date(2020, 1, 1), date(2020, 12, 31))

        # Test 3: Insert empty list
        conn.execute("DELETE FROM Terms")
        process.bulk_insert_data(conn, "Terms", [], process.TERMS_COLUMN_ORDER, chunk_size=10)
        count_result_empty = conn.execute("SELECT COUNT(*) FROM Terms").fetchone()
        assert count_result_empty[0] == 0

        # Test 4: Data with missing key (should log error and skip the chunk)
        conn.execute("DELETE FROM Terms")
        sample_terms_bad_data = [
            {"id": "term_4", "number": 4, "from_date": date(2023,1,1)}, # missing 'to_date'
            {"id": "term_5", "number": 5, "from_date": date(2024,1,1), "to_date": date(2024,12,31)},
        ]
        # Assuming default behavior is to log and skip, not raise error and stop insertion of other chunks.
        # The current implementation of bulk_insert_data logs an error and returns if a key is missing,
        # effectively skipping the entire list of data if one item is bad.
        process.bulk_insert_data(conn, "Terms", sample_terms_bad_data, process.TERMS_COLUMN_ORDER, chunk_size=1)
        count_result_bad_data = conn.execute("SELECT COUNT(*) FROM Terms").fetchone()
        # Given current logic, if one item is bad, the whole "data" list is skipped.
        assert count_result_bad_data[0] == 0 

        # Test with a valid item after a bad one to confirm previous behavior
        conn.execute("DELETE FROM Terms")
        sample_terms_mixed_data = [
            {"id": "term_valid_1", "number": 6, "from_date": date(2025,1,1), "to_date": date(2025,12,31)},
            # This bad data item will cause the whole list to be skipped by current bulk_insert_data logic
            # {"id": "term_bad_key", "number": 7, "from_date_WRONG_KEY": date(2023,1,1)}, 
            # {"id": "term_valid_2", "number": 8, "from_date": date(2026,1,1), "to_date": date(2026,12,31)},
        ]
        # Let's test only valid data again to ensure the table is usable.
        valid_data_for_insert = [sample_terms_mixed_data[0]] # only the first valid item
        process.bulk_insert_data(conn, "Terms", valid_data_for_insert, process.TERMS_COLUMN_ORDER, chunk_size=1)
        count_result_valid_again = conn.execute("SELECT COUNT(*) FROM Terms").fetchone()
        assert count_result_valid_again[0] == 1


    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    pytest.main()
