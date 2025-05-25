import duckdb
import pytest # For potential use of fixtures or markers later, good practice
from sejm_scraper.database import create_tables_if_not_exists

def test_create_tables_if_not_exists():
    """
    Tests the create_tables_if_not_exists function to ensure all tables
    are created with the correct basic schema.
    """
    conn = None
    try:
        conn = duckdb.connect(':memory:')
        create_tables_if_not_exists(conn)

        # 1. Verify all expected tables are created
        tables = conn.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='main' ORDER BY table_name;").fetchall()
        table_names = [table[0] for table in tables]

        expected_tables = sorted([
            "Terms", "Sittings", "Votings", "VotingOptions", "MPs", "Votes", "MpToTermLink"
        ])
        assert table_names == expected_tables, f"Expected tables {expected_tables}, but got {table_names}"

        # 2. Verify schema for 'Terms' table
        terms_info = conn.execute("PRAGMA table_info('Terms')").fetchall()
        # (cid, name, type, notnull, dflt_value, pk)
        # For DuckDB: type can be e.g. 'INTEGER', 'VARCHAR', 'DATE', 'TIMESTAMP'
        # notnull is 0 for False, 1 for True
        # pk is 0 for False, 1 for True
        
        # Convert to a more comparable format: (name, type, notnull, pk)
        # Note: dflt_value is often None, so we might skip it for simplicity unless specific defaults are set.
        # For VARCHAR, DuckDB PRAGMA table_info might show 'VARCHAR' without size.
        
        # Helper to extract relevant parts for comparison
        def extract_pragma_info(row):
            return (row[1], row[2], row[3], row[5]) # name, type, notnull, pk

        terms_schema = [extract_pragma_info(row) for row in terms_info]

        assert ('id', 'VARCHAR', 0, 1) in terms_schema # id VARCHAR, notnull=0 (can be null if not PK, but it is PK), pk=1
        assert ('number', 'INTEGER', 1, 0) in terms_schema # number INTEGER, notnull=1, pk=0
        assert ('from_date', 'DATE', 1, 0) in terms_schema # from_date DATE, notnull=1, pk=0
        assert ('to_date', 'DATE', 0, 0) in terms_schema # to_date DATE, notnull=0 (nullable), pk=0

        # 3. Verify schema for 'Votes' table (as another example)
        votes_info = conn.execute("PRAGMA table_info('Votes')").fetchall()
        votes_schema = [extract_pragma_info(row) for row in votes_info]

        assert ('id', 'VARCHAR', 0, 1) in votes_schema
        assert ('voting_option_id', 'VARCHAR', 1, 0) in votes_schema # Foreign key, so NOT NULL
        assert ('mp_id', 'VARCHAR', 1, 0) in votes_schema # Foreign key, so NOT NULL
        assert ('vote', 'VARCHAR', 1, 0) in votes_schema
        assert ('party', 'VARCHAR', 0, 0) in votes_schema # Nullable

        # 4. Check Foreign Key constraints (example for Sittings referencing Terms)
        # DuckDB's PRAGMA foreign_key_list(table_name) can be used.
        # PRAGMA foreign_key_list('Sittings');
        # Returns: id, seq, table, from, to, on_update, on_delete, match
        
        sittings_fk_info = conn.execute("PRAGMA foreign_key_list('Sittings');").fetchall()
        # We expect one FK from Sittings.term_id to Terms.id
        found_sittings_fk = False
        for fk_row in sittings_fk_info:
            if fk_row[2] == 'Terms' and fk_row[3] == 'term_id' and fk_row[4] == 'id':
                found_sittings_fk = True
                break
        assert found_sittings_fk, "Foreign key from Sittings.term_id to Terms.id not found or misconfigured."

        # Check a table with multiple FKs, e.g., Votes
        votes_fk_info = conn.execute("PRAGMA foreign_key_list('Votes');").fetchall()
        fk_targets_in_votes = {(fk[2], fk[3], fk[4]) for fk in votes_fk_info} # (referenced_table, from_col, to_col)
        
        assert ('VotingOptions', 'voting_option_id', 'id') in fk_targets_in_votes, \
            "FK Votes.voting_option_id -> VotingOptions.id missing."
        assert ('MPs', 'mp_id', 'id') in fk_targets_in_votes, \
            "FK Votes.mp_id -> MPs.id missing."

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    pytest.main()
