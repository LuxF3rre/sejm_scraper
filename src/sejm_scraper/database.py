import os

import duckdb

DUCKDB_FILE = os.getenv("SEJM_SCRAPER_DUCKDB_FILE", "sejm_scraper.duckdb")


def get_duckdb_connection():
    return duckdb.connect(DUCKDB_FILE, read_only=False)


def create_tables_if_not_exists(conn):
    """
    Creates tables in the DuckDB database based on the SQLAlchemy models
    if they do not already exist.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Terms (
            id VARCHAR PRIMARY KEY,
            number INTEGER NOT NULL,
            from_date DATE NOT NULL,
            to_date DATE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Sittings (
            id VARCHAR PRIMARY KEY,
            term_id VARCHAR NOT NULL REFERENCES Terms(id),
            title VARCHAR NOT NULL,
            number INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Votings (
            id VARCHAR PRIMARY KEY,
            sitting_id VARCHAR NOT NULL REFERENCES Sittings(id),
            sitting_day INTEGER NOT NULL,
            number INTEGER NOT NULL,
            date TIMESTAMP NOT NULL,
            title VARCHAR NOT NULL,
            description VARCHAR,
            topic VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS VotingOptions (
            id VARCHAR PRIMARY KEY,
            voting_id VARCHAR NOT NULL REFERENCES Votings(id),
            index INTEGER NOT NULL,
            description VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS MPs (
            id VARCHAR PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            second_name VARCHAR,
            last_name VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            birth_place VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Votes (
            id VARCHAR PRIMARY KEY,
            voting_option_id VARCHAR NOT NULL REFERENCES VotingOptions(id),
            mp_id VARCHAR NOT NULL REFERENCES MPs(id),
            vote VARCHAR NOT NULL,
            party VARCHAR
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS MpToTermLink (
            id VARCHAR PRIMARY KEY,
            mp_id VARCHAR NOT NULL REFERENCES MPs(id),
            term_id VARCHAR NOT NULL REFERENCES Terms(id),
            in_term_id INTEGER NOT NULL,
            education VARCHAR,
            profession VARCHAR,
            voivodeship VARCHAR,
            district_name VARCHAR NOT NULL,
            inactivity_cause VARCHAR,
            inactivity_description VARCHAR
        );
        """
    )
