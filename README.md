# Sejm Scraper

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![code style - Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![imports - isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![ty - checked](https://img.shields.io/badge/ty-checked-green)](https://github.com/astral-sh/ty)
[![Build](https://github.com/LuxF3rre/sejm_scraper/actions/workflows/test.yml/badge.svg)](https://github.com/LuxF3rre/sejm_scraper/actions/workflows/test.yml)

## Overview

The [Sejm API](https://api.sejm.gov.pl/) facilitates access to comprehensive details about the terms, sittings, votings, votes, and MPs of the [Polish Sejm](https://en.wikipedia.org/wiki/Sejm). However, it presents several challenges:

- Absence of primary and foreign keys.
- API documentation does not specify nullable constraints.
- Inconsistent handling of votings that involve single or multiple voting options.
- MPs are defined per term rather than being treated as continuous entities across different terms.

To address these issues, the following solutions have been implemented:

- Creation of a database that includes tables with [natural keys](https://en.wikipedia.org/wiki/Natural_key), utilizing SHA-256 for hashing and enforced key constraints.
- Implementation of API response schema validation alongside null constraints within the database.
- Normalization of votings to accommodate single-option scenarios uniformly.

Furthermore, maintaining a local copy of the data ensures rapid access to the entire dataset, significantly enhancing analysis capabilities.

### Data quality notes

The Sejm API models MPs on a term-by-term basis rather than maintaining a continuous, global MP entity. Turning them into continuous entities is not a trival issue due to various inconsistencies originating from the Sejm API, including:

- Data entry errors, such as typos or inconsistent naming of birthplaces.
- Changes in an MP's last name, commonly due to marriage.
- Previously missing fields that have been added later and are integral to our primary key, like birthplace.

## Features

- [x] Built with **🐍Python** and **🦆DuckDB**.
- [x] Normalized data model with primary keys, foreign keys, and not null constrains.
- [x] Fast and reliable processing thanks to the custom client for [Sejm API](https://api.sejm.gov.pl/sejm/openapi/ui).
- [x] Able to resume work from a given term, sitting, and voting.

## Data model

```mermaid
erDiagram
    Term {
        string id PK
        int number
        date from_date
        date to_date "nullable"
    }

    Sitting {
        string id PK
        string term_id FK
        string title
        int number
    }

    Voting {
        string id PK
        string sitting_id FK
        int number
        int day_number
        date date
        string title
        string description "nullable"
        string topic "nullable"
    }

    VotingOption {
        string id PK
        string voting_id FK
        int index
        string description "nullable"
    }

    PartyInTerm {
        string id PK
        string term_id FK
        string abbreviation
        string name
        string phone "nullable"
        string fax "nullable"
        string email "nullable"
        int member_count
    }

    Vote {
        string id PK
        string voting_option_id FK
        string mp_in_term_id FK
        string party_in_term_id FK "nullable"
        string vote
    }

    MpInTerm {
        string id PK
        string term_id FK
        int in_term_id
        string first_name
        string second_name "nullable"
        string last_name
        date birth_date
        string birth_place "nullable"
        string education "nullable"
        string profession "nullable"
        string voivodeship "nullable"
        string district_name
        string inactivity_cause "nullable"
        string inactivity_description "nullable"
    }

    Term ||--o{ Sitting : "contains"
    Term ||--o{ MpInTerm : "has"
    Term ||--o{ PartyInTerm : "has"
    Sitting ||--o{ Voting : "contains"
    Voting ||--o{ VotingOption : "has"
    VotingOption ||--o{ Vote : "receives"
    MpInTerm ||--o{ Vote : "casts"
    PartyInTerm ||--o{ Vote : "in"
```

## Installation & usage

### 0. Requirements

- git
- Python 3.12

### 1. Clone and navigate into the repository

```console
git clone https://github.com/LuxF3rre/sejm_scraper && cd sejm_scraper
```

### 2. Install dependencies

```console
pip install -r requirements.txt
pip install -e .
```

If you have uv:

```console
uv venv
source .venv/bin/activate
uv sync
uv pip install -e .
```

### 3. Create the database

```console
python -m sejm-scraper prepare-database
```

### 4. Start scraping

```console
python -m sejm-scraper start-pipeline
```

### 5. Resume scraping

#### From the latest available point in the database

```console
python -m sejm-scraper resume-pipeline
```

#### From a specific point

```console
python -m sejm-scraper resume-pipeline --term <number> [--sitting <number> [--voting <number>]]
```

### 6. See help

```console
python -m sejm-scraper --help
```

## Limitations

This project's scope is constrained by the data availability from the Sejm API:

1. Absence of MP data for term 2.
2. Limited to only term and MP data for terms 3 through 7 and votes data from term 8 onwards.
3. Absence of exact dates of becoming active or inactive for MPs as well as changing the party.
4. MP data bounded to terms, instead of treating them as continuous enitities.

To address the first two gaps, future development efforts should aim to source the missing data directly from the Sejm's official website. The data is not exposed directly on the webpage, but can be obtained by using the following URL pattern:

`https://sejm.gov.pl/sejm10.nsf/agent.xsp?symbol=glosowania&NrKadencji={term_number}&NrPosiedzenia={sitting_number}&NrGlosowania={voting_number}`

For example:

`https://sejm.gov.pl/sejm10.nsf/agent.xsp?symbol=glosowania&NrKadencji=3&NrPosiedzenia=6&NrGlosowania=2`

## References

Sejm API:

- [ELI & Sejm API documentation](https://api.sejm.gov.pl/)
- [API for Polish Sejm Swagger UI](https://api.sejm.gov.pl/sejm/openapi/ui)

Similar projects:

- [Sejm VIII Kadencji](https://github.com/prokulski/sejm_viii_kadencji/)

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT License
