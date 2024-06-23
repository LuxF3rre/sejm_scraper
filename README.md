# Sejm Scraper

[![building - Poetry](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/python-poetry/website/main/static/badge/v0.json)](https://python-poetry.org/)
[![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![code style - black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![imports - isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![mypy - checked](https://img.shields.io/badge/mypy-checked-blue.svg)](https://mypy-lang.org/)
[![Build](https://github.com/LuxF3rre/sejm_scraper/actions/workflows/python.yml/badge.svg)](https://github.com/LuxF3rre/sejm_scraper/actions/workflows/python.yml)

## Usage

### Install dependencies

```console
poetry install
```

### Launch database

```console
docker-compose up
```

### Start straping

```console
sejm-scraper
```

## Development

### Install dependencies

```console
poetry install --with dev
pre-commit install
```
