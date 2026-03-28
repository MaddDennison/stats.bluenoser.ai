# Contributing to Stats Bluenoser

Thanks for your interest in contributing. This document covers how to set up a development environment, run the pipeline, and submit changes.

## Prerequisites

- Python 3.12+
- Docker and Docker Compose
- Hugo (for the static site)
- Git

## Setup

```bash
# Clone the repo
git clone git@github.com:MaddDennison/stats.bluenoser.ai.git
cd stats.bluenoser.ai

# Create a Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest

# Copy environment config
cp .env.example .env
# Edit .env with your DATABASE_URL and any API keys

# Start the database
docker compose up -d db

# Verify the database is seeded
make smoke-test
```

## Project Structure

```
pipeline/           Core data pipeline
  statcan_client.py   StatsCan WDS API client
  ingester.py         Data ingestion + revision detection
  analyzer.py         Claude API release generation
  publisher.py        Hugo publishing + newsletter
  chart_generator.py  Static chart generation
  config.py           Table watchlist and vector mappings
  db.py               Database connection management
  run_daily.py        Daily pipeline orchestrator

scripts/            Utility scripts
  backfill.py         Historical data loader
  test_api.py         API smoke test
  map_vectors.py      Vector discovery tool
  generate_release.py Release generation CLI
  verify_data.py      Data quality checker

site/               Hugo static site
db/                 Schema, seeds, migrations
tests/              Test suite
```

## Running the Pipeline

```bash
# Full pipeline (needs StatsCan API access)
python -m pipeline.run_daily --force --no-analyze

# Backfill a specific table
python -m scripts.backfill 18100004 5   # CPI, 5 years

# Generate a release (needs ANTHROPIC_API_KEY)
python -m scripts.generate_release cpi 2026-02-01 --dry-run

# Preview data context without calling Claude
python -m scripts.generate_release lfs 2026-02-01 --context-only

# Build the Hugo site
cd site && hugo server   # Local preview at localhost:1313

# Run with Docker
docker compose up --build
```

## Running Tests

```bash
python -m pytest tests/ -v
```

Tests use mocked HTTP responses and don't require a database or API access. All tests should pass before submitting a PR.

## Adding a New StatsCan Table

1. **Discover vectors**: `python -m scripts.map_vectors <PID>` (e.g., `18100004`)
2. **Add to watchlist**: Edit `pipeline/config.py` — add the table to `WATCHLIST` with its vectors
3. **Backfill data**: `python -m scripts.backfill <PID>`
4. **Verify data**: `python -m scripts.verify_data <PID>`
5. **Create a prompt template** (optional): Add to `pipeline/templates/` and wire into `analyzer.py`

## Code Style

- Python: follow existing patterns in the codebase
- No unnecessary abstractions — prefer simple, readable code
- Parameterized SQL queries only (no string interpolation)
- All AI-generated content must be explicitly labeled

## Submitting Changes

1. Create a feature branch from `main`
2. Make your changes
3. Run `python -m pytest tests/ -v` — all tests must pass
4. Commit with a clear message describing what and why
5. Open a pull request against `main`

## Data Licensing

All Statistics Canada data is used under the [Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada). When adding new data sources, verify their licence permits redistribution.
