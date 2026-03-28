# Stats Bluenoser

Automated economic intelligence platform for Nova Scotia. Ingests data from Statistics Canada's Web Data Service API, generates AI-written analytical releases styled after government econ/stats teams, and publishes through a static website, email newsletter, and data API.

## Why This Exists

Nova Scotia's economic data is public, abundant, and almost entirely inaccessible to the people who need it most. Government econ/stats teams publish excellent analysis — but it's manually produced, static HTML, text-only, unsearchable by API, and often archived without revision tracking.

This platform closes that gap by:

- **Connecting directly to Statistics Canada's APIs** to pull data the moment it's released
- **Tracking every data revision** — when StatsCan revises GDP from 2.1% to 1.8%, we record both values and when the change was detected
- **Generating analytical releases in real time** using AI, with embedded charts and structured data
- **Publishing through modern channels** — website, newsletter, RSS, data API, and MCP server
- **Applying a Nova Scotia lens** — NS vs. national, Halifax vs. rest of province, provincial rankings

## Current Status

**Phase 1 — Core pipeline complete, publishing in progress**

- 5 StatsCan tables ingested with 80 time series and ~18,000 data points (20 years of history)
- Daily pipeline runs at 08:31 ET: checks for updates, ingests data, detects revisions, generates charts
- AI release generation ready (CPI and Labour Market templates)
- Hugo site with RSS feeds, professional styling, and AI transparency labeling
- Email newsletter template and Resend integration built
- 37 automated tests, GitHub Actions CI, full Docker support

## Architecture

```
StatsCan WDS API ──> Ingest ──> PostgreSQL ──> Analyze (Claude) ──> Publish
                                    |
                     +--------------+--------------+
                     v              v              v
                  Website       Newsletter      Charts
                  (Hugo)        (Resend)      (matplotlib)
```

**Stack:** Python, PostgreSQL, Hugo, Claude API (Sonnet), Cloudflare Pages, Resend

Every component is replaceable with a configuration change, not a rewrite. No vendor lock-in. Standard, boring technology.

## Data Coverage

### 5 Starter Tables (all backfilled with 20 years of history)

| Table | Description | Vectors | Frequency |
|-------|-------------|---------|-----------|
| 18-10-0004-01 | Consumer Price Index | 14 (NS, Halifax, Canada + components) | Monthly |
| 14-10-0287-01 | Labour force characteristics | 38 (headlines + age/gender breakdowns) | Monthly |
| 36-10-0434-01 | GDP at basic prices, by industry | 12 (national industry aggregates) | Monthly |
| 20-10-0008-01 | Retail trade sales by province | 8 (NS + Canada, total + subsectors) | Monthly |
| 34-10-0066-01 | Building permits, by type | 8 (NS, Halifax, Canada + res/non-res) | Monthly |

Expanding to 15+ additional StatsCan tables, then international sources (BLS, FRED, Eurostat, ONS, e-Stat).

## Quick Start

```bash
# Clone
git clone git@github.com:MaddDennison/stats.bluenoser.ai.git
cd stats.bluenoser.ai

# Option 1: Docker (recommended)
cp .env.example .env
docker compose up --build

# Option 2: Local development
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with DATABASE_URL
docker compose up -d db          # Start just the database
make smoke-test                  # Verify StatsCan API works
python -m scripts.backfill       # Load 20 years of historical data
python -m pipeline.run_daily --force --no-analyze  # Run the pipeline

# Preview the website
cd site && hugo server
```

## Project Structure

```
pipeline/
  statcan_client.py     StatsCan WDS API client (rate limiting, retries, 409/429 handling)
  ingester.py           Data ingestion + revision detection
  analyzer.py           Claude API release generation (CPI + Labour Market)
  publisher.py          Hugo publishing + Resend newsletter
  chart_generator.py    Static chart generation (CPI trend, unemployment rate, generic)
  config.py             Table watchlist with 80 vector mappings
  db.py                 PostgreSQL connection pool + query helpers
  run_daily.py          Daily pipeline orchestrator (CHECK > INGEST > ANALYZE > CHARTS > PUBLISH)
  logging_config.py     Structured JSON logging + rotation
  templates/            Prompt templates (CPI, labour, generic)

scripts/
  backfill.py           Historical data loader (safe to re-run)
  test_api.py           StatsCan API smoke test
  map_vectors.py        Vector discovery tool
  generate_release.py   Release generation CLI
  verify_data.py        Data quality checker
  cron_daily.sh         Cron wrapper with env loading

site/                   Hugo static site (layouts, CSS, content)
db/                     PostgreSQL schema, seeds, migrations
tests/                  37 automated tests (client, ingester, analyzer)
```

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v    # 37 tests, ~0.3s, no external dependencies
```

## AI Transparency

All AI-generated content is explicitly labeled. Every release includes:

> This content is AI-generated from Statistics Canada data and has not been reviewed by an economist.

## Data Licensing

Statistics Canada data is used under the [Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada), which permits reproduction, redistribution, and commercial use with attribution.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions, code style, and how to add new data tables.

## Deployment

See [docs/deployment.md](docs/deployment.md) for Docker, VPS, Cloudflare Pages, and Resend setup.

## Licence

TBD
