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

**Phase 1 — In Development**

The data pipeline foundation is built: database schema, StatsCan API client, and configuration for the first 5 tables (CPI, Labour Force, GDP, Retail Trade, Building Permits). Working toward generating the first end-to-end CPI release from live data.

## Architecture

```
StatsCan WDS API ──→ Ingest ──→ PostgreSQL ──→ Analyze (Claude) ──→ Publish
                                    │
                     ┌──────────────┼──────────────┐
                     ▼              ▼              ▼
                  Website       Newsletter      Data API
                  (Hugo)        (Resend)       (Phase 3)
```

**Stack:** Python, PostgreSQL (Supabase), Hugo, Claude API (Sonnet), Cloudflare Pages, Resend

Every component is replaceable with a configuration change, not a rewrite. No vendor lock-in. Standard, boring technology.

## Data Sources

### Phase 1 — Statistics Canada (5 starter tables)

| Table | Description | Frequency |
|-------|-------------|-----------|
| 18-10-0004-01 | Consumer Price Index | Monthly |
| 14-10-0287-01 | Labour force characteristics | Monthly |
| 36-10-0434-01 | GDP at basic prices, by industry | Monthly |
| 20-10-0008-01 | Retail trade sales by province | Monthly |
| 34-10-0066-01 | Building permits, by type | Monthly |

Expanding to 17+ StatsCan tables, then international sources (BLS, FRED, Eurostat, ONS, e-Stat).

## Quick Start

```bash
# Clone
git clone git@github.com:MaddDennison/stats.bluenoser.ai.git
cd stats.bluenoser.ai

# Set up Python environment
make setup
cp .env.example .env
# Edit .env with your database URL and API keys

# Start local PostgreSQL (or use Supabase)
docker compose up -d

# Verify the StatsCan API works
make smoke-test

# Discover vectors for the CPI table
python -m scripts.map_vectors 18100004
```

## Project Structure

```
stats-bluenoser/
├── db/                     # Schema, seeds, migrations
├── pipeline/
│   ├── statcan_client.py   # StatsCan WDS API client
│   ├── config.py           # Table watchlist and vector mappings
│   ├── models.py           # Data models
│   ├── ingester.py         # Data ingestion + revision detection (WIP)
│   ├── analyzer.py         # Claude API release generation (WIP)
│   └── publisher.py        # Site build + email sending (WIP)
├── scripts/
│   ├── test_api.py         # API smoke test
│   ├── map_vectors.py      # Vector discovery for a table
│   └── backfill.py         # Historical data load (WIP)
├── site/                   # Hugo static site (Phase 2)
├── mcp/                    # MCP server (Phase 3)
└── tests/
```

## AI Transparency

All AI-generated content is explicitly labeled. Every release includes:

> This content is AI-generated from Statistics Canada data and has not been reviewed by an economist.

## Data Licensing

Statistics Canada data is used under the [Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada), which permits reproduction, redistribution, and commercial use with attribution.

## Licence

TBD — see [plan document](docs/architecture.md) for licensing considerations.
