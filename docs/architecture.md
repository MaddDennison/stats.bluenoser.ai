# Architecture

## Guiding Principles

1. **No vendor lock-in.** Every component is replaceable with a configuration change, not a rewrite. PostgreSQL via standard connection strings, raw SQL, no ORM.
2. **Standard, boring technology.** Python, PostgreSQL, Hugo, Docker. Nothing that requires specialized knowledge to operate.
3. **Portable by default.** `pg_dump` + one environment variable change = migrated to any PostgreSQL host.
4. **Start simple, earn complexity.** Cron before orchestration frameworks. Log files before monitoring dashboards.

## System Overview

```
08:31 ET daily (cron)
        |
        v
  +-----------+     +-------------+     +-----------+
  |  CHECK    |---->|  INGEST     |---->|  ANALYZE  |
  |           |     |             |     |           |
  | StatsCan  |     | Pull data   |     | Claude    |
  | changed?  |     | Upsert DB   |     | Sonnet    |
  | Filter    |     | Detect      |     | Generate  |
  | watchlist |     | revisions   |     | releases  |
  +-----------+     +-------------+     +-----------+
                                              |
                    +-------------+     +-----------+
                    |  PUBLISH    |<----|  CHARTS   |
                    |             |     |           |
                    | Hugo build  |     | matplotlib|
                    | Newsletter  |     | PNG files |
                    | Deploy      |     |           |
                    +-------------+     +-----------+
```

## Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Database | PostgreSQL (Supabase or Docker) | Standard SQL, portable, free managed tier |
| Ingestion | Python + httpx | Simple HTTP client with rate limiting and retries |
| AI Analysis | Claude API (Sonnet) | Strong analytical writing, cost-effective |
| Website | Hugo (static site) | Single binary, no dependencies, markdown-native |
| Charts | matplotlib | Static PNGs that work in email, PDF, and HTML |
| Newsletter | Resend API | Programmatic, no platform lock-in |
| CI | GitHub Actions | Free for public repos, runs tests + Hugo build |
| Hosting | Cloudflare Pages | Free, auto-deploys on push |

## Database Schema

```
DATA LAYER                          CONTENT LAYER
+----------+                        +--------+
| sources  |                        | topics |
+----+-----+                        +----+---+
     |                                   |
+----v--------+                    +-----v------+
| data_tables |                    |  releases  |
+----+--------+                    +-----+------+
     |                                   |
+----v---+     +-----------+     +-------v--------+
| series +---->| data_points|    | release_series |
+----+---+     +-----------+    +----------------+
     |
+----v------+                   +------------------+
| revisions |                   | newsletter_sends |
+-----------+                   +------------------+
```

**Key design decisions:**

- **`series` + `data_points`**: Handles any dimensionality through `dimension_labels` JSONB. StatsCan tables vary from 1 to 10 dimensions — JSONB gives 90% of the value for 10% of the effort.
- **`revisions` table**: When StatsCan revises a previously published value, both the old and new values are recorded with a timestamp. This is a genuine differentiator.
- **Content layer separation**: Releases are stored independently from data, linked via `release_series`. This allows regenerating releases without re-ingesting data.

## Pipeline Flow

### Daily Run (`pipeline/run_daily.py`)

1. **CHECK** — `getChangedCubeList(today)` from StatsCan, filter against watchlist
2. **INGEST** — For each changed table: pull latest 3 periods, upsert data points, detect revisions
3. **ANALYZE** — For tables with prompt templates: build data context, call Claude, store release
4. **CHARTS** — Regenerate CPI trend and unemployment rate charts
5. **PUBLISH** — Generate Hugo content, build site, send newsletter digest
6. **LOG** — Emit structured JSON health summary, alert on total failure

### Error Handling

- **HTTP 409** (data lock window): detected and reported, no retry
- **HTTP 429** (rate limiting): exponential backoff, 3 retries
- **Transient failures**: exponential backoff on timeouts and connection errors
- **Partial failures**: one table failing doesn't block others
- **Claude API**: retries on rate limits and overload (HTTP 529)
- **Database**: stale connection detection and transparent reconnect

### Frequency-Aware Gating

The pipeline won't generate releases for stale data:
- Monthly tables: skip if latest data > 62 days old
- Quarterly tables: skip if > 120 days old
- Annual tables: skip if > 400 days old

## Data Flow: StatsCan API

```
getChangedCubeList(date)          # What changed today?
    |
    v (filter against WATCHLIST)
getDataFromVectorsAndLatestNPeriods(vectors, n=3)  # Pull latest data
    |
    v (parse vectorDataPoint[])
compare against stored data_points
    |
    +-- new? --> INSERT into data_points
    +-- changed? --> INSERT into revisions, UPDATE data_points
    +-- same? --> skip
```

**API base URL:** `https://www150.statcan.gc.ca/t1/wds/rest`

Discovery endpoints use GET (date in URL path). Data endpoints use POST (JSON body). All responses wrapped in `{"status": "SUCCESS", "object": {...}}`.

## Three Possible Futures

The architecture keeps these open without foreclosing any:

1. **Government modernization tool** — hand to a provincial econ/stats team to augment their workflow
2. **Independent commercial data product** — licence to executives and analysts across Atlantic Canada
3. **Open-source platform** — release ingestion/analysis/publishing tools; proprietary value in curated data feeds
