# Stats Bluenoser — Task Tracker

**Last updated:** 2026-03-26
**Legend:** `[ ]` not started · `[~]` in progress · `[x]` done

---

## Phase 1, Month 1: The First End-to-End Release

The goal is to generate one CPI release from live StatsCan data that is comparable in quality to a government econ/stats release — end to end, from API call to published webpage.

### Week 1: Prove the API Works

**Infrastructure & Project Setup**

- [x] Initialize git repo and push to GitHub
- [x] Create project directory structure (`db/`, `pipeline/`, `scripts/`, `tests/`, `site/`, `mcp/`, `scrapers/`, `docs/`)
- [x] Write `.gitignore`, `.env.example`, `Makefile`, `Dockerfile`, `docker-compose.yml`
- [x] Write `requirements.txt` with core dependencies
- [x] Write `README.md`
- [x] Set up PostgreSQL database (Supabase project or local Docker)
- [x] Run `schema.sql` — create all 9 tables and 10 indexes
- [x] Run `seed_sources.sql` — insert Statistics Canada as source
- [x] Run `seed_topics.sql` — insert 24-topic taxonomy
- [x] Verify database is accessible and seeded correctly

**StatsCan API Client**

- [x] Write `pipeline/statcan_client.py` with all core WDS methods
  - [x] `get_changed_cube_list` — daily trigger (what changed today?)
  - [x] `get_changed_series_list` — granular change detection
  - [x] `get_all_cubes_list_lite` — catalogue discovery
  - [x] `get_cube_metadata` — table structure, dimensions, members
  - [x] `get_series_info_from_vector` — vector lookup
  - [x] `get_code_sets` — reference code definitions
  - [x] `get_data_from_vectors_latest_n` — primary daily data pull
  - [x] `get_bulk_vector_data_by_range` — pull by release date (revision catching)
  - [x] `get_data_by_ref_period_range` — pull by reference period (backfill)
  - [x] `get_full_table_csv_url` — resolve CSV download URL
  - [x] `download_full_table_csv` — download full table ZIP
  - [x] Rate limiting (`_throttle` at 20 req/sec)
  - [x] Retry logic (3 attempts with exponential backoff)
  - [x] HTTP 409 handling (data lock window detection)
- [x] Write `scripts/test_api.py` — smoke test against live WDS API
- [x] Write `scripts/map_vectors.py` — discover vectors by geography for a table

**API Verification & Vector Discovery**

- [x] Run `test_api.py` — verify StatsCan API is reachable and returning expected data
- [x] Run `map_vectors.py` against CPI table (18-10-0004-01)
  - [x] Identify vector IDs for: NS All-items, Halifax All-items, Canada All-items
  - [x] Identify vector IDs for: NS All-items excl. food & energy, Canada All-items excl. food & energy
  - [x] Identify vector IDs for any other CPI components used in government releases (shelter, transportation, food, energy, etc.)
- [x] Populate CPI vectors in `pipeline/config.py` WATCHLIST
- [x] Verify pulled CPI data by spot-checking 5+ values against the StatsCan website
- [x] Save CPI metadata fixture to `tests/fixtures/metadata_18100004.json`

**Configuration & Models**

- [x] Write `pipeline/config.py` — watchlist with 5 starter tables, rate limits, AI config
- [x] Write `pipeline/models.py` — dataclasses for Source, DataTable, Series, DataPoint, Revision, Release

---

### Week 2: Ingest Real Data

**Database Helper / Connection Layer**

- [x] Write `pipeline/db.py` — database connection management
  - [x] Connection pool using `psycopg2` (reads `DATABASE_URL` from env)
  - [x] Context manager for transactions
  - [x] Helper for parameterized queries (prevent SQL injection)

**Data Ingester**

- [x] Write `pipeline/ingester.py`
  - [x] `ensure_table_exists(source_pid)` — upsert into `data_tables` from config watchlist
  - [x] `ensure_series_exists(table_id, vector_id, metadata)` — upsert into `series` with geo/dimension info
  - [x] `ingest_data_points(series_id, data_points)` — upsert into `data_points`
    - [x] On conflict (same series + ref_period): compare values
    - [x] If value changed: insert into `revisions` table before updating
    - [x] Handle NULL/suppressed values (StatsCan uses status codes for suppression)
  - [x] `ingest_from_vectors(vector_ids, n_periods)` — pull latest N periods via client, parse WDS response, write to DB
  - [x] `ingest_backfill(vector_ids, n_periods)` — backfill method (uses latestN; ref-period-range endpoint returns 405)
  - [x] Parse StatsCan WDS JSON response format:
    - [x] Extract `refPer` (reference period) → convert to `DATE`
    - [x] Extract `value` → `NUMERIC`
    - [x] Extract `statusCode`, `symbolCode`, `decimals`, `releaseDate`
    - [x] Handle the nested `object` → `vectorDataPoint` response structure
  - [x] Log ingestion stats: rows inserted, rows updated, revisions detected

**Backfill Script**

- [x] Write `scripts/backfill.py`
  - [x] Accept table PID as argument (or "all" for entire watchlist)
  - [x] Calculate date range from `config.BACKFILL_YEARS` (20 years)
  - [x] Batch vector requests to avoid hitting rate limits (chunk by 10–25 vectors)
  - [x] Progress reporting (table X of Y, Z data points ingested)
  - [x] Idempotent — safe to re-run without duplicating data
- [x] Run backfill for CPI table (18-10-0004-01) — ~20 years of monthly data
- [x] Verify ingested CPI data: spot-check at least 10 data points across different periods against StatsCan website
- [x] Verify revision detection works: manually modify a stored value, re-ingest, confirm revision record is created

**Data Quality Checks**

- [x] Write `scripts/verify_data.py` — spot-check script
  - [x] For a given table + vector, pull latest value from DB and from StatsCan API
  - [x] Compare and report discrepancies
  - [x] Run across all ingested vectors as a data quality gate

---

### Week 3: Generate Your First AI Release

**Prompt Templates**

- [x] Create `pipeline/templates/cpi_release.md` — CPI release prompt template
  - [x] Match the structure of government econ/stats CPI releases
  - [x] Section 1: Year-over-year comparison (NS rate, change from prior month's YoY, national average, Halifax rate)
  - [x] Section 2: Month-over-month comparison (same structure)
  - [x] Define placeholders for structured data injection (`{data_json}`, `{ref_period}`, etc.)
  - [x] Include rules: exact numbers, no rounding, "increased"/"decreased"/"unchanged", percentage point changes
  - [x] Include AI-generated labeling requirement
  - [x] Include source attribution (StatsCan table PID)

**Analysis Engine**

- [x] Write `pipeline/analyzer.py`
  - [x] `build_cpi_context(ref_period)` — query DB for current month, prior month, year-ago month for all CPI vectors (NS, Halifax, Canada, excl. food+energy)
  - [x] `build_analysis_prompt(template, data_context)` — inject structured data into prompt template
  - [x] `generate_release(prompt)` — call Claude API (Sonnet), return markdown
    - [x] Use `config.AI_MODEL` and `config.AI_MAX_TOKENS`
    - [x] Include system prompt establishing the analytical voice
    - [x] Return structured result: markdown body, title, slug, significance score
  - [x] `calculate_significance_score(data_context)` — simple heuristic:
    - [x] How far is the latest value from the 12-month moving average?
    - [x] Did the direction change (inflation rising → falling)?
    - [x] Is NS diverging from the national trend?
  - [x] `create_release_record(release_data)` — insert into `releases` table
  - [x] Link release to source series via `release_series` table

**Release Generation & Validation**

- [ ] Generate 3–5 sample CPI releases from historical data
  - [ ] Simulate October 2025 release
  - [ ] Simulate November 2025 release
  - [ ] Simulate December 2025 release
  - [ ] Simulate January 2026 release (if data available)
  - [ ] Simulate February 2026 release (if data available)
- [ ] Write `scripts/compare_releases.py` — side-by-side output
  - [ ] Pull actual government release text (if available/archived)
  - [ ] Display AI-generated version alongside for comparison
  - [ ] Highlight key differences: numbers, framing, missing context
- [ ] Iterate on CPI prompt template based on comparison results
- [ ] Verify all numbers in AI-generated releases match source data exactly

---

### Week 4: Close the Loop — Second Table + Daily Pipeline

**Labour Force Survey — The Hard Test**

- [x] Run `map_vectors.py` against LFS table (14-10-0287-01)
  - [x] Map vectors for: unemployment rate, employment, participation rate, full-time/part-time
  - [x] Map across dimensions: geography (NS, Halifax, Canada), age groups, sex
  - [x] This table has many more dimensions than CPI — validates pipeline flexibility
- [x] Populate LFS vectors in `pipeline/config.py` WATCHLIST
- [x] Backfill LFS data (20 years)
- [ ] Create `pipeline/templates/labour_release.md` — Labour Market Trends prompt template
  - [ ] This is the flagship release (~3,000 words in the government version)
  - [ ] Sections: headline numbers, age cohorts, gender breakdown, industry sectors, regional comparisons, provincial rankings, CMA-level analysis
  - [ ] Much more complex than CPI — tests the AI's ability to handle multi-dimensional data
- [ ] Generate 2–3 sample Labour Market releases from historical data
- [ ] Compare against government Labour Market Trends releases
- [ ] Iterate on labour prompt template

**Daily Pipeline Script**

- [x] Write `pipeline/run_daily.py` — single entry point for the full pipeline
  - [x] Step 1 — CHECK: call `getChangedCubeList(today)`, filter against watchlist
  - [x] Step 2 — INGEST: for each changed table, pull updated vectors, upsert data, log revisions
  - [x] Step 3 — ANALYZE: for each updated table, build context, generate release, store draft
  - [x] Step 4 — LOG: write pipeline run summary (tables checked, data points ingested, releases generated, errors)
  - [x] Handle "no updates today" gracefully (log and exit)
  - [x] Handle partial failures (one table fails, others continue)
  - [x] Structured logging with timestamps
  - [x] Exit codes: 0 = success, 1 = partial failure, 2 = total failure
- [ ] Set up cron job to run at 08:31 ET daily
  - [ ] On local machine (development) or $5/mo VPS (production)
  - [ ] Cron expression: `31 8 * * 1-5` (8:31 AM ET, weekdays — StatsCan publishes on business days)
  - [ ] Redirect output to log file
- [ ] Run pipeline daily for remaining days of the month
- [ ] Review every generated release manually — build confidence in output quality

**Generic Release Template**

- [ ] Create `pipeline/templates/generic_release.md` — fallback template
  - [ ] Works for any table type without a custom template
  - [ ] Summarizes: what changed, by how much, NS vs. national, direction of trend
  - [ ] Less detailed than custom templates but functional for expansion tables

---

## Phase 1, Month 2: Publish

### Week 5: Website

**Hugo Site Setup**

- [ ] Install Hugo and initialize site in `site/` directory
- [ ] Choose a clean, professional theme (gov.uk-style, not startup)
- [ ] Configure `site/config.toml` — site title, base URL, menu structure
- [ ] Create layout templates:
  - [ ] `layouts/_default/baseof.html` — base template with AI disclaimer in footer
  - [ ] `layouts/index.html` — homepage: latest releases + key indicators summary
  - [ ] `layouts/releases/single.html` — individual release page with embedded charts
  - [ ] `layouts/releases/list.html` — release archive, filterable by topic
  - [ ] `layouts/topics/single.html` — topic-filtered view
  - [ ] `layouts/partials/release-card.html` — reusable release summary card
  - [ ] `layouts/partials/ai-disclaimer.html` — standard AI-generated content notice
  - [ ] `layouts/_default/single.html` — generic page (about, etc.)
- [ ] Create static pages:
  - [ ] `site/content/about.md` — what this is, data sources, methodology, AI disclosure
  - [ ] `site/content/newsletter.md` — archive + subscribe link
- [ ] Style with minimal CSS — clean typography, data tables, responsive
- [ ] Every page includes AI-generated content disclaimer

**Publisher Module**

- [ ] Write `pipeline/publisher.py`
  - [ ] `generate_hugo_markdown(release)` — convert release DB record to Hugo content file
    - [ ] Front matter: title, date, topic, geography, draft status, source tables
    - [ ] Body: release markdown content
    - [ ] Write to `site/content/releases/{slug}.md`
  - [ ] `build_site()` — run `hugo build`, handle errors
  - [ ] `deploy_site()` — git commit + push to trigger Cloudflare Pages auto-deploy
- [ ] Integrate publisher into `run_daily.py` as Step 4 (PUBLISH)

**Deployment**

- [ ] Create Cloudflare Pages project connected to GitHub repo
- [ ] Configure build: Hugo build command, publish directory (`site/public/`)
- [ ] Register domain (if ready) or use Cloudflare Pages default URL
- [ ] Verify auto-deploy on push works
- [ ] Publish the backlog of test releases from Month 1

---

### Week 6: Newsletter + Charts

**Newsletter**

- [ ] Set up Resend account (free tier: 100 emails/day)
- [ ] Create `pipeline/templates/daily_digest.html` — email template
  - [ ] Subject line format: "NS Economic Data — {date}"
  - [ ] List today's releases with 1-sentence summary and link
  - [ ] Footer: AI-generated disclaimer, unsubscribe link
- [ ] Add to `publisher.py`:
  - [ ] `compile_daily_digest(releases)` — build email HTML from today's releases
  - [ ] `send_newsletter(digest_html, subject)` — send via Resend API
  - [ ] `log_newsletter_send(subject, recipient_count)` — insert into `newsletter_sends`
- [ ] Subscribe yourself — receive daily digest for one week
- [ ] Verify email rendering in major clients (Gmail, Apple Mail)

**Static Chart Generation**

- [ ] Write `pipeline/chart_generator.py`
  - [ ] `generate_cpi_chart(data)` — line chart: NS CPI vs. national over last 24 months
  - [ ] `generate_labour_chart(data)` — multi-series: unemployment rate by geography
  - [ ] `generate_generic_chart(data, title, series_labels)` — reusable for any indicator
  - [ ] Output: PNG files saved to `site/static/charts/{slug}.png`
  - [ ] Style: clean, minimal, accessible colors, StatsCan-appropriate
  - [ ] Include chart in release markdown as embedded image
- [ ] Add chart generation to analyzer or publisher pipeline step

---

### Weeks 7–8: Expand to All 5 Starter Tables

**GDP by Industry (36-10-0434-01)**

- [ ] Run `map_vectors.py` — discover NS/Canada vectors
- [ ] Populate vectors in config
- [ ] Backfill data
- [ ] Create `pipeline/templates/gdp_release.md` (or test generic template)
- [ ] Generate sample releases and validate

**Retail Trade (20-10-0008-01)**

- [ ] Run `map_vectors.py` — discover NS/Canada vectors
- [ ] Populate vectors in config
- [ ] Backfill data
- [ ] Generate sample releases and validate

**Building Permits (34-10-0066-01)**

- [ ] Run `map_vectors.py` — discover NS/Canada vectors
- [ ] Populate vectors in config
- [ ] Backfill data
- [ ] Generate sample releases and validate

**Pipeline Integration**

- [ ] Verify daily pipeline handles all 5 tables end-to-end
- [ ] Verify website publishes all 5 table types correctly
- [ ] Verify newsletter includes all released tables in daily digest

---

## Phase 1, Month 3: Stability + First Audience

### Weeks 9–10: Expand to 15+ Tables

- [ ] Add expansion set tables one by one (15 tables from `config.EXPANSION_PIDS`):
  - [ ] 18-10-0005-01 — CPI basket weights (annual)
  - [ ] 14-10-0355-01 — Employment by industry
  - [ ] 14-10-0288-01 — Employment by class of worker
  - [ ] 14-10-0380-01 — Labour force, 3-month moving average, SA
  - [ ] 14-10-0036-01 — Actual hours worked by industry
  - [ ] 14-10-0063-01 — Employee wages by industry
  - [ ] 14-10-0459-01 — Labour force, 3-month moving average, SA (CMA)
  - [ ] 36-10-0104-01 — GDP, expenditure-based, quarterly
  - [ ] 12-10-0011-01 — International merchandise trade
  - [ ] 16-10-0048-01 — Manufacturing sales by industry
  - [ ] 17-10-0009-01 — Population estimates, quarterly
  - [ ] 14-10-0011-01 — Employment insurance statistics
  - [ ] 20-10-0074-01 — Wholesale trade
  - [ ] 34-10-0175-01 — Capital expenditures (annual)
  - [ ] 34-10-0135-01 — Building permits, by activity sector
- [ ] For each: discover vectors, populate config, backfill, generate test releases
- [ ] Refine generic release template to handle most table types without a custom template
- [ ] Systematically verify data quality: compare 3+ values per table against StatsCan website
- [ ] Add RSS/Atom feed — configure Hugo to generate (`config.toml` settings)

### Week 11: Hardening

**Error Handling**

- [ ] Handle StatsCan downtime: HTTP 409 during lock window (midnight–08:30 ET)
- [ ] Handle StatsCan API errors: malformed JSON, unexpected response structure
- [ ] Handle rate limiting: back off if receiving HTTP 429
- [ ] Handle database errors: connection drops, constraint violations
- [ ] Handle Claude API errors: rate limits, content filtering, malformed responses
- [ ] Partial failure resilience: one table failing doesn't block others

**Logging & Monitoring**

- [ ] Implement structured logging throughout pipeline (JSON format)
- [ ] Daily health summary: tables checked, data points ingested, releases generated, errors
- [ ] Append health summary to daily newsletter (as footer or separate "pipeline health" section)
- [ ] Log file rotation (don't fill disk)
- [ ] Alert on total pipeline failure (email via Resend to yourself)

**Edge Cases**

- [ ] Tables with no NS-specific data (national only) — handle gracefully in templates
- [ ] Suppressed values (StatsCan uses status codes 'x', 'F', etc.) — don't treat as zero
- [ ] Annual-only tables — don't expect monthly data; adjust release cadence
- [ ] Quarterly tables — release only when new quarter data available
- [ ] Tables that haven't updated in a long time — don't generate stale releases
- [ ] Reference period format variations (some tables use quarters, some use months)

### Week 12: First Audience

- [ ] Invite 5–10 friends/colleagues to the newsletter
- [ ] Collect structured feedback: what's useful, what's wrong, what's missing
- [ ] Fix the most critical issues surfaced by feedback
- [ ] Write contributor documentation (`CONTRIBUTING.md`)
- [ ] Write deployment documentation (`docs/deployment.md`)
- [ ] Review and update `README.md` with current state

---

## Phase 2, Months 4–6: Intelligence Layer

### Month 4: Cross-Indicator Synthesis + Anomaly Detection

**Weekly Synthesis**

- [ ] Create "This Week in the NS Economy" weekly release
  - [ ] Pull data from all topics that updated during the week
  - [ ] Connect CPI movement to employment changes to housing data to trade
  - [ ] Structured prompt that provides multi-indicator context to Claude
  - [ ] Publish every Friday (or Monday for the prior week)

**Anomaly Detection**

- [ ] Implement anomaly detection in `analyzer.py`
  - [ ] Calculate rolling 12-month mean and standard deviation per series
  - [ ] Flag data points >2 SD from the rolling mean (`config.ANOMALY_THRESHOLD_SD`)
  - [ ] Flag direction changes (e.g., inflation rising → falling)
  - [ ] Flag NS divergence from national trend (e.g., NS employment falling while national rises)
- [ ] Store anomaly flags in releases (use `significance_score` field)
- [ ] Alert-based notifications: email when anomaly detected (separate from daily digest)

**Subscriber Personalization**

- [ ] Add subscriber management (database table or Resend lists)
- [ ] Topic-based subscription: let subscribers choose which topics they receive
- [ ] Separate daily digest (all topics) from topic-specific alerts

### Month 5: Indicators Dashboard + Data Explorer

**Key Indicators Dashboard**

- [ ] Create `site/content/indicators.md` with Hugo shortcodes or partial templates
- [ ] Display at-a-glance: NS unemployment rate, CPI (YoY), GDP growth, housing starts, population
- [ ] Show trend arrows (up/down/flat) and comparison to national
- [ ] Updated automatically when new data is ingested

**Data Explorer (v1)**

- [ ] Build simple interactive page (separate from Hugo static site, or Hugo + JS)
  - [ ] Series selector: pick a table → pick a geography → pick a metric
  - [ ] Time-series chart (Chart.js or Recharts)
  - [ ] Data table below chart
  - [ ] CSV download button
- [ ] Provincial comparison view: NS vs. national vs. selected provinces for any metric

### Month 6: MCP Server + Data API

**MCP Server**

- [ ] Write `mcp/server.py` implementing MCP protocol (Python/FastAPI)
- [ ] Implement tools from Section 5 of the plan:
  - [ ] `get_latest_release` — most recent release for a topic
  - [ ] `query_time_series` — pull data points for a series
  - [ ] `compare_geographies` — compare a metric across geographies
  - [ ] `get_ns_economic_snapshot` — key indicators dashboard as structured data
  - [ ] `search_releases` — search past releases
  - [ ] `get_release_schedule` — what's coming from StatsCan
  - [ ] `get_revision_history` — how a data point was revised over time
  - [ ] `run_custom_analysis` — ad-hoc AI analysis on platform data
  - [ ] `get_anomalies` — recent significant data deviations
- [ ] Test with Claude — verify Claude can answer "What was NS CPI last month?"

**REST Data API**

- [ ] Write `api/` module using FastAPI or PostgREST
- [ ] Endpoints:
  - [ ] `GET /api/v1/series/{vector_id}` — time series data
  - [ ] `GET /api/v1/releases` — list releases (filterable by topic, date)
  - [ ] `GET /api/v1/releases/{slug}` — single release
  - [ ] `GET /api/v1/indicators` — current key indicators
  - [ ] `GET /api/v1/revisions/{vector_id}` — revision history
- [ ] Publish API documentation (OpenAPI/Swagger auto-generated by FastAPI)

---

## Ongoing / Cross-Cutting

**Testing**

- [ ] Write `tests/test_statcan_client.py` — unit tests with mocked HTTP responses
- [ ] Write `tests/test_ingester.py` — test data parsing, upserts, revision detection
- [ ] Write `tests/test_analyzer.py` — test prompt building, data context assembly
- [ ] Create `tests/fixtures/` with sample WDS API responses for offline testing
- [ ] Set up pytest configuration (`pytest.ini` or `pyproject.toml`)

**Documentation**

- [ ] Write `docs/architecture.md` — condensed version of the full plan
- [ ] Write `docs/data_sources.md` — detailed API docs per source
- [ ] Write `docs/deployment.md` — how to deploy the full stack
- [ ] Keep `CONTRIBUTING.md` updated

**DevOps**

- [ ] Set up GitHub Actions for basic CI (run tests on push)
- [ ] Dockerize the full pipeline (verify `docker compose up` runs end-to-end)
- [ ] Environment-based configuration (dev/staging/prod via `.env`)

---

## Future Phases (Not Yet Detailed)

**Phase 3, Months 7–8: International Coverage**
- US sources: BLS, BEA, FRED
- EU: Eurostat SDMX API
- UK: ONS API
- Japan: e-Stat API

**Phase 3, Months 7–8: Open Data Portal Integration**
- Open Canada (CKAN API)
- Nova Scotia Open Data (Socrata/SODA API)
- Halifax Open Data (ArcGIS Hub API)

**Phase 4, Months 9–10: Proprietary Data Layer**
- Government procurement web scraping
- Policy/regulatory change monitoring
- Infrastructure project tracking

**Phase 4, Months 11–12: Multi-User + Commercial**
- User authentication and role-based access
- Usage analytics
- Licence structure for commercial distribution
- Government handoff documentation
