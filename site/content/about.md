---
title: "About Stats Bluenoser"
---

## What is this?

Stats Bluenoser is an automated economic intelligence platform for Nova Scotia. It connects directly to Statistics Canada's Web Data Service API, ingests economic data the moment it's released, and generates analytical summaries using AI.

## How it works

1. **Data ingestion** — Every business day at 08:31 ET, the pipeline checks Statistics Canada for new data releases. When updates are detected for tables we track, the latest data is pulled and stored.

2. **Revision tracking** — When Statistics Canada revises previously published data (which happens regularly), we detect and record both the old and new values. This is a feature that most government publications don't offer.

3. **AI analysis** — Structured data is fed to Claude (Anthropic's AI) with carefully designed prompts that produce factual, neutral statistical summaries in the style of government econ/stats teams.

4. **Publishing** — Releases are published to this website and distributed via email newsletter.

## Data sources

All data comes from Statistics Canada's public Web Data Service, used under the [Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada). Current coverage:

- **Consumer Price Index** (Table 18-10-0004-01) — Monthly CPI for Nova Scotia, Halifax, and Canada
- **Labour Force Survey** (Table 14-10-0287-01) — Employment, unemployment, participation rates with age and gender breakdowns

More tables are being added progressively: GDP, retail trade, building permits, manufacturing, trade, and population data.

## AI transparency

Every release on this site is AI-generated and clearly labeled as such. The AI does not speculate, editorialize, or fabricate data. All numbers come directly from Statistics Canada.

AI-generated analysis is not a substitute for expert economic interpretation. For official Nova Scotia economic statistics, consult the provincial government's Economics and Statistics Division.

## Open data

This platform is built on open government data. Statistics Canada's Open Government Licence permits reproduction, redistribution, and commercial use with attribution.
