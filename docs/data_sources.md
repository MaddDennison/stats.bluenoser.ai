# Data Sources

## Statistics Canada Web Data Service (WDS)

**Base URL:** `https://www150.statcan.gc.ca/t1/wds/rest`
**Protocol:** RESTful HTTPS, returns JSON
**Authentication:** None required (public API)
**Rate Limits:** 25 requests/second per IP (we throttle to 20)
**Availability:** 24/7; data updates at 08:30 ET daily; tables locked midnight-08:30 ET (HTTP 409)
**Licence:** [Open Government Licence — Canada](https://open.canada.ca/en/open-government-licence-canada)

### API Methods

| Method | HTTP | Path | Purpose |
|--------|------|------|---------|
| getChangedCubeList | GET | `/getChangedCubeList/{date}` | What tables changed since date |
| getChangedSeriesList | GET | `/getChangedSeriesList/{date}` | What vectors changed since date |
| getAllCubesListLite | GET | `/getAllCubesListLite` | List all available tables |
| getCodeSets | GET | `/getCodeSets` | Status/symbol/frequency code definitions |
| getCubeMetadata | POST | `/getCubeMetadata` | Table structure, dimensions, members |
| getSeriesInfoFromVector | POST | `/getSeriesInfoFromVector` | Vector metadata lookup |
| getDataFromVectorsAndLatestNPeriods | POST | `/getDataFromVectorsAndLatestNPeriods` | Latest N periods for vectors |
| getBulkVectorDataByRange | POST | `/getBulkVectorDataByRange` | Data by release date range |
| getFullTableDownloadCSV | GET | `/getFullTableDownloadCSV/{pid}/en` | Full table CSV download URL |

### Response Format

All responses wrap data in `{"status": "SUCCESS", "object": {...}}`. List endpoints return arrays of these wrappers. The client's `_unwrap()` method strips this envelope.

### Data Points

Each data point contains:
- `refPer` — Reference period (YYYY-MM-DD)
- `value` — Numeric value (may be 0 for suppressed data)
- `statusCode` — 0 = valid; 4-14 = various suppression reasons
- `symbolCode` — Additional quality indicators
- `decimals` — Decimal precision
- `releaseTime` — When StatsCan published this value

### Known Quirks

- **Data lock window:** Tables return HTTP 409 between midnight and 08:30 ET while StatsCan updates data
- **getDataFromVectorByReferencePeriodRange:** Returns HTTP 405 (not supported). Use `getDataFromVectorsAndLatestNPeriods` with a large N for backfill instead
- **getBulkVectorDataByRange:** Returns HTTP 406. Not usable
- **getFullTableDownloadCSV:** Returns a JSON response with the actual download URL in the `object` field, not the file itself

## Current Tables

### CPI (18-10-0004-01)

Consumer Price Index, monthly, not seasonally adjusted.

| Vector | Description |
|--------|-------------|
| 41690973 | Canada; All-items |
| 41691233 | Canada; All-items excluding food and energy |
| 41690974 | Canada; Food |
| 41691050 | Canada; Shelter |
| 41691239 | Canada; Energy |
| 41691513 | Nova Scotia; All-items |
| 41691638 | Nova Scotia; All-items excluding food and energy |
| 41691514 | Nova Scotia; Food |
| 41691546 | Nova Scotia; Shelter |
| 41691581 | Nova Scotia; Transportation |
| 41691644 | Nova Scotia; Energy |
| 41691573 | Nova Scotia; Clothing and footwear |
| 41691597 | Nova Scotia; Health and personal care |
| 41692858 | Halifax; All-items |

### LFS (14-10-0287-01)

Labour force characteristics, monthly, seasonally adjusted. 38 vectors covering:
- Canada + NS headlines (population, labour force, employment, unemployment rate, participation rate, FT/PT)
- NS age breakdowns (15-24, 25-54, 55+) for employment, unemployment rate, participation rate, employment rate
- NS gender breakdowns (Men+, Women+) for the same indicators

Full vector list in `pipeline/config.py`.

### GDP (36-10-0434-01)

GDP at basic prices, by industry, monthly. National only (no provincial breakdown). 12 vectors covering all industries, goods/services, construction, manufacturing, retail, mining, finance, real estate, public admin, health care, accommodation.

### Retail Trade (20-10-0008-01)

Retail trade sales by province, monthly. 8 vectors: Canada + NS total retail (SA and unadjusted), NS subsectors (motor vehicles, food/beverage, general merchandise, gasoline).

### Building Permits (34-10-0066-01)

Building permits by type, monthly. 8 vectors: Canada, NS, Halifax total value (SA and unadjusted), NS residential and non-residential breakdown.

## Planned Sources (Phase 2+)

### International

| Source | Geography | API | Key Series |
|--------|-----------|-----|------------|
| US Bureau of Labor Statistics | US | REST (JSON) | CPI, Employment |
| US Bureau of Economic Analysis | US | REST (JSON) | GDP, Trade |
| FRED (Federal Reserve) | US | REST (JSON) | Fed Funds Rate, yields |
| Eurostat | EU | REST (SDMX) | GDP, HICP, unemployment |
| UK ONS | UK | REST (JSON) | GDP, CPI, labour market |
| Japan e-Stat | Japan | REST (JSON/XML) | GDP, CPI, labour |

### Canadian Open Data Portals

| Portal | Platform | API | Key Data |
|--------|----------|-----|----------|
| Open Canada | CKAN | REST (JSON) | Federal spending, immigration, contracts |
| NS Open Data | Socrata/SODA | REST (SoQL) | Minimum wage history, provincial programs |
| Halifax Open Data | ArcGIS Hub | REST/GeoJSON | Building permits, transit, development |

### Expansion StatsCan Tables (15 additional)

Configured in `pipeline/config.py` as `EXPANSION_PIDS`. Includes employment by industry, wages, hours worked, quarterly GDP, international trade, manufacturing, population estimates, employment insurance, wholesale trade, capital expenditures, and additional building permits data.
