"""Pipeline configuration — table watchlist, vector mappings, thresholds."""

from __future__ import annotations

# Statistics Canada WDS API
STATCAN_BASE_URL = "https://www150.statcan.gc.ca/t1/wds/rest"

# Rate limiting (requests per second, per IP limit is 25)
STATCAN_RATE_LIMIT = 20

# Data lock window — StatsCan locks tables midnight–08:30 ET
DATA_LOCK_START_HOUR = 0
DATA_LOCK_END_HOUR = 8
DATA_LOCK_END_MINUTE = 30

# Pipeline schedule
PIPELINE_TRIGGER_HOUR = 8
PIPELINE_TRIGGER_MINUTE = 31
PIPELINE_TIMEZONE = "America/Toronto"

# Backfill depth (years of historical data to pull on first load)
BACKFILL_YEARS = 20


# -- Table Watchlist ---------------------------------------------------------
# Each entry maps a StatsCan PID to its topic, display title, and the
# geographic vectors we care about. Vector IDs will be discovered via
# map_vectors.py and filled in here.
#
# Format:
#   "source_pid": {
#       "title": str,
#       "frequency": str,
#       "topic_slug": str,
#       "vectors": {
#           "description": "vector_id"   (filled by map_vectors.py)
#       }
#   }

# Phase 1 — Starter Set (5 tables)
WATCHLIST: dict[str, dict] = {
    "18100004": {
        "title": "Consumer Price Index, monthly, not seasonally adjusted",
        "frequency": "monthly",
        "topic_slug": "consumer-price-index",
        "vectors": {
            # Canada
            "Canada;All-items": 41690973,
            "Canada;All-items excluding food and energy": 41691233,
            "Canada;Food": 41690974,
            "Canada;Shelter": 41691050,
            "Canada;Energy": 41691239,
            # Nova Scotia
            "Nova Scotia;All-items": 41691513,
            "Nova Scotia;All-items excluding food and energy": 41691638,
            "Nova Scotia;Food": 41691514,
            "Nova Scotia;Shelter": 41691546,
            "Nova Scotia;Transportation": 41691581,
            "Nova Scotia;Energy": 41691644,
            "Nova Scotia;Clothing and footwear": 41691573,
            "Nova Scotia;Health and personal care": 41691597,
            # Halifax CMA
            "Halifax;All-items": 41692858,
        },
    },
    "14100287": {
        "title": "Labour force characteristics, monthly, seasonally adjusted",
        "frequency": "monthly",
        "topic_slug": "labour-market-monthly",
        "vectors": {
            # Canada — headline (Total gender, 15+, SA, Estimate)
            "Canada;Population": 2062809,
            "Canada;Labour force": 2062810,
            "Canada;Employment": 2062811,
            "Canada;Full-time employment": 2062812,
            "Canada;Part-time employment": 2062813,
            "Canada;Unemployment": 2062814,
            "Canada;Unemployment rate": 2062815,
            "Canada;Participation rate": 2062816,
            "Canada;Employment rate": 2062817,
            # Nova Scotia — headline
            "Nova Scotia;Population": 2063376,
            "Nova Scotia;Labour force": 2063377,
            "Nova Scotia;Employment": 2063378,
            "Nova Scotia;Full-time employment": 2063379,
            "Nova Scotia;Part-time employment": 2063380,
            "Nova Scotia;Unemployment": 2063381,
            "Nova Scotia;Unemployment rate": 2063382,
            "Nova Scotia;Participation rate": 2063383,
            "Nova Scotia;Employment rate": 2063384,
            # Nova Scotia — age breakdowns (Total gender, SA)
            "Nova Scotia;Employment;15 to 24 years": 2063405,
            "Nova Scotia;Unemployment rate;15 to 24 years": 2063409,
            "Nova Scotia;Participation rate;15 to 24 years": 2063410,
            "Nova Scotia;Employment rate;15 to 24 years": 2063411,
            "Nova Scotia;Employment;25 to 54 years": 2063513,
            "Nova Scotia;Unemployment rate;25 to 54 years": 2063517,
            "Nova Scotia;Participation rate;25 to 54 years": 2063518,
            "Nova Scotia;Employment rate;25 to 54 years": 2063519,
            "Nova Scotia;Employment;55 years and over": 2063540,
            "Nova Scotia;Unemployment rate;55 years and over": 2063544,
            "Nova Scotia;Participation rate;55 years and over": 2063545,
            "Nova Scotia;Employment rate;55 years and over": 2063546,
            # Nova Scotia — gender breakdowns (15+, SA)
            "Nova Scotia;Employment;Men+": 2063387,
            "Nova Scotia;Unemployment rate;Men+": 2063391,
            "Nova Scotia;Participation rate;Men+": 2063392,
            "Nova Scotia;Employment rate;Men+": 2063393,
            "Nova Scotia;Employment;Women+": 2063396,
            "Nova Scotia;Unemployment rate;Women+": 2063400,
            "Nova Scotia;Participation rate;Women+": 2063401,
            "Nova Scotia;Employment rate;Women+": 2063402,
        },
    },
    "36100434": {
        "title": "GDP at basic prices, by industry, monthly",
        "frequency": "monthly",
        "topic_slug": "gdp-economic-accounts",
        "vectors": {
            # National only (no provincial breakdown in this table)
            # Seasonally adjusted at annual rates, Chained (2017) dollars
            "Canada;All industries": 65201210,
            "Canada;Goods-producing industries": 65201211,
            "Canada;Services-producing industries": 65201212,
            "Canada;Construction": 65201247,
            "Canada;Manufacturing": 65201239,
            "Canada;Retail trade": 65201257,
            "Canada;Mining, quarrying, and oil and gas extraction": 65201228,
            "Canada;Finance and insurance": 65201261,
            "Canada;Real estate and rental and leasing": 65201263,
            "Canada;Public administration": 65201282,
            "Canada;Health care and social assistance": 65201276,
            "Canada;Accommodation and food services": 65201271,
        },
    },
    "20100008": {
        "title": "Retail trade sales by province and territory",
        "frequency": "monthly",
        "topic_slug": "retail-wholesale",
        "vectors": {
            # Seasonally adjusted, total retail
            "Canada;Retail trade;Seasonally adjusted": 52367097,
            "Nova Scotia;Retail trade;Seasonally adjusted": 52367454,
            # Unadjusted — total and key subsectors for NS
            "Canada;Retail trade;Unadjusted": 52367096,
            "Nova Scotia;Retail trade;Unadjusted": 52367453,
            "Nova Scotia;Motor vehicle and parts dealers;Unadjusted": 52367465,
            "Nova Scotia;Food and beverage retailers;Unadjusted": 52367487,
            "Nova Scotia;General merchandise and warehouse clubs and superstores;Unadjusted": 52367497,
            "Nova Scotia;Gasoline stations and fuel vendors;Unadjusted": 52367503,
        },
    },
    "34100066": {
        "title": "Building permits, by type of structure and type of work",
        "frequency": "monthly",
        "topic_slug": "construction-housing",
        "vectors": {
            # Total residential + non-residential, value of permits, SA current dollars
            "Canada;Total;Value of permits;SA": 121293395,
            "Nova Scotia;Total;Value of permits;SA": 121357475,
            "Halifax;Total;Value of permits;SA": 121677875,
            # Unadjusted current dollars
            "Canada;Total;Value of permits;Unadjusted": 121293394,
            "Nova Scotia;Total;Value of permits;Unadjusted": 121357474,
            "Halifax;Total;Value of permits;Unadjusted": 121677874,
            # Residential vs non-residential (NS, unadjusted current)
            "Nova Scotia;Residential;Value of permits;Unadjusted": 121357560,
            "Nova Scotia;Non-residential;Value of permits;Unadjusted": 121357690,
        },
    },
}

# Phase 2 — Expansion Set (added after pipeline is stable)
EXPANSION_PIDS = [
    "18100005",  # CPI basket weights
    "14100355",  # Employment by industry, monthly
    "14100288",  # Employment by class of worker
    "14100380",  # Labour force, 3-month moving average, SA
    "14100036",  # Actual hours worked by industry
    "14100063",  # Employee wages by industry
    "14100459",  # Labour force, 3-month moving average, SA
    "36100104",  # GDP, expenditure-based, quarterly
    "12100011",  # International merchandise trade
    "16100048",  # Manufacturing sales by industry
    "17100009",  # Population estimates, quarterly
    "14100011",  # Employment insurance statistics
    "20100074",  # Wholesale trade
    "34100175",  # Capital expenditures
    "34100135",  # Building permits, by activity sector
]

# Anomaly detection threshold (standard deviations from recent trend)
ANOMALY_THRESHOLD_SD = 2.0

# AI release generation
AI_MODEL = "claude-sonnet-4-20250514"
AI_MAX_TOKENS = 4096
