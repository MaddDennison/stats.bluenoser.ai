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
            # To be populated by map_vectors.py against CPI table
            # Expected: NS All-items, Halifax All-items, Canada All-items,
            #           NS All-items excl food+energy, Canada All-items excl food+energy
        },
    },
    "14100287": {
        "title": "Labour force characteristics, monthly, seasonally adjusted",
        "frequency": "monthly",
        "topic_slug": "labour-market-monthly",
        "vectors": {},
    },
    "36100434": {
        "title": "GDP at basic prices, by industry, monthly",
        "frequency": "monthly",
        "topic_slug": "gdp-economic-accounts",
        "vectors": {},
    },
    "20100008": {
        "title": "Retail trade sales by province and territory",
        "frequency": "monthly",
        "topic_slug": "retail-wholesale",
        "vectors": {},
    },
    "34100066": {
        "title": "Building permits, by type of structure and type of work",
        "frequency": "monthly",
        "topic_slug": "construction-housing",
        "vectors": {},
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
