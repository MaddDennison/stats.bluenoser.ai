"""Historical data backfill for Stats Bluenoser.

Pulls historical data from Statistics Canada for tables in the watchlist.
Safe to re-run — uses upsert logic, so duplicate runs won't create duplicates.

Usage:
    python -m scripts.backfill                  # Backfill all watchlist tables
    python -m scripts.backfill 18100004         # Backfill CPI only
    python -m scripts.backfill 18100004 5       # Backfill CPI, 5 years only
"""

from __future__ import annotations

import logging
import sys
from datetime import date

from pipeline.config import BACKFILL_YEARS, WATCHLIST
from pipeline.db import close_pool
from pipeline.ingester import ingest_backfill
from pipeline.statcan_client import StatCanClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def backfill_table(
    client: StatCanClient, source_pid: str, years: int = BACKFILL_YEARS
):
    """Backfill a single table."""
    config = WATCHLIST.get(source_pid)
    if not config:
        logger.error(f"Table {source_pid} not in WATCHLIST")
        return

    vectors = config.get("vectors", {})
    if not vectors:
        logger.warning(f"Table {source_pid} has no vectors configured — skipping")
        return

    # Convert years to periods (monthly = 12/year)
    n_periods = years * 12

    logger.info(f"Backfilling {source_pid}: {config['title']}")
    logger.info(f"  Vectors: {len(vectors)}")
    logger.info(f"  Periods: {n_periods} ({years} years)")

    stats = ingest_backfill(client, source_pid, vectors, n_periods=n_periods)
    logger.info(f"  Result: {stats}")
    return stats


def main():
    # Parse args
    target_pid = None
    years = BACKFILL_YEARS
    if len(sys.argv) > 1:
        target_pid = sys.argv[1].replace("-", "")
    if len(sys.argv) > 2:
        years = int(sys.argv[2])

    # Determine which tables to backfill
    if target_pid:
        if target_pid not in WATCHLIST:
            logger.error(f"Table {target_pid} not in WATCHLIST. Available: {list(WATCHLIST.keys())}")
            sys.exit(1)
        tables = [target_pid]
    else:
        # Only backfill tables that have vectors configured
        tables = [pid for pid, cfg in WATCHLIST.items() if cfg.get("vectors")]

    logger.info(f"Backfill: {len(tables)} table(s), {years} years of history")

    with StatCanClient() as client:
        for i, pid in enumerate(tables, 1):
            logger.info(f"\n--- Table {i}/{len(tables)}: {pid} ---")
            try:
                backfill_table(client, pid, years)
            except Exception as e:
                logger.error(f"Failed to backfill {pid}: {e}")
                continue

    close_pool()
    logger.info("Backfill complete.")


if __name__ == "__main__":
    main()
