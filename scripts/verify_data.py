"""Data quality verification for Stats Bluenoser.

For each ingested vector, pulls the latest value from the database and
from the StatsCan API, compares them, and reports discrepancies.

Usage:
    python -m scripts.verify_data               # Verify all ingested series
    python -m scripts.verify_data 18100004      # Verify CPI only
"""

from __future__ import annotations

import logging
import sys
from decimal import Decimal

from pipeline import db
from pipeline.db import close_pool
from pipeline.statcan_client import StatCanClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def verify_all(source_pid: str | None = None):
    """Compare DB values against live API for all ingested series."""
    # Get all series from the database
    if source_pid:
        series_rows = db.execute(
            """SELECT s.series_id, s.vector_id, s.description, dt.source_pid
               FROM series s
               JOIN data_tables dt ON s.table_id = dt.table_id
               WHERE dt.source_pid = %s
               ORDER BY s.series_id""",
            (source_pid,),
        )
    else:
        series_rows = db.execute(
            """SELECT s.series_id, s.vector_id, s.description, dt.source_pid
               FROM series s
               JOIN data_tables dt ON s.table_id = dt.table_id
               ORDER BY dt.source_pid, s.series_id""",
        )

    if not series_rows:
        logger.warning("No series found in database. Run backfill first.")
        return

    logger.info(f"Verifying {len(series_rows)} series against live API...")

    # Get latest DB value for each series
    matches = 0
    mismatches = 0
    errors = 0

    with StatCanClient() as client:
        for row in series_rows:
            series_id = row["series_id"]
            vector_id = row["vector_id"]
            desc = row["description"] or f"v{vector_id}"

            # Latest value from DB
            db_row = db.execute_one(
                """SELECT ref_period, value FROM data_points
                   WHERE series_id = %s ORDER BY ref_period DESC LIMIT 1""",
                (series_id,),
            )
            if not db_row:
                logger.warning(f"  {desc}: no data points in DB")
                continue

            db_period = db_row["ref_period"]
            db_value = db_row["value"]

            # Latest value from API
            try:
                api_result = client.get_data_from_vectors_latest_n(
                    [int(vector_id)], n=1
                )
                if api_result:
                    pts = api_result[0].get("vectorDataPoint", [])
                    if pts:
                        api_value = pts[0].get("value")
                        api_period = pts[0].get("refPer")

                        # Compare
                        if str(db_period) == api_period and (
                            (db_value is None and api_value is None)
                            or (
                                db_value is not None
                                and api_value is not None
                                and Decimal(str(db_value)) == Decimal(str(api_value))
                            )
                        ):
                            matches += 1
                            logger.info(f"  OK  {desc}: {db_period} = {db_value}")
                        else:
                            mismatches += 1
                            logger.warning(
                                f"  MISMATCH {desc}: "
                                f"DB={db_period}/{db_value} vs API={api_period}/{api_value}"
                            )
                    else:
                        errors += 1
                        logger.warning(f"  {desc}: API returned no data points")
            except Exception as e:
                errors += 1
                logger.error(f"  {desc}: API error — {e}")

    logger.info(f"\nVerification complete: {matches} OK, {mismatches} mismatches, {errors} errors")
    close_pool()

    if mismatches > 0:
        sys.exit(1)


def main():
    source_pid = None
    if len(sys.argv) > 1:
        source_pid = sys.argv[1].replace("-", "")
    verify_all(source_pid)


if __name__ == "__main__":
    main()
