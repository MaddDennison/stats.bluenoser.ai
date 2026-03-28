"""Data ingestion and revision detection for Stats Bluenoser.

Pulls data from Statistics Canada's WDS API (via statcan_client),
parses the response, writes to PostgreSQL, and detects revisions
when StatsCan updates previously published values.

This module is the bridge between the API client and the database.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from pipeline import db
from pipeline.config import WATCHLIST
from pipeline.statcan_client import StatCanClient

logger = logging.getLogger(__name__)

# StatsCan status codes that indicate suppressed/unavailable data
SUPPRESSED_STATUS_CODES = {4, 5, 6, 7, 9, 10, 11, 12, 13, 14}


@dataclass
class IngestionStats:
    """Tracks results of an ingestion run."""

    vectors_processed: int = 0
    points_inserted: int = 0
    points_updated: int = 0
    revisions_detected: int = 0
    errors: int = 0

    def __str__(self):
        return (
            f"vectors={self.vectors_processed}, "
            f"inserted={self.points_inserted}, "
            f"updated={self.points_updated}, "
            f"revisions={self.revisions_detected}, "
            f"errors={self.errors}"
        )


# -- Table and series management ---------------------------------------------


def ensure_source_exists() -> int:
    """Ensure Statistics Canada source record exists. Returns source_id."""
    row = db.execute_one(
        "SELECT source_id FROM sources WHERE api_type = 'statcan_wds'"
    )
    if row:
        return row["source_id"]
    raise RuntimeError("Statistics Canada source not found — run seed_sources.sql")


def ensure_table_exists(source_pid: str) -> int:
    """Upsert a data_tables record from the watchlist config. Returns table_id."""
    config = WATCHLIST.get(source_pid)
    if not config:
        raise ValueError(f"Table {source_pid} not in WATCHLIST config")

    source_id = ensure_source_exists()

    row = db.execute_one(
        "SELECT table_id FROM data_tables WHERE source_id = %s AND source_pid = %s",
        (source_id, source_pid),
    )
    if row:
        return row["table_id"]

    row = db.execute_one(
        """INSERT INTO data_tables (source_id, source_pid, title, frequency)
           VALUES (%s, %s, %s, %s)
           RETURNING table_id""",
        (source_id, source_pid, config["title"], config["frequency"]),
    )
    logger.info(f"Created data_tables record for {source_pid}: table_id={row['table_id']}")
    return row["table_id"]


def ensure_series_exists(
    table_id: int,
    vector_id: str,
    description: str | None = None,
    geo_name: str | None = None,
    coordinate: str | None = None,
    unit_of_measure: str | None = None,
    scalar_factor: int = 0,
) -> int:
    """Upsert a series record. Returns series_id."""
    row = db.execute_one(
        "SELECT series_id FROM series WHERE table_id = %s AND vector_id = %s",
        (table_id, vector_id),
    )
    if row:
        return row["series_id"]

    row = db.execute_one(
        """INSERT INTO series (table_id, vector_id, description, geo_name,
                               coordinate, unit_of_measure, scalar_factor)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           RETURNING series_id""",
        (table_id, vector_id, description, geo_name, coordinate, unit_of_measure, scalar_factor),
    )
    logger.info(f"Created series record for vector {vector_id}: series_id={row['series_id']}")
    return row["series_id"]


# -- Data point ingestion with revision detection ----------------------------


def ingest_data_points(series_id: int, data_points: list[dict], stats: IngestionStats):
    """Upsert data points for a series, detecting revisions.

    For each data point:
    1. Check if a value already exists for this series + ref_period
    2. If no existing value: INSERT
    3. If existing value differs: record revision, then UPDATE
    4. If existing value matches: skip (no-op)

    Args:
        series_id: The series to ingest into.
        data_points: List of parsed data point dicts with keys:
            ref_period, value, status_code, symbol_code, decimal_precision, release_date
        stats: IngestionStats to accumulate counts.
    """
    for pt in data_points:
        ref_period = pt["ref_period"]
        new_value = pt["value"]
        status_code = pt.get("status_code")
        symbol_code = pt.get("symbol_code")
        decimal_precision = pt.get("decimal_precision")
        release_date = pt.get("release_date")

        # Check for existing value
        existing = db.execute_one(
            "SELECT value FROM data_points WHERE series_id = %s AND ref_period = %s",
            (series_id, ref_period),
        )

        if existing is None:
            # New data point — INSERT
            db.execute(
                """INSERT INTO data_points
                   (series_id, ref_period, value, status_code, symbol_code,
                    decimal_precision, release_date)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (series_id, ref_period, new_value, status_code, symbol_code,
                 decimal_precision, release_date),
            )
            stats.points_inserted += 1
        else:
            old_value = existing["value"]
            # Compare values (handle None/NULL)
            if _values_differ(old_value, new_value):
                # Revision detected — record it
                db.execute(
                    """INSERT INTO revisions
                       (series_id, ref_period, previous_value, new_value)
                       VALUES (%s, %s, %s, %s)""",
                    (series_id, ref_period, old_value, new_value),
                )
                # Update the data point
                db.execute(
                    """UPDATE data_points
                       SET value = %s, status_code = %s, symbol_code = %s,
                           decimal_precision = %s, release_date = %s, ingested_at = NOW()
                       WHERE series_id = %s AND ref_period = %s""",
                    (new_value, status_code, symbol_code, decimal_precision,
                     release_date, series_id, ref_period),
                )
                stats.points_updated += 1
                stats.revisions_detected += 1
                logger.info(
                    f"Revision detected: series={series_id} period={ref_period} "
                    f"{old_value} → {new_value}"
                )


def _values_differ(old: Decimal | None, new: Decimal | None) -> bool:
    """Compare two values, treating None == None as equal."""
    if old is None and new is None:
        return False
    if old is None or new is None:
        return True
    return Decimal(str(old)) != Decimal(str(new))


# -- WDS response parsing ---------------------------------------------------


def parse_vector_response(vector_data: dict) -> list[dict]:
    """Parse a single vector's WDS response into a list of data point dicts.

    The WDS returns (after unwrapping):
    {
        "vectorId": 41690973,
        "productId": 18100004,
        "coordinate": "2.2.0.0.0.0.0.0.0.0",
        "vectorDataPoint": [
            {"refPer": "2026-02-01", "value": 165.9, "decimals": 1,
             "statusCode": 0, "symbolCode": 0, "releaseTime": "2026-03-16T08:30"},
            ...
        ]
    }
    """
    points = []
    for pt in vector_data.get("vectorDataPoint", []):
        ref_per_str = pt.get("refPer") or pt.get("refPerRaw")
        if not ref_per_str:
            continue

        # Parse reference period — handles multiple formats:
        # Monthly: "2026-02-01", Quarterly: "2026-01-01" (Q1 start), Annual: "2026-01-01"
        # Some responses may omit the day: "2026-02"
        try:
            if len(ref_per_str) == 7:  # "YYYY-MM"
                ref_period = date.fromisoformat(ref_per_str + "-01")
            elif len(ref_per_str) == 4:  # "YYYY"
                ref_period = date.fromisoformat(ref_per_str + "-01-01")
            else:
                ref_period = date.fromisoformat(ref_per_str)
        except ValueError:
            logger.warning(f"Unparseable reference period: {ref_per_str!r} — skipping")
            continue

        # Parse value — None if suppressed
        status_code = pt.get("statusCode", 0)
        value = None
        if status_code not in SUPPRESSED_STATUS_CODES:
            raw_value = pt.get("value")
            if raw_value is not None:
                value = Decimal(str(raw_value))

        # Parse release time
        release_time = pt.get("releaseTime")
        release_date = None
        if release_time:
            try:
                release_date = datetime.fromisoformat(release_time)
            except (ValueError, TypeError):
                pass

        points.append({
            "ref_period": ref_period,
            "value": value,
            "status_code": str(status_code) if status_code else None,
            "symbol_code": str(pt.get("symbolCode", "")) or None,
            "decimal_precision": pt.get("decimals"),
            "release_date": release_date,
        })

    return points


# -- High-level ingestion methods -------------------------------------------


def ingest_from_vectors(
    client: StatCanClient,
    source_pid: str,
    vector_map: dict[str, int],
    n_periods: int = 12,
) -> IngestionStats:
    """Pull latest N periods for vectors and ingest into the database.

    This is the primary daily ingestion method.

    Args:
        client: StatCanClient instance.
        source_pid: Table PID (e.g. "18100004").
        vector_map: Dict of {description: vector_id} from config.
        n_periods: Number of recent periods to pull.

    Returns:
        IngestionStats with counts.
    """
    stats = IngestionStats()
    table_id = ensure_table_exists(source_pid)
    vector_ids = list(vector_map.values())

    logger.info(f"Ingesting {len(vector_ids)} vectors for table {source_pid}, latest {n_periods} periods")

    # Pull data from API
    results = client.get_data_from_vectors_latest_n(vector_ids, n=n_periods)

    # Build reverse map for descriptions
    id_to_desc = {v: k for k, v in vector_map.items()}

    for vector_data in results:
        vid = vector_data.get("vectorId")
        desc = id_to_desc.get(vid, f"v{vid}")
        coordinate = vector_data.get("coordinate")

        # Parse geo from description (format: "Geography;Product")
        geo_name = desc.split(";")[0] if ";" in desc else None

        # Ensure series exists
        series_id = ensure_series_exists(
            table_id=table_id,
            vector_id=str(vid),
            description=desc,
            geo_name=geo_name,
            coordinate=coordinate,
        )

        # Parse and ingest data points
        data_points = parse_vector_response(vector_data)
        ingest_data_points(series_id, data_points, stats)
        stats.vectors_processed += 1

    logger.info(f"Ingestion complete for {source_pid}: {stats}")
    return stats


def ingest_backfill(
    client: StatCanClient,
    source_pid: str,
    vector_map: dict[str, int],
    n_periods: int = 300,
) -> IngestionStats:
    """Backfill historical data using latestN periods.

    Uses getDataFromVectorsAndLatestNPeriods with a large N value.
    The ref-period-range endpoint (getDataFromVectorByReferencePeriodRange)
    returns 405, so we use latestN instead. N=300 gives ~25 years of
    monthly data.

    Args:
        client: StatCanClient instance.
        source_pid: Table PID.
        vector_map: Dict of {description: vector_id} from config.
        n_periods: Number of historical periods to pull (300 ≈ 25 years monthly).

    Returns:
        IngestionStats with counts.
    """
    stats = IngestionStats()
    table_id = ensure_table_exists(source_pid)
    vector_ids = list(vector_map.values())

    logger.info(
        f"Backfill: {len(vector_ids)} vectors for table {source_pid}, "
        f"latest {n_periods} periods"
    )

    # Chunk vectors to avoid overwhelming the API (max 25 per request)
    chunk_size = 25
    id_to_desc = {v: k for k, v in vector_map.items()}

    for i in range(0, len(vector_ids), chunk_size):
        chunk = vector_ids[i : i + chunk_size]
        logger.info(
            f"  Chunk {i // chunk_size + 1}: vectors {i + 1}–{min(i + chunk_size, len(vector_ids))}"
        )

        results = client.get_data_from_vectors_latest_n(chunk, n=n_periods)

        for vector_data in results:
            vid = vector_data.get("vectorId")
            desc = id_to_desc.get(vid, f"v{vid}")
            coordinate = vector_data.get("coordinate")
            geo_name = desc.split(";")[0] if ";" in desc else None

            series_id = ensure_series_exists(
                table_id=table_id,
                vector_id=str(vid),
                description=desc,
                geo_name=geo_name,
                coordinate=coordinate,
            )

            data_points = parse_vector_response(vector_data)
            ingest_data_points(series_id, data_points, stats)
            stats.vectors_processed += 1

    logger.info(f"Backfill complete for {source_pid}: {stats}")
    return stats
