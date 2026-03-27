"""Statistics Canada Web Data Service (WDS) API client.

Implements the public REST API documented at:
https://www.statcan.gc.ca/en/developers/wds

All methods return parsed JSON. The client handles rate limiting,
retries, and the midnight–08:30 ET data lock window.

This module has no database dependencies — it only talks to the API
and returns Python dicts/lists. The ingester module handles persistence.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime

import httpx

from pipeline.config import STATCAN_BASE_URL, STATCAN_RATE_LIMIT

logger = logging.getLogger(__name__)

# Minimum seconds between requests (derived from rate limit)
_MIN_INTERVAL = 1.0 / STATCAN_RATE_LIMIT


class StatCanError(Exception):
    """Raised when the StatsCan API returns an error or unexpected response."""


class StatCanClient:
    """Client for the Statistics Canada Web Data Service."""

    def __init__(self, base_url: str = STATCAN_BASE_URL, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._last_request_time: float = 0.0
        self._client = httpx.Client(timeout=timeout)

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # -- Rate limiting -------------------------------------------------------

    def _throttle(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < _MIN_INTERVAL:
            time.sleep(_MIN_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    # -- HTTP helpers --------------------------------------------------------

    def _post(self, method: str, body: list | dict | None = None) -> list | dict:
        """POST to a WDS endpoint with retry on transient errors."""
        url = f"{self.base_url}/{method}"
        self._throttle()

        for attempt in range(3):
            try:
                resp = self._client.post(url, json=body or [])
                if resp.status_code == 409:
                    raise StatCanError(
                        f"Data lock window (HTTP 409) — tables locked midnight–08:30 ET"
                    )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError:
                raise
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < 2:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        f"StatsCan request failed ({e}), retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    raise StatCanError(f"StatsCan API unreachable after 3 attempts: {e}")

    def _get(self, path: str) -> httpx.Response:
        """GET request (used for CSV/SDMX downloads)."""
        url = f"{self.base_url}/{path}"
        self._throttle()
        resp = self._client.get(url, follow_redirects=True)
        resp.raise_for_status()
        return resp

    # -- Discovery methods ---------------------------------------------------

    def get_changed_cube_list(self, start_date: date) -> list[dict]:
        """Get list of tables that changed on or after start_date.

        This is the daily trigger — call at 08:31 ET to see what updated.
        Returns list of dicts with productId, releaseTime, etc.
        """
        body = {"startDate": start_date.strftime("%Y-%m-%d")}
        result = self._post("getChangedCubeList", body)
        logger.info(f"Changed cubes since {start_date}: {len(result)} tables")
        return result

    def get_changed_series_list(self, start_date: date) -> list[dict]:
        """Get list of series (vectors) that changed on or after start_date."""
        body = {"startDate": start_date.strftime("%Y-%m-%d")}
        return self._post("getChangedSeriesList", body)

    def get_all_cubes_list_lite(self) -> list[dict]:
        """List all available tables (lightweight metadata)."""
        return self._post("getAllCubesListLite")

    # -- Metadata methods ----------------------------------------------------

    def get_cube_metadata(self, product_id: int) -> dict:
        """Get full metadata for a table: dimensions, members, structure.

        Args:
            product_id: Table PID as integer (e.g., 18100004 for CPI).

        Returns:
            Dict with dimension info, member lists, footnotes, etc.
        """
        result = self._post("getCubeMetadata", [{"productId": product_id}])
        if isinstance(result, list) and len(result) > 0:
            obj = result[0].get("object", result[0])
            return obj
        return result

    def get_series_info_from_vector(self, vector_ids: list[int]) -> list[dict]:
        """Look up series details by vector ID.

        Args:
            vector_ids: List of vector IDs (integers, without 'v' prefix).
        """
        body = [{"vectorId": v} for v in vector_ids]
        return self._post("getSeriesInfoFromVector", body)

    def get_code_sets(self) -> dict:
        """Get reference code definitions (status codes, symbol codes, etc.)."""
        return self._post("getCodeSets")

    # -- Data retrieval methods ----------------------------------------------

    def get_data_from_vectors_latest_n(
        self, vector_ids: list[int], n: int = 12
    ) -> list[dict]:
        """Pull latest N periods for specific vectors.

        This is the primary daily data pull method.

        Args:
            vector_ids: Vector IDs (integers).
            n: Number of most recent periods to retrieve.

        Returns:
            List of response objects, one per vector, each containing
            a 'vectorDataPoint' list of data points.
        """
        body = [{"vectorId": v, "latestN": n} for v in vector_ids]
        result = self._post("getDataFromVectorsAndLatestNPeriods", body)
        logger.info(f"Pulled latest {n} periods for {len(vector_ids)} vectors")
        return result

    def get_bulk_vector_data_by_range(
        self,
        vector_ids: list[int],
        start_release_date: datetime,
        end_release_date: datetime,
    ) -> list[dict]:
        """Pull data by release date range (when StatsCan published it).

        Useful for catching revisions — pulls all data points released
        within the date window, including revised values.

        Args:
            vector_ids: Vector IDs (integers).
            start_release_date: Start of release date window.
            end_release_date: End of release date window.
        """
        body = [
            {
                "vectorId": v,
                "startDataPointReleaseDate": start_release_date.strftime(
                    "%Y-%m-%dT%H:%M"
                ),
                "endDataPointReleaseDate": end_release_date.strftime(
                    "%Y-%m-%dT%H:%M"
                ),
            }
            for v in vector_ids
        ]
        return self._post("getBulkVectorDataByRange", body)

    def get_data_by_ref_period_range(
        self, vector_ids: list[int], start_period: date, end_period: date
    ) -> list[dict]:
        """Pull data by reference period range (the time the data describes).

        Best for historical backfill — "give me CPI from Jan 2005 to Dec 2025."

        Args:
            vector_ids: Vector IDs (integers).
            start_period: First reference period (inclusive).
            end_period: Last reference period (inclusive).
        """
        body = [
            {
                "vectorId": v,
                "startRefPeriod": start_period.strftime("%Y-%m-%d"),
                "endRefPeriod": end_period.strftime("%Y-%m-%d"),
            }
            for v in vector_ids
        ]
        return self._post("getDataFromVectorByReferencePeriodRange", body)

    # -- Bulk downloads ------------------------------------------------------

    def get_full_table_csv_url(self, product_id: int) -> str:
        """Get the URL for a full table CSV download.

        Does not download the file — returns the redirect URL so the caller
        can stream or save it as needed.
        """
        path = f"getFullTableDownloadCSV/{product_id}/en"
        url = f"{self.base_url}/{path}"
        # HEAD request to resolve redirect without downloading
        self._throttle()
        resp = self._client.head(url, follow_redirects=True)
        resp.raise_for_status()
        return str(resp.url)

    def download_full_table_csv(self, product_id: int) -> bytes:
        """Download the full CSV ZIP for a table.

        Returns raw ZIP bytes. The caller is responsible for unzipping
        and parsing. Use for initial database seeding.
        """
        resp = self._get(f"getFullTableDownloadCSV/{product_id}/en")
        logger.info(
            f"Downloaded full CSV for table {product_id} ({len(resp.content)} bytes)"
        )
        return resp.content
