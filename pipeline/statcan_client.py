"""Statistics Canada Web Data Service (WDS) API client.

Implements the public REST API documented at:
https://www.statcan.gc.ca/en/developers/wds

Base URL: https://www150.statcan.gc.ca/t1/wds/rest

API conventions (discovered empirically):
  - Discovery/listing endpoints are GET with parameters in the URL path
  - Data retrieval endpoints are POST with JSON body
  - Responses wrap data in {"status": "SUCCESS", "object": {...}}
  - List endpoints return a flat list of these wrapper objects

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

    def _request(
        self, method: str, path: str, body: list | dict | None = None
    ) -> list | dict:
        """Make a request to a WDS endpoint with retry on transient errors.

        Handles:
        - HTTP 409: data lock window (midnight–08:30 ET) — raises immediately
        - HTTP 429: rate limiting — backs off and retries
        - Timeouts / connection errors — retries with exponential backoff
        - Malformed JSON — raises StatCanError
        """
        url = f"{self.base_url}/{path}"

        for attempt in range(3):
            self._throttle()
            try:
                if method == "GET":
                    resp = self._client.get(url, follow_redirects=True)
                else:
                    resp = self._client.post(url, json=body or [])

                if resp.status_code == 409:
                    raise StatCanError(
                        "Data lock window (HTTP 409) — tables locked midnight–08:30 ET"
                    )

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 2)  # 4s, 8s, 16s
                    logger.warning(
                        f"Rate limited (HTTP 429), backing off {wait}s "
                        f"(attempt {attempt + 1}/3)..."
                    )
                    if attempt < 2:
                        time.sleep(wait)
                        continue
                    raise StatCanError("Rate limited after 3 attempts (HTTP 429)")

                resp.raise_for_status()

                try:
                    return resp.json()
                except (ValueError, TypeError) as e:
                    raise StatCanError(
                        f"Malformed JSON response from {path}: {e}"
                    )

            except httpx.HTTPStatusError:
                raise
            except StatCanError:
                raise
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                if attempt < 2:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        f"StatsCan request failed ({e}), retrying in {wait}s..."
                    )
                    time.sleep(wait)
                else:
                    raise StatCanError(
                        f"StatsCan API unreachable after 3 attempts: {e}"
                    )

    @staticmethod
    def _unwrap(response: list | dict) -> list | dict:
        """Unwrap the standard WDS response envelope.

        WDS wraps responses in {"status": "SUCCESS", "object": {...}}.
        For list endpoints, it returns a list of these wrappers.
        """
        if isinstance(response, list):
            return [
                item.get("object", item) if isinstance(item, dict) else item
                for item in response
            ]
        if isinstance(response, dict) and "object" in response:
            return response["object"]
        return response

    # -- Discovery methods (GET) ---------------------------------------------

    def get_changed_cube_list(self, start_date: date) -> list[dict]:
        """Get list of tables that changed on or after start_date.

        This is the daily trigger — call at 08:31 ET to see what updated.
        Returns list of dicts with productId, releaseTime, etc.
        """
        date_str = start_date.strftime("%Y-%m-%d")
        result = self._request("GET", f"getChangedCubeList/{date_str}")
        unwrapped = self._unwrap(result)
        if isinstance(unwrapped, list):
            logger.info(f"Changed cubes since {start_date}: {len(unwrapped)} tables")
        return unwrapped

    def get_changed_series_list(self, start_date: date) -> list[dict]:
        """Get list of series (vectors) that changed on or after start_date."""
        date_str = start_date.strftime("%Y-%m-%d")
        return self._unwrap(
            self._request("GET", f"getChangedSeriesList/{date_str}")
        )

    def get_all_cubes_list_lite(self) -> list[dict]:
        """List all available tables (lightweight metadata)."""
        return self._request("GET", "getAllCubesListLite")

    def get_code_sets(self) -> list | dict:
        """Get reference code definitions (status codes, symbol codes, etc.)."""
        return self._unwrap(self._request("GET", "getCodeSets"))

    # -- Metadata methods (POST) ---------------------------------------------

    def get_cube_metadata(self, product_id: int) -> dict:
        """Get full metadata for a table: dimensions, members, structure.

        Args:
            product_id: Table PID as integer (e.g., 18100004 for CPI).

        Returns:
            Dict with dimension info, member lists, footnotes, etc.
        """
        result = self._request(
            "POST", "getCubeMetadata", [{"productId": product_id}]
        )
        unwrapped = self._unwrap(result)
        # getCubeMetadata returns a list with one element
        if isinstance(unwrapped, list) and len(unwrapped) > 0:
            return unwrapped[0]
        return unwrapped

    def get_series_info_from_vector(self, vector_ids: list[int]) -> list[dict]:
        """Look up series details by vector ID.

        Args:
            vector_ids: List of vector IDs (integers, without 'v' prefix).
        """
        body = [{"vectorId": v} for v in vector_ids]
        return self._unwrap(
            self._request("POST", "getSeriesInfoFromVector", body)
        )

    # -- Data retrieval methods (POST) ---------------------------------------

    def get_data_from_vectors_latest_n(
        self, vector_ids: list[int], n: int = 12
    ) -> list[dict]:
        """Pull latest N periods for specific vectors.

        This is the primary daily data pull method.

        Args:
            vector_ids: Vector IDs (integers).
            n: Number of most recent periods to retrieve.

        Returns:
            List of unwrapped response objects, one per vector, each
            containing a 'vectorDataPoint' list of data points.
        """
        body = [{"vectorId": v, "latestN": n} for v in vector_ids]
        result = self._request(
            "POST", "getDataFromVectorsAndLatestNPeriods", body
        )
        unwrapped = self._unwrap(result)
        logger.info(f"Pulled latest {n} periods for {len(vector_ids)} vectors")
        return unwrapped

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
        return self._unwrap(
            self._request("POST", "getBulkVectorDataByRange", body)
        )

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
        return self._unwrap(
            self._request("POST", "getDataFromVectorByReferencePeriodRange", body)
        )

    # -- Bulk downloads ------------------------------------------------------

    def get_full_table_csv_url(self, product_id: int) -> str:
        """Get the URL for a full table CSV download.

        Does not download the file — returns the redirect URL so the caller
        can stream or save it as needed.
        """
        url = f"{self.base_url}/getFullTableDownloadCSV/{product_id}/en"
        self._throttle()
        resp = self._client.head(url, follow_redirects=True)
        resp.raise_for_status()
        return str(resp.url)

    def download_full_table_csv(self, product_id: int) -> bytes:
        """Download the full CSV ZIP for a table.

        Returns raw ZIP bytes. The caller is responsible for unzipping
        and parsing. Use for initial database seeding.
        """
        url = f"{self.base_url}/getFullTableDownloadCSV/{product_id}/en"
        self._throttle()
        resp = self._client.get(url, follow_redirects=True)
        resp.raise_for_status()
        logger.info(
            f"Downloaded full CSV for table {product_id} ({len(resp.content)} bytes)"
        )
        return resp.content
