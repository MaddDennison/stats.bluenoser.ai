"""Tests for the data ingester — parsing, upserts, revision detection."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from pipeline.ingester import (
    IngestionStats,
    _values_differ,
    parse_vector_response,
)
from tests.fixtures.wds_responses import (
    VECTORS_LATEST_N_RESPONSE,
    VECTORS_WITH_SUPPRESSED,
)


class TestParseVectorResponse:
    def test_parses_standard_response(self):
        vector_data = VECTORS_LATEST_N_RESPONSE[0]["object"]
        points = parse_vector_response(vector_data)

        assert len(points) == 3
        assert points[0]["ref_period"] == date(2026, 2, 1)
        assert points[0]["value"] == Decimal("169.2")
        assert points[0]["decimal_precision"] == 1
        assert points[0]["status_code"] is None  # statusCode 0 → None (means valid data)

    def test_parses_release_date(self):
        vector_data = VECTORS_LATEST_N_RESPONSE[0]["object"]
        points = parse_vector_response(vector_data)

        assert points[0]["release_date"] == datetime(2026, 3, 16, 8, 30)

    def test_handles_suppressed_values(self):
        vector_data = VECTORS_WITH_SUPPRESSED[0]["object"]
        points = parse_vector_response(vector_data)

        assert len(points) == 1
        assert points[0]["value"] is None  # statusCode 6 → suppressed

    def test_parses_multiple_periods(self):
        vector_data = VECTORS_LATEST_N_RESPONSE[0]["object"]
        points = parse_vector_response(vector_data)

        periods = [p["ref_period"] for p in points]
        assert date(2026, 2, 1) in periods
        assert date(2026, 1, 1) in periods
        assert date(2025, 2, 1) in periods

    def test_empty_data_points(self):
        vector_data = {"vectorId": 1, "vectorDataPoint": []}
        points = parse_vector_response(vector_data)
        assert points == []

    def test_missing_data_points_key(self):
        vector_data = {"vectorId": 1}
        points = parse_vector_response(vector_data)
        assert points == []


class TestValuesDiffer:
    def test_same_values(self):
        assert _values_differ(Decimal("169.2"), Decimal("169.2")) is False

    def test_different_values(self):
        assert _values_differ(Decimal("169.2"), Decimal("170.0")) is True

    def test_none_none(self):
        assert _values_differ(None, None) is False

    def test_none_vs_value(self):
        assert _values_differ(None, Decimal("169.2")) is True

    def test_value_vs_none(self):
        assert _values_differ(Decimal("169.2"), None) is True

    def test_float_precision(self):
        # Ensure we don't get float comparison issues
        assert _values_differ(Decimal("0.1"), Decimal("0.1")) is False
        assert _values_differ(Decimal("0.1"), Decimal("0.10")) is False


class TestIngestionStats:
    def test_str_format(self):
        stats = IngestionStats(
            vectors_processed=5,
            points_inserted=100,
            points_updated=3,
            revisions_detected=2,
            errors=0,
        )
        s = str(stats)
        assert "vectors=5" in s
        assert "inserted=100" in s
        assert "updated=3" in s
        assert "revisions=2" in s

    def test_defaults(self):
        stats = IngestionStats()
        assert stats.vectors_processed == 0
        assert stats.points_inserted == 0
        assert stats.revisions_detected == 0
