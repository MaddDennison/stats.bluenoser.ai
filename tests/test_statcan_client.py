"""Tests for the StatsCan WDS API client.

Uses mocked HTTP responses — no live API calls needed.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from pipeline.statcan_client import StatCanClient, StatCanError
from tests.fixtures.wds_responses import (
    CHANGED_CUBE_LIST_RESPONSE,
    CUBE_METADATA_RESPONSE,
    SERIES_INFO_RESPONSE,
    VECTORS_LATEST_N_RESPONSE,
)


@pytest.fixture
def client():
    c = StatCanClient(base_url="https://mock.statcan.gc.ca/t1/wds/rest")
    yield c
    c.close()


def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        from httpx import HTTPStatusError, Request, Response

        request = Request("POST", "https://mock.statcan.gc.ca")
        response = Response(status_code, request=request)
        resp.raise_for_status.side_effect = HTTPStatusError(
            "error", request=request, response=response
        )
    return resp


class TestUnwrap:
    def test_unwrap_list(self):
        raw = [{"status": "SUCCESS", "object": {"id": 1}}, {"status": "SUCCESS", "object": {"id": 2}}]
        result = StatCanClient._unwrap(raw)
        assert result == [{"id": 1}, {"id": 2}]

    def test_unwrap_dict(self):
        raw = {"status": "SUCCESS", "object": {"id": 1}}
        result = StatCanClient._unwrap(raw)
        assert result == {"id": 1}

    def test_unwrap_passthrough(self):
        raw = [{"id": 1}]
        result = StatCanClient._unwrap(raw)
        assert result == [{"id": 1}]


class TestGetCubeMetadata:
    @patch.object(StatCanClient, "_request")
    def test_returns_metadata(self, mock_request, client):
        mock_request.return_value = CUBE_METADATA_RESPONSE
        meta = client.get_cube_metadata(18100004)

        assert meta["productId"] == 18100004
        assert "Consumer Price Index" in meta["cubeTitleEn"]
        assert len(meta["dimension"]) == 2
        mock_request.assert_called_once_with(
            "POST", "getCubeMetadata", [{"productId": 18100004}]
        )

    @patch.object(StatCanClient, "_request")
    def test_geography_members(self, mock_request, client):
        mock_request.return_value = CUBE_METADATA_RESPONSE
        meta = client.get_cube_metadata(18100004)

        geo_dim = meta["dimension"][0]
        assert geo_dim["dimensionNameEn"] == "Geography"
        geo_names = [m["memberNameEn"] for m in geo_dim["member"]]
        assert "Canada" in geo_names
        assert "Nova Scotia" in geo_names
        assert "Halifax, Nova Scotia" in geo_names


class TestGetDataFromVectors:
    @patch.object(StatCanClient, "_request")
    def test_returns_data_points(self, mock_request, client):
        mock_request.return_value = VECTORS_LATEST_N_RESPONSE
        result = client.get_data_from_vectors_latest_n([41691513, 41690973], n=3)

        assert len(result) == 2
        # First vector (NS All-items)
        ns = result[0]
        assert ns["vectorId"] == 41691513
        assert len(ns["vectorDataPoint"]) == 3
        assert ns["vectorDataPoint"][0]["value"] == 169.2
        assert ns["vectorDataPoint"][0]["refPer"] == "2026-02-01"

    @patch.object(StatCanClient, "_request")
    def test_request_body_format(self, mock_request, client):
        mock_request.return_value = VECTORS_LATEST_N_RESPONSE
        client.get_data_from_vectors_latest_n([41691513], n=6)

        mock_request.assert_called_once_with(
            "POST",
            "getDataFromVectorsAndLatestNPeriods",
            [{"vectorId": 41691513, "latestN": 6}],
        )


class TestGetChangedCubeList:
    @patch.object(StatCanClient, "_request")
    def test_returns_changed_cubes(self, mock_request, client):
        mock_request.return_value = CHANGED_CUBE_LIST_RESPONSE
        result = client.get_changed_cube_list(date(2026, 3, 16))

        assert len(result) == 3
        pids = [c["productId"] for c in result]
        assert 18100004 in pids
        assert 36100434 in pids

    @patch.object(StatCanClient, "_request")
    def test_uses_get_with_date_in_path(self, mock_request, client):
        mock_request.return_value = CHANGED_CUBE_LIST_RESPONSE
        client.get_changed_cube_list(date(2026, 3, 16))

        mock_request.assert_called_once_with(
            "GET", "getChangedCubeList/2026-03-16"
        )


class TestGetSeriesInfo:
    @patch.object(StatCanClient, "_request")
    def test_returns_series_info(self, mock_request, client):
        mock_request.return_value = SERIES_INFO_RESPONSE
        result = client.get_series_info_from_vector([41691513])

        assert len(result) == 1
        assert result[0]["vectorId"] == 41691513
        assert result[0]["productId"] == 18100004


class TestHTTP409:
    def test_raises_on_lock_window(self, client):
        with patch.object(client._client, "post") as mock_post:
            mock_post.return_value = _mock_response({}, status_code=409)
            mock_post.return_value.status_code = 409

            with pytest.raises(StatCanError, match="Data lock window"):
                client._request("POST", "getCubeMetadata", [])
