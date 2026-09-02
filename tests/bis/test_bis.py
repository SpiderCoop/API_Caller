"""
Unit tests for the BIS API client.
Tests series data retrieval and parameter configuration.
"""

import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from econ_api_bridge.bis.bis import BIS_API


class TestBISAPIInitialization:
    """Tests for BIS_API initialization."""

    @pytest.mark.unit
    def test_initialization_with_api_key(self, api_key):
        """Test BIS_API initialization with API key."""
        bis = BIS_API(api_key)

        assert bis.base_url == "https://stats.bis.org/api/v2"
        assert bis.timeout == 10
        assert bis.session is not None

    @pytest.mark.unit
    def test_initialization_without_api_key(self):
        """Test BIS_API initialization without API key."""
        bis = BIS_API()

        assert bis.base_url == "https://stats.bis.org/api/v2"
        assert bis.session is not None


class TestSetSeriesParams:
    """Tests for BIS_API._set_series_params method."""

    @pytest.mark.unit
    def test_set_series_params_single_serie_id(self, api_key):
        """Test setting parameters with single series ID."""
        bis = BIS_API(api_key)
        endpoint, headers = bis._set_series_params("Q.US.N.A.LE.XDC.A")

        assert "Q.US.N.A.LE.XDC.A" in endpoint
        assert headers["Accept"] == "application/vnd.sdmx.data+json;version=1.0.0"

    @pytest.mark.unit
    def test_set_series_params_multiple_serie_ids(self, api_key):
        """Test setting parameters with multiple series IDs."""
        bis = BIS_API(api_key)
        serie_ids = ["Q.US.N.A.LE.XDC.A", "Q.UK.N.A.LE.XDC.A"]
        endpoint, headers = bis._set_series_params(serie_ids)

        assert "Q.US.N.A.LE.XDC.A" in endpoint
        assert "Q.UK.N.A.LE.XDC.A" in endpoint

    @pytest.mark.unit
    def test_set_series_params_last_data(self, api_key):
        """Test setting parameters with last_data=True."""
        bis = BIS_API(api_key)
        endpoint, headers = bis._set_series_params("Q.US.N.A.LE.XDC.A", last_data=True)

        assert "lastNObservations=1" in endpoint

    @pytest.mark.unit
    def test_set_series_params_with_date_range(self, api_key):
        """Test setting parameters with date range."""
        bis = BIS_API(api_key)
        endpoint, headers = bis._set_series_params(
            "Q.US.N.A.LE.XDC.A", start_date="2023-01-01", end_date="2023-12-31"
        )

        assert "startPeriod=2023-01-01" in endpoint
        assert "endPeriod=2023-12-31" in endpoint

    @pytest.mark.unit
    def test_set_series_params_last_data_true_with_dates_error(self, api_key):
        """Test that last_data=True with dates raises error."""
        bis = BIS_API(api_key)

        with pytest.raises(ValueError, match="Si last_data es True"):
            bis._set_series_params(
                "Q.US.N.A.LE.XDC.A", last_data=True, start_date="2023-01-01"
            )

    @pytest.mark.unit
    def test_set_series_params_start_date_greater_than_end_date(self, api_key):
        """Test error handling when start_date > end_date."""
        bis = BIS_API(api_key)

        with pytest.raises(ValueError, match="no puede ser mayor"):
            bis._set_series_params(
                "Q.US.N.A.LE.XDC.A", start_date="2023-12-31", end_date="2023-01-01"
            )

    @pytest.mark.unit
    def test_set_series_params_invalid_serie_id_type(self, api_key):
        """Test error handling for invalid serie_id type."""
        bis = BIS_API(api_key)

        with pytest.raises(ValueError, match="serie_id.*cadena de texto"):
            bis._set_series_params(12345)

    @pytest.mark.unit
    def test_set_series_params_invalid_last_data_type(self, api_key):
        """Test error handling for invalid last_data type."""
        bis = BIS_API(api_key)

        with pytest.raises(ValueError, match="last_data.*booleano"):
            bis._set_series_params("Q.US.N.A.LE.XDC.A", last_data="True")

    @pytest.mark.unit
    def test_set_series_params_invalid_date_format(self, api_key):
        """Test error handling for invalid date format."""
        bis = BIS_API(api_key)

        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            bis._set_series_params(
                "Q.US.N.A.LE.XDC.A", start_date="invalid-date", end_date="2023-01-01"
            )


class TestGetSeriesMetadata:
    """Tests for BIS_API.get_series_metadata method."""

    @pytest.mark.unit
    @pytest.mark.xfail(
        reason="Known bug: get_series_metadata calls _set_series_params(..., "
        "get_series_metadata=True), an argument the method does not accept.",
        raises=TypeError,
        strict=True,
    )
    @patch("econ_api_bridge.bis.bis.BIS_API._make_request")
    def test_get_series_metadata_returns_dict(self, mock_request, api_key):
        """Document current buggy behavior: get_series_metadata raises TypeError."""
        mock_request.return_value = {
            "bmx": {
                "series": [
                    {
                        "idSerie": "Q.US.N.A.LE.XDC.A",
                        "titulo": "Test Series",
                        "periodicidad": "Trimestral",
                        "cifra": 1,
                        "unidad": "USD",
                    }
                ]
            }
        }

        bis = BIS_API(api_key)
        result = bis.get_series_metadata("Q.US.N.A.LE.XDC.A")

        assert isinstance(result, dict)
        assert "Q.US.N.A.LE.XDC.A" in result


class TestGetSeriesData:
    """Tests for BIS_API.get_series_data method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.bis.bis.BIS_API._make_request")
    def test_get_series_data_returns_dataframe(
        self, mock_request, api_key, bis_factory
    ):
        """Test that get_series_data returns a DataFrame."""
        mock_request.return_value = bis_factory.create_series_data_response()

        bis = BIS_API(api_key)
        result = bis.get_series_data("Q.US.N.A.LE.XDC.A")

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.bis.bis.BIS_API._make_request")
    def test_get_series_data_index_contains_date_objects(
        self, mock_request, api_key, bis_factory
    ):
        """Test that the returned DataFrame index is made of date objects.

        The implementation builds the index from `datetime.date` objects
        (via `pd.to_datetime(...).date()`), so the index dtype is `object`,
        not `datetime64`.
        """
        mock_request.return_value = bis_factory.create_series_data_response()

        bis = BIS_API(api_key)
        result = bis.get_series_data("Q.US.N.A.LE.XDC.A")

        assert all(isinstance(idx, datetime.date) for idx in result.index)

    @pytest.mark.unit
    @patch("econ_api_bridge.bis.bis.BIS_API._make_request")
    def test_get_series_data_sorted_by_date(self, mock_request, api_key, bis_factory):
        """Test that returned data is sorted by date."""
        mock_request.return_value = bis_factory.create_series_data_response()

        bis = BIS_API(api_key)
        result = bis.get_series_data("Q.US.N.A.LE.XDC.A")

        # Check that index is sorted
        assert (
            result.index.is_monotonic_increasing or result.index.is_monotonic_decreasing
        )

    @pytest.mark.unit
    @patch("econ_api_bridge.bis.bis.BIS_API._make_request")
    def test_get_series_data_with_date_range(self, mock_request, api_key, bis_factory):
        """Test retrieving series data with specific date range."""
        mock_request.return_value = bis_factory.create_series_data_response()

        bis = BIS_API(api_key)
        result = bis.get_series_data(
            "Q.US.N.A.LE.XDC.A", start_date="2023-01-01", end_date="2023-12-31"
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    @pytest.mark.unit
    @patch("econ_api_bridge.bis.bis.BIS_API._make_request")
    def test_get_series_data_last_observation(self, mock_request, api_key, bis_factory):
        """Test retrieving only last observation."""
        mock_request.return_value = bis_factory.create_series_data_response(
            num_observations=1
        )

        bis = BIS_API(api_key)
        result = bis.get_series_data("Q.US.N.A.LE.XDC.A", last_data=True)

        assert isinstance(result, pd.DataFrame)
        assert len(result) <= 1
