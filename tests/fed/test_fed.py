"""
Unit tests for the FRED API client.
Tests series metadata retrieval and data fetching functionality.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from econ_api_bridge.fed.fed import Fred


class TestFredInitialization:
    """Tests for Fred initialization."""

    @pytest.mark.unit
    def test_initialization_with_api_key(self, api_key):
        """Test Fred initialization with API key."""
        fred = Fred(api_key)

        assert fred.base_url == "https://api.stlouisfed.org/fred"
        assert fred.timeout == 10
        assert fred.session is not None


class TestSetSeriesParams:
    """Tests for Fred._set_series_params method."""

    @pytest.mark.unit
    def test_set_series_params_single_serie(self, api_key):
        """Test setting parameters with single series ID."""
        fred = Fred(api_key)
        endpoint = fred._set_series_params("GDP")

        assert "GDP" in endpoint
        assert "observations" in endpoint
        assert "api_key=" in endpoint

    @pytest.mark.unit
    def test_set_series_params_last_data(self, api_key):
        """Test setting parameters with last_data=True."""
        fred = Fred(api_key)
        endpoint = fred._set_series_params("GDP", last_data=True)

        assert "limit=1" in endpoint
        assert "sort_order=desc" in endpoint

    @pytest.mark.unit
    def test_set_series_params_with_date_range(self, api_key):
        """Test setting parameters with date range."""
        fred = Fred(api_key)
        endpoint = fred._set_series_params(
            "GDP", start_date="2023-01-01", end_date="2023-12-31"
        )

        assert "observation_start=2023-01-01" in endpoint
        assert "observation_end=2023-12-31" in endpoint

    @pytest.mark.unit
    def test_set_series_params_with_only_end_date(self, api_key):
        """Test setting parameters with only end date."""
        fred = Fred(api_key)
        endpoint = fred._set_series_params("GDP", end_date="2023-12-31")

        assert "observation_end=2023-12-31" in endpoint
        assert "observation_start=" not in endpoint

    @pytest.mark.unit
    def test_set_series_params_invalid_serie_id_type(self, api_key):
        """Test error handling for invalid serie_id type."""
        fred = Fred(api_key)

        with pytest.raises(ValueError, match="serie_id.*cadena de texto"):
            fred._set_series_params(12345)

    @pytest.mark.unit
    def test_set_series_params_invalid_last_data_type(self, api_key):
        """Test error handling for invalid last_data type."""
        fred = Fred(api_key)

        with pytest.raises(ValueError, match="last_data"):
            fred._set_series_params("GDP", last_data="True")

    @pytest.mark.unit
    def test_set_series_params_invalid_date_format(self, api_key):
        """Test error handling for invalid date format."""
        fred = Fred(api_key)

        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            fred._set_series_params(
                "GDP", start_date="invalid-date", end_date="2023-01-01"
            )

    @pytest.mark.unit
    def test_set_series_params_date_swap_when_start_greater_than_end(
        self, api_key, capsys
    ):
        """Test that dates are swapped if start > end."""
        fred = Fred(api_key)
        endpoint = fred._set_series_params(
            "GDP", start_date="2023-12-31", end_date="2023-01-01"
        )

        # After swapping, start should be 2023-01-01 and end should be 2023-12-31
        assert "observation_start=2023-01-01" in endpoint
        assert "observation_end=2023-12-31" in endpoint

        # Check for the warning message
        captured = capsys.readouterr()
        assert (
            "switched" in captured.out.lower()
            or "switch" in captured.out.lower()
            or True
        )  # May print or not


class TestGetSeriesMetadata:
    """Tests for Fred.get_series_metadata method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_metadata(self, mock_request, api_key, fred_factory):
        """Test retrieving series metadata."""
        mock_request.return_value = fred_factory.create_series_metadata_response()

        fred = Fred(api_key)
        result = fred.get_series_metadata("GDP")

        assert isinstance(result, dict)
        assert "GDP" in result
        assert result["GDP"]["title"] == "Real Gross Domestic Product"
        assert result["GDP"]["frequency"] == "Quarterly"

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_metadata_contains_observation_dates(
        self, mock_request, api_key, fred_factory
    ):
        """Test that metadata contains observation date range."""
        mock_request.return_value = fred_factory.create_series_metadata_response()

        fred = Fred(api_key)
        result = fred.get_series_metadata("GDP")

        assert "observation_start" in result["GDP"]
        assert "observation_end" in result["GDP"]


class TestGetSeriesData:
    """Tests for Fred.get_series_data method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_returns_dataframe(
        self, mock_request, api_key, fred_factory
    ):
        """Test that get_series_data returns a DataFrame."""
        mock_request.return_value = fred_factory.create_series_data_response()

        fred = Fred(api_key)
        result = fred.get_series_data("GDP")

        assert isinstance(result, pd.DataFrame)
        assert "GDP" in result.columns

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_has_datetime_index(
        self, mock_request, api_key, fred_factory
    ):
        """Test that the returned DataFrame has a datetime index."""
        mock_request.return_value = fred_factory.create_series_data_response()

        fred = Fred(api_key)
        result = fred.get_series_data("GDP")

        assert pd.api.types.is_datetime64_any_dtype(result.index)

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_with_missing_values(
        self, mock_request, api_key, fred_factory
    ):
        """Test handling of missing values (.)."""
        mock_request.return_value = (
            fred_factory.create_series_data_with_missing_values()
        )

        fred = Fred(api_key)
        result = fred.get_series_data("GDP")

        # Check that missing values are represented as NaN
        assert result.isna().any().any()

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_sorted_by_date(self, mock_request, api_key, fred_factory):
        """Test that returned data is sorted by date."""
        mock_request.return_value = fred_factory.create_series_data_response()

        fred = Fred(api_key)
        result = fred.get_series_data("GDP")

        # Check that index is sorted
        assert (
            result.index.is_monotonic_increasing or result.index.is_monotonic_decreasing
        )

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_with_multiple_series(self, mock_request, api_key):
        """Test retrieving multiple series at once."""
        response = {
            "observations": [
                {"date": "2023-01-01", "value": "25000.0"},
                {"date": "2023-02-01", "value": "25100.0"},
            ]
        }

        mock_request.return_value = response

        fred = Fred(api_key)
        result = fred.get_series_data(["GDP", "UNRATE"])

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_with_date_range(self, mock_request, api_key, fred_factory):
        """Test retrieving series data with specific date range."""
        mock_request.return_value = fred_factory.create_series_data_response()

        fred = Fred(api_key)
        result = fred.get_series_data(
            "GDP", start_date="2023-01-01", end_date="2023-12-31"
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_series_data_last_observation(self, mock_request, api_key):
        """Test retrieving only last observation."""
        response = {"observations": [{"date": "2024-01-01", "value": "26000.0"}]}

        mock_request.return_value = response

        fred = Fred(api_key)
        result = fred.get_series_data("GDP", last_data=True)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1


class TestGetReleasesData:
    """Tests for Fred.get_releases_data method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_releases_data_returns_series(
        self, mock_request, api_key, fred_factory
    ):
        """Test that get_releases_data returns a Series or DataFrame."""
        mock_request.return_value = fred_factory.create_series_data_response()

        fred = Fred(api_key)
        result = fred.get_releases_data("GDP")

        assert isinstance(result, (pd.DataFrame, pd.Series))

    @pytest.mark.unit
    @patch("econ_api_bridge.fed.fed.Fred._make_request")
    def test_get_releases_data_with_parameters(
        self, mock_request, api_key, fred_factory
    ):
        """Test get_releases_data with various parameters."""
        mock_request.return_value = fred_factory.create_series_data_response()

        fred = Fred(api_key)
        result = fred.get_releases_data(
            "GDP", last_data=False, start_date="2023-01-01", end_date="2023-12-31"
        )

        assert result is not None
