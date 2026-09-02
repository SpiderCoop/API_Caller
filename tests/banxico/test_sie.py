"""
Unit tests for the Banxico SIE API client.
Tests series metadata retrieval and data fetching functionality.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from econ_api_bridge.banxico.sie import Banxico_SIE


class TestBanxicoSIEInitialization:
    """Tests for Banxico_SIE initialization."""

    @pytest.mark.unit
    def test_initialization_with_api_key(self, api_key):
        """Test Banxico_SIE initialization with API key."""
        sie = Banxico_SIE(api_key)

        assert sie.base_url == "https://www.banxico.org.mx/SieAPIRest/service/v1"
        assert sie.timeout == 10
        assert sie.session is not None

    @pytest.mark.unit
    def test_initialization_inherits_from_baseapi(self, api_key):
        """Test that Banxico_SIE properly inherits from BaseAPI."""
        sie = Banxico_SIE(api_key)

        assert hasattr(sie, "_make_request")
        assert hasattr(sie, "session")
        assert hasattr(sie, "timeout")


class TestSetSeriesParams:
    """Tests for Banxico_SIE._set_series_params method."""

    @pytest.mark.unit
    def test_set_series_params_single_serie_id(self, api_key):
        """Test setting parameters with single series ID."""
        sie = Banxico_SIE(api_key)
        endpoint, headers = sie._set_series_params("SF43718", get_series_metadata=True)

        assert "/series/SF43718" in endpoint
        assert headers["Bmx-Token"] == api_key

    @pytest.mark.unit
    def test_set_series_params_multiple_serie_ids(self, api_key):
        """Test setting parameters with multiple series IDs."""
        sie = Banxico_SIE(api_key)
        serie_ids = ["SF43718", "SF43719", "SF43720"]
        endpoint, headers = sie._set_series_params(serie_ids, get_series_metadata=True)

        assert "SF43718" in endpoint
        assert "SF43719" in endpoint
        assert "SF43720" in endpoint

    @pytest.mark.unit
    def test_set_series_params_last_data(self, api_key):
        """Test setting parameters with last_data=True."""
        sie = Banxico_SIE(api_key)
        endpoint, headers = sie._set_series_params("SF43718", last_data=True)

        assert "oportuno" in endpoint

    @pytest.mark.unit
    def test_set_series_params_with_date_range(self, api_key):
        """Test setting parameters with date range."""
        sie = Banxico_SIE(api_key)
        endpoint, headers = sie._set_series_params(
            "SF43718", start_date="2023-01-01", end_date="2023-12-31"
        )

        assert "2023-01-01" in endpoint
        assert "2023-12-31" in endpoint
        assert "/datos/" in endpoint

    @pytest.mark.unit
    def test_set_series_params_with_percentage_change(self, api_key):
        """Test setting parameters with percentage change option."""
        sie = Banxico_SIE(api_key)
        endpoint, headers = sie._set_series_params(
            "SF43718", percentage_change="PorcAnual"
        )

        assert "incremento=PorcAnual" in endpoint

    @pytest.mark.unit
    def test_set_series_params_with_no_decimals(self, api_key):
        """Test setting parameters with no_decimals option."""
        sie = Banxico_SIE(api_key)
        endpoint, headers = sie._set_series_params("SF43718", no_decimals=True)

        assert "decimales=sinCeros" in endpoint

    @pytest.mark.unit
    def test_set_series_params_invalid_serie_id_type(self, api_key):
        """Test error handling for invalid serie_id type."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="serie_id.*cadena de texto"):
            sie._set_series_params(12345)

    @pytest.mark.unit
    def test_set_series_params_invalid_last_data_type(self, api_key):
        """Test error handling for invalid last_data type."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="last_data.*booleano"):
            sie._set_series_params("SF43718", last_data="True")

    @pytest.mark.unit
    def test_set_series_params_invalid_percentage_change_value(self, api_key):
        """Test error handling for invalid percentage_change value."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="percentage_change"):
            sie._set_series_params("SF43718", percentage_change="InvalidOption")

    @pytest.mark.unit
    def test_set_series_params_invalid_no_decimals_type(self, api_key):
        """Test error handling for invalid no_decimals type."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="no_decimals.*booleano"):
            sie._set_series_params("SF43718", no_decimals="True")

    @pytest.mark.unit
    def test_set_series_params_last_data_true_with_dates_error(self, api_key):
        """Test that last_data=True with dates raises error."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="Si last_data es True"):
            sie._set_series_params("SF43718", last_data=True, start_date="2023-01-01")

    @pytest.mark.unit
    def test_set_series_params_start_date_greater_than_end_date(self, api_key):
        """Test error handling when start_date > end_date."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="no puede ser mayor"):
            sie._set_series_params(
                "SF43718", start_date="2023-12-31", end_date="2023-01-01"
            )

    @pytest.mark.unit
    def test_set_series_params_invalid_date_format(self, api_key):
        """Test error handling for invalid date format."""
        sie = Banxico_SIE(api_key)

        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            sie._set_series_params(
                "SF43718", start_date="invalid-date", end_date="2023-01-01"
            )


class TestGetSeriesMetadata:
    """Tests for Banxico_SIE.get_series_metadata method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    def test_get_series_metadata_single_serie(
        self, mock_request, api_key, banxico_sie_factory
    ):
        """Test retrieving metadata for a single series."""
        mock_request.return_value = (
            banxico_sie_factory.create_series_metadata_response()
        )

        sie = Banxico_SIE(api_key)
        result = sie.get_series_metadata("SF43718")

        assert isinstance(result, dict)
        assert "SF43718" in result
        assert result["SF43718"]["titulo"] == "Test Series"
        assert result["SF43718"]["periodicidad"] == "Trimestral"

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    def test_get_series_metadata_multiple_series(self, mock_request, api_key):
        """Test retrieving metadata for multiple series."""
        mock_request.return_value = {
            "bmx": {
                "series": [
                    {
                        "idSerie": "SF43718",
                        "titulo": "Series 1",
                        "periodicidad": "Trimestral",
                        "cifra": 1,
                        "unidad": "Índice",
                    },
                    {
                        "idSerie": "SF43719",
                        "titulo": "Series 2",
                        "periodicidad": "Mensual",
                        "cifra": 1,
                        "unidad": "Millones",
                    },
                ]
            }
        }

        sie = Banxico_SIE(api_key)
        result = sie.get_series_metadata(["SF43718", "SF43719"])

        assert len(result) == 2
        assert "SF43718" in result
        assert "SF43719" in result


class TestGetSeriesData:
    """Tests for Banxico_SIE.get_series_data method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE.get_series_metadata")
    def test_get_series_data_returns_dataframe(
        self, mock_metadata, mock_request, api_key, banxico_sie_factory
    ):
        """Test that get_series_data returns a DataFrame."""
        mock_request.return_value = banxico_sie_factory.create_series_data_response()
        mock_metadata.return_value = {"SF43718": {"periodicidad": "Mensual"}}

        sie = Banxico_SIE(api_key)
        result = sie.get_series_data("SF43718")

        assert isinstance(result, pd.DataFrame)
        assert "SF43718" in result.columns

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE.get_series_metadata")
    def test_get_series_data_has_datetime_index(
        self, mock_metadata, mock_request, api_key, banxico_sie_factory
    ):
        """Test that the returned DataFrame has a datetime index."""
        mock_request.return_value = banxico_sie_factory.create_series_data_response()
        mock_metadata.return_value = {"SF43718": {"periodicidad": "Mensual"}}

        sie = Banxico_SIE(api_key)
        result = sie.get_series_data("SF43718")

        assert pd.api.types.is_datetime64_any_dtype(result.index)

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE.get_series_metadata")
    def test_get_series_data_with_missing_values(
        self, mock_metadata, mock_request, api_key, banxico_sie_factory
    ):
        """Test handling of missing values (N/E)."""
        mock_request.return_value = (
            banxico_sie_factory.create_series_data_with_missing_values()
        )
        mock_metadata.return_value = {"SF43718": {"periodicidad": "Mensual"}}

        sie = Banxico_SIE(api_key)
        result = sie.get_series_data("SF43718")

        # Check that missing values are represented as NaN
        assert result.isna().any().any()

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE.get_series_metadata")
    def test_get_series_data_quarterly_adjustment(
        self, mock_metadata, mock_request, api_key
    ):
        """Test quarterly data adjustment."""
        response = {
            "bmx": {
                "series": [
                    {
                        "idSerie": "SF43718",
                        "datos": [
                            {"fecha": "01/01/2023", "dato": "100.0"},
                            {"fecha": "01/04/2023", "dato": "101.0"},
                        ],
                    }
                ]
            }
        }

        mock_request.return_value = response
        mock_metadata.return_value = {"SF43718": {"periodicidad": "Trimestral"}}

        sie = Banxico_SIE(api_key)
        result = sie.get_series_data(
            "SF43718", start_date="2023-01-01", end_date="2023-12-31"
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE.get_series_metadata")
    def test_get_series_data_sorted_by_date(
        self, mock_metadata, mock_request, api_key, banxico_sie_factory
    ):
        """Test that returned data is sorted by date."""
        mock_request.return_value = banxico_sie_factory.create_series_data_response()
        mock_metadata.return_value = {"SF43718": {"periodicidad": "Mensual"}}

        sie = Banxico_SIE(api_key)
        result = sie.get_series_data("SF43718")

        # Check that index is sorted
        assert (
            result.index.is_monotonic_increasing or result.index.is_monotonic_decreasing
        )
