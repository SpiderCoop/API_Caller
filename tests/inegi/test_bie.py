"""
Unit tests for the INEGI BIE API client.
Tests series metadata retrieval and data fetching functionality.
"""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from econ_api_bridge.inegi.bie import INEGI_BIE


class TestINEGIBIEInitialization:
    """Tests for INEGI_BIE initialization."""

    @pytest.mark.unit
    def test_initialization_with_api_key(self, api_key):
        """Test INEGI_BIE initialization with API key."""
        bie = INEGI_BIE(api_key)

        assert (
            bie.base_url
            == "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"
        )
        assert bie.timeout == 10
        assert bie.session is not None


class TestFreqHandler:
    """Tests for INEGI_BIE._freq_handler method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    def test_freq_handler_monthly(self, mock_request, api_key, inegi_bie_factory):
        """Test frequency handler for monthly data."""
        mock_request.return_value = (
            inegi_bie_factory.create_frequency_metadata_response(frequency_id=8)
        )

        bie = INEGI_BIE(api_key)
        result = bie._freq_handler(8)

        assert result == "Mensual"

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    def test_freq_handler_quarterly(self, mock_request, api_key, inegi_bie_factory):
        """Test frequency handler for quarterly data."""
        mock_request.return_value = (
            inegi_bie_factory.create_frequency_metadata_response(frequency_id=6)
        )

        bie = INEGI_BIE(api_key)
        result = bie._freq_handler(6)

        assert result == "Trimestral"


class TestUnitHandler:
    """Tests for INEGI_BIE._unit_handler method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    def test_unit_handler_index(self, mock_request, api_key, inegi_bie_factory):
        """Test unit handler for index units."""
        mock_request.return_value = inegi_bie_factory.create_unit_metadata_response(
            unit_id=1
        )

        bie = INEGI_BIE(api_key)
        result = bie._unit_handler(1)

        assert result == "Índice"

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    def test_unit_handler_percentage(self, mock_request, api_key, inegi_bie_factory):
        """Test unit handler for percentage units."""
        mock_request.return_value = inegi_bie_factory.create_unit_metadata_response(
            unit_id=2
        )

        bie = INEGI_BIE(api_key)
        result = bie._unit_handler(2)

        assert result == "Porcentaje"


class TestTransformTimePeriods:
    """Tests for INEGI_BIE._transform_time_periods method."""

    @pytest.mark.unit
    def test_transform_time_periods_monthly(self, api_key):
        """Test transforming monthly time periods."""
        bie = INEGI_BIE(api_key)
        time_periods = ["2023/01", "2023/02", "2023/03"]

        result = bie._transform_time_periods(time_periods, frequency_id=8)

        assert len(result) == 3
        assert all(isinstance(x, str) for x in result)

    @pytest.mark.unit
    def test_transform_time_periods_quarterly(self, api_key):
        """Test transforming quarterly time periods."""
        bie = INEGI_BIE(api_key)
        time_periods = ["2023/1", "2023/2", "2023/3", "2023/4"]

        result = bie._transform_time_periods(time_periods, frequency_id=6)

        assert result == [
            date(2023, 3, 1),
            date(2023, 6, 1),
            date(2023, 9, 1),
            date(2023, 12, 1),
        ]

    @pytest.mark.unit
    def test_transform_time_periods_unsupported_frequency(self, api_key):
        """Test error handling for unsupported frequency."""
        bie = INEGI_BIE(api_key)
        time_periods = ["2023/01"]

        with pytest.raises(ValueError, match="no soportada"):
            bie._transform_time_periods(time_periods, frequency_id=1)


class TestSetSeriesParams:
    """Tests for INEGI_BIE._set_series_params method."""

    @pytest.mark.unit
    def test_set_series_params_single_serie(self, api_key):
        """Test setting parameters with single series ID."""
        bie = INEGI_BIE(api_key)
        endpoint = bie._set_series_params("736183")

        assert "736183" in endpoint
        assert "/INDICATOR/" in endpoint
        assert "false" in endpoint

    @pytest.mark.unit
    def test_set_series_params_multiple_series(self, api_key):
        """Test setting parameters with multiple series IDs."""
        bie = INEGI_BIE(api_key)
        endpoint = bie._set_series_params(["736183", "628208"])

        assert "736183" in endpoint
        assert "628208" in endpoint

    @pytest.mark.unit
    def test_set_series_params_last_data_true(self, api_key):
        """Test setting parameters with last_data=True."""
        bie = INEGI_BIE(api_key)
        endpoint = bie._set_series_params("736183", last_data=True)

        assert "true" in endpoint

    @pytest.mark.unit
    def test_set_series_params_last_data_false(self, api_key):
        """Test setting parameters with last_data=False."""
        bie = INEGI_BIE(api_key)
        endpoint = bie._set_series_params("736183", last_data=False)

        assert "false" in endpoint

    @pytest.mark.unit
    def test_set_series_params_invalid_serie_id_type(self, api_key):
        """Test error handling for invalid serie_id type."""
        bie = INEGI_BIE(api_key)

        with pytest.raises(ValueError, match="serie_id.*cadena de texto"):
            bie._set_series_params(12345)

    @pytest.mark.unit
    def test_set_series_params_invalid_last_data_type(self, api_key):
        """Test error handling for invalid last_data type."""
        bie = INEGI_BIE(api_key)

        with pytest.raises(ValueError, match="last_data.*booleano"):
            bie._set_series_params("736183", last_data="True")


class TestGetSeriesMetadata:
    """Tests for INEGI_BIE.get_series_metadata method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._freq_handler")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._unit_handler")
    def test_get_series_metadata(
        self, mock_unit, mock_freq, mock_request, api_key, inegi_bie_factory
    ):
        """Test retrieving series metadata."""
        mock_request.return_value = inegi_bie_factory.create_series_metadata_response()
        mock_freq.return_value = "Mensual"
        mock_unit.return_value = "Índice"

        bie = INEGI_BIE(api_key)
        result = bie.get_series_metadata("736183")

        assert isinstance(result, dict)
        assert "736183" in result

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._freq_handler")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._unit_handler")
    def test_get_series_metadata_contains_periodicidad_and_unidad(
        self, mock_unit, mock_freq, mock_request, api_key, inegi_bie_factory
    ):
        """Test that metadata contains periodicidad and unidad fields."""
        mock_request.return_value = inegi_bie_factory.create_series_metadata_response()
        mock_freq.return_value = "Mensual"
        mock_unit.return_value = "Índice"

        bie = INEGI_BIE(api_key)
        result = bie.get_series_metadata("736183")

        assert "periodicidad" in result["736183"]
        assert "unidad" in result["736183"]


class TestGetSeriesData:
    """Tests for INEGI_BIE.get_series_data method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._transform_time_periods")
    def test_get_series_data_returns_dataframe(
        self, mock_transform, mock_request, api_key, inegi_bie_factory
    ):
        """Test that get_series_data returns a DataFrame."""
        mock_request.return_value = inegi_bie_factory.create_series_data_response(
            num_observations=3
        )
        mock_transform.return_value = pd.date_range(
            start="2023-01-01", periods=3, freq="MS"
        )

        bie = INEGI_BIE(api_key)
        result = bie.get_series_data("736183")

        assert isinstance(result, pd.DataFrame)
        assert "736183" in result.columns

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._transform_time_periods")
    def test_get_series_data_has_datetime_index(
        self, mock_transform, mock_request, api_key, inegi_bie_factory
    ):
        """Test that the returned DataFrame has a datetime index."""
        mock_request.return_value = inegi_bie_factory.create_series_data_response(
            num_observations=3
        )
        mock_transform.return_value = pd.date_range(
            start="2023-01-01", periods=3, freq="MS"
        )

        bie = INEGI_BIE(api_key)
        result = bie.get_series_data("736183")

        assert pd.api.types.is_datetime64_any_dtype(result.index)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._transform_time_periods")
    def test_get_series_data_preserves_transformed_period_order(
        self, mock_transform, mock_request, api_key, inegi_bie_factory
    ):
        """Test that the DataFrame index matches the order returned by _transform_time_periods.

        get_series_data does not sort the data itself; it relies on
        _transform_time_periods (tested separately) to provide ordered periods.
        """
        mock_request.return_value = inegi_bie_factory.create_series_data_response(
            num_observations=3
        )
        expected_index = pd.date_range(start="2023-01-01", periods=3, freq="MS")
        mock_transform.return_value = expected_index

        bie = INEGI_BIE(api_key)
        result = bie.get_series_data("736183")

        assert list(result.index) == list(expected_index)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._transform_time_periods")
    def test_get_series_data_with_multiple_series(
        self, mock_transform, mock_request, api_key
    ):
        """Test retrieving multiple series at once."""
        response = {
            "Series": [
                {
                    "INDICADOR": "736183",
                    "FREQ": 8,
                    "OBSERVATIONS": [
                        {"TIME_PERIOD": "2023/01", "OBS_VALUE": 100.0},
                        {"TIME_PERIOD": "2023/02", "OBS_VALUE": 101.0},
                    ],
                },
                {
                    "INDICADOR": "628208",
                    "FREQ": 8,
                    "OBSERVATIONS": [
                        {"TIME_PERIOD": "2023/01", "OBS_VALUE": 200.0},
                        {"TIME_PERIOD": "2023/02", "OBS_VALUE": 201.0},
                    ],
                },
            ]
        }

        mock_request.return_value = response
        mock_transform.return_value = pd.date_range(
            start="2023-01-01", periods=2, freq="MS"
        )

        bie = INEGI_BIE(api_key)
        result = bie.get_series_data(["736183", "628208"])

        assert isinstance(result, pd.DataFrame)
        assert "736183" in result.columns
        assert "628208" in result.columns

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._make_request")
    @patch("econ_api_bridge.inegi.bie.INEGI_BIE._transform_time_periods")
    def test_get_series_data_last_observation(
        self, mock_transform, mock_request, api_key
    ):
        """Test retrieving only last observation."""
        response = {
            "Series": [
                {
                    "INDICADOR": "736183",
                    "FREQ": 8,
                    "OBSERVATIONS": [{"TIME_PERIOD": "2024/01", "OBS_VALUE": 105.0}],
                }
            ]
        }

        mock_request.return_value = response
        mock_transform.return_value = pd.date_range(
            start="2024-01-01", periods=1, freq="MS"
        )

        bie = INEGI_BIE(api_key)
        result = bie.get_series_data("736183", last_data=True)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
