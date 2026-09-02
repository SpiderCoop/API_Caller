"""
Unit tests for the INEGI DENUE API client.
Tests establishment search and retrieval functionality.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from econ_api_bridge.inegi.denue import INEGI_DENUE


class TestINEGIDENUEInitialization:
    """Tests for INEGI_DENUE initialization."""

    @pytest.mark.unit
    def test_initialization_with_api_key(self, api_key):
        """Test INEGI_DENUE initialization with API key."""
        denue = INEGI_DENUE(api_key)

        assert denue.base_url == "https://www.inegi.org.mx/app/api/denue/v1/consulta"
        assert denue.timeout == 10
        assert denue.session is not None


class TestBuscar:
    """Tests for INEGI_DENUE.buscar method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_returns_dataframe(self, mock_request, api_key, inegi_denue_factory):
        """Test that buscar returns a DataFrame."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.buscar("restaurantes", 19.4326, -99.1332)

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_with_custom_radio(self, mock_request, api_key, inegi_denue_factory):
        """Test buscar with custom search radius."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.buscar("restaurantes", 19.4326, -99.1332, metros=500)

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_empty_results(self, mock_request, api_key):
        """Test buscar with empty results."""
        mock_request.return_value = None

        denue = INEGI_DENUE(api_key)
        result = denue.buscar("nonexistent", 0, 0)

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_constructs_correct_endpoint(self, mock_request, api_key):
        """Test that buscar constructs the correct endpoint."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.buscar("restaurantes", 19.4326, -99.1332, metros=250)

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "Buscar" in endpoint
        assert "restaurantes" in endpoint
        assert "19.4326" in endpoint
        assert "-99.1332" in endpoint
        assert "250" in endpoint

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_with_varios_condition(
        self, mock_request, api_key, inegi_denue_factory
    ):
        """Test buscar with 'todos' condition."""
        mock_request.return_value = inegi_denue_factory.create_search_response(
            num_results=10
        )

        denue = INEGI_DENUE(api_key)
        result = denue.buscar("todos", 19.4326, -99.1332)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0


class TestFicha:
    """Tests for INEGI_DENUE.ficha method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_ficha_returns_dataframe(self, mock_request, api_key, inegi_denue_factory):
        """Test that ficha returns a DataFrame.

        The DENUE API returns a JSON array (list) with the establishment
        record, so the mock must wrap the factory dict in a list -
        pd.DataFrame cannot be built from a dict of scalar values alone.
        """
        mock_request.return_value = [inegi_denue_factory.create_ficha_response()]

        denue = INEGI_DENUE(api_key)
        result = denue.ficha(1)

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_ficha_with_establishment_id(
        self, mock_request, api_key, inegi_denue_factory
    ):
        """Test ficha with specific establishment ID."""
        mock_request.return_value = [
            inegi_denue_factory.create_ficha_response(id_establecimiento=12345)
        ]

        denue = INEGI_DENUE(api_key)
        result = denue.ficha(12345)

        assert isinstance(result, pd.DataFrame)
        assert result.iloc[0]["id"] == "12345"

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_ficha_constructs_correct_endpoint(self, mock_request, api_key):
        """Test that ficha constructs the correct endpoint."""
        mock_request.return_value = {}

        denue = INEGI_DENUE(api_key)
        denue.ficha(12345)

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "Ficha" in endpoint
        assert "12345" in endpoint

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_ficha_empty_response(self, mock_request, api_key):
        """Test ficha with empty response."""
        mock_request.return_value = None

        denue = INEGI_DENUE(api_key)
        result = denue.ficha(1)

        assert isinstance(result, pd.DataFrame)


class TestBuscarEntidad:
    """Tests for INEGI_DENUE.buscar_entidad method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_entidad_returns_dataframe(
        self, mock_request, api_key, inegi_denue_factory
    ):
        """Test that buscar_entidad returns a DataFrame."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.buscar_entidad("restaurantes", 9)  # 9 = CDMX

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_entidad_with_registro_range(
        self, mock_request, api_key, inegi_denue_factory
    ):
        """Test buscar_entidad with specific registro range."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.buscar_entidad(
            "restaurantes", 9, registro_inicial=0, registro_final=100
        )

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_entidad_constructs_correct_endpoint(self, mock_request, api_key):
        """Test that buscar_entidad constructs the correct endpoint."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.buscar_entidad("restaurantes", 9, registro_inicial=0, registro_final=100)

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "BuscarEntidad" in endpoint
        assert "restaurantes" in endpoint
        assert "9" in endpoint


class TestNombre:
    """Tests for INEGI_DENUE.nombre method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_nombre_returns_dataframe(self, mock_request, api_key, inegi_denue_factory):
        """Test that nombre returns a DataFrame."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.nombre("MARRIOTT")

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_nombre_empty_results(self, mock_request, api_key):
        """Test nombre with empty results."""
        mock_request.return_value = None

        denue = INEGI_DENUE(api_key)
        result = denue.nombre("nonexistent")

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_nombre_constructs_correct_endpoint(self, mock_request, api_key):
        """Test that nombre constructs the correct endpoint."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.nombre(
            "MARRIOTT", entidad_federativa=1, registro_inicial=1, registro_final=10
        )

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "Nombre" in endpoint
        assert "MARRIOTT" in endpoint
        assert "/1/" in endpoint
        assert "10" in endpoint


class TestBuscarAreaActividad:
    """Tests for INEGI_DENUE.buscar_area_actividad method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_area_actividad_returns_dataframe(
        self, mock_request, api_key, inegi_denue_factory
    ):
        """Test that buscar_area_actividad returns a DataFrame."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.buscar_area_actividad(
            entidad_federativa=1, nombre_establecimiento="oxxo"
        )

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_area_actividad_empty_results(self, mock_request, api_key):
        """Test buscar_area_actividad with empty results."""
        mock_request.return_value = None

        denue = INEGI_DENUE(api_key)
        result = denue.buscar_area_actividad()

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_area_actividad_constructs_correct_endpoint(
        self, mock_request, api_key
    ):
        """Test that buscar_area_actividad constructs the correct endpoint with the documented parameter order."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.buscar_area_actividad(
            entidad_federativa=1,
            nombre_establecimiento="oxxo",
            registro_inicial=1,
            registro_final=15,
        )

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "BuscarAreaAct/" in endpoint
        assert "BuscarAreaActEstr" not in endpoint
        assert "oxxo" in endpoint
        assert endpoint.endswith(f"/{api_key}")


class TestBuscarAreaActividadEstrato:
    """Tests for INEGI_DENUE.buscar_area_actividad_estrato method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_area_actividad_estrato_returns_dataframe(
        self, mock_request, api_key, inegi_denue_factory
    ):
        """Test that buscar_area_actividad_estrato returns a DataFrame."""
        mock_request.return_value = inegi_denue_factory.create_search_response()

        denue = INEGI_DENUE(api_key)
        result = denue.buscar_area_actividad_estrato(
            entidad_federativa=1, nombre_establecimiento="oxxo", estrato=1
        )

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_area_actividad_estrato_empty_results(self, mock_request, api_key):
        """Test buscar_area_actividad_estrato with empty results."""
        mock_request.return_value = None

        denue = INEGI_DENUE(api_key)
        result = denue.buscar_area_actividad_estrato()

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_buscar_area_actividad_estrato_constructs_correct_endpoint(
        self, mock_request, api_key
    ):
        """Test that buscar_area_actividad_estrato constructs the correct endpoint including estrato."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.buscar_area_actividad_estrato(
            entidad_federativa=1,
            nombre_establecimiento="oxxo",
            registro_inicial=1,
            registro_final=15,
            estrato=1,
        )

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "BuscarAreaActEstr" in endpoint
        assert "oxxo" in endpoint
        assert endpoint.endswith(f"/1/{api_key}")


class TestCuantificar:
    """Tests for INEGI_DENUE.cuantificar method."""

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_cuantificar_returns_dataframe(self, mock_request, api_key):
        """Test that cuantificar returns a DataFrame."""
        mock_request.return_value = [["111,112", "01001,01005", 42]]

        denue = INEGI_DENUE(api_key)
        result = denue.cuantificar("111,112", "01001,01005")

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_cuantificar_empty_results(self, mock_request, api_key):
        """Test cuantificar with empty results."""
        mock_request.return_value = None

        denue = INEGI_DENUE(api_key)
        result = denue.cuantificar()

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_cuantificar_joins_list_parameters_with_comma(self, mock_request, api_key):
        """Test that cuantificar joins list parameters into comma-separated strings."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.cuantificar(
            actividad_economica=["111", "112"], area_geografica=["01001", "01005"]
        )

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "111,112" in endpoint
        assert "01001,01005" in endpoint

    @pytest.mark.unit
    @patch("econ_api_bridge.inegi.denue.INEGI_DENUE._make_request")
    def test_cuantificar_constructs_correct_endpoint(self, mock_request, api_key):
        """Test that cuantificar constructs the correct endpoint."""
        mock_request.return_value = []

        denue = INEGI_DENUE(api_key)
        denue.cuantificar("111,112", "01001,01005", estrato=0)

        call_args = mock_request.call_args
        endpoint = call_args[1]["endpoint"]

        assert "Cuantificar" in endpoint
        assert endpoint.endswith(f"/0/{api_key}")
