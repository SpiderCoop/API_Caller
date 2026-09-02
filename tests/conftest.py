"""
Pytest configuration and shared fixtures for all tests.
Includes factories and fixtures for mocking API responses.
"""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ============================================================================
# Fixtures for API Keys and Configuration
# ============================================================================


@pytest.fixture
def api_key():
    """Fixture providing a test API key."""
    return "test_api_key_12345"


@pytest.fixture
def base_url_banxico():
    """Fixture providing Banxico base URL."""
    return "https://www.banxico.org.mx/SieAPIRest/service/v1"


@pytest.fixture
def base_url_bis():
    """Fixture providing BIS base URL."""
    return "https://stats.bis.org/api/v2"


@pytest.fixture
def base_url_fed():
    """Fixture providing FRED base URL."""
    return "https://api.stlouisfed.org/fred"


@pytest.fixture
def base_url_inegi_bie():
    """Fixture providing INEGI BIE base URL."""
    return "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml"


@pytest.fixture
def base_url_inegi_denue():
    """Fixture providing INEGI DENUE base URL."""
    return "https://www.inegi.org.mx/app/api/denue/v1/consulta"


@pytest.fixture
def base_url_worldbank():
    """Fixture providing World Bank base URL."""
    return "https://api.worldbank.org/v2"


# ============================================================================
# Mock Data Factories for Banxico SIE API
# ============================================================================


class BanxicoSIEDataFactory:
    """Factory for creating mock Banxico SIE API responses."""

    @staticmethod
    def create_series_metadata_response(serie_id="SF43718"):
        """Create a mock response for series metadata."""
        return {
            "bmx": {
                "series": [
                    {
                        "idSerie": serie_id,
                        "titulo": "Test Series",
                        "periodicidad": "Trimestral",
                        "cifra": 1,
                        "unidad": "Índice de volumen",
                    }
                ]
            }
        }

    @staticmethod
    def create_series_data_response(serie_id="SF43718", num_observations=12):
        """Create a mock response for series data."""
        base_date = datetime(2023, 1, 1)
        datos = []

        for i in range(num_observations):
            date = base_date + timedelta(days=30 * i)
            datos.append({"fecha": date.strftime("%d/%m/%Y"), "dato": str(100.0 + i)})

        return {"bmx": {"series": [{"idSerie": serie_id, "datos": datos}]}}

    @staticmethod
    def create_series_data_with_missing_values(serie_id="SF43718"):
        """Create mock data with N/E (missing) values."""
        return {
            "bmx": {
                "series": [
                    {
                        "idSerie": serie_id,
                        "datos": [
                            {"fecha": "01/01/2023", "dato": "100.0"},
                            {"fecha": "02/01/2023", "dato": "N/E"},
                            {"fecha": "03/01/2023", "dato": "101.5"},
                        ],
                    }
                ]
            }
        }


@pytest.fixture
def banxico_sie_factory():
    """Fixture providing Banxico SIE data factory."""
    return BanxicoSIEDataFactory()


# ============================================================================
# Mock Data Factories for FRED API
# ============================================================================


class FredDataFactory:
    """Factory for creating mock FRED API responses."""

    @staticmethod
    def create_series_metadata_response(serie_id="GDP"):
        """Create a mock response for series metadata."""
        return {
            "seriess": [
                {
                    "id": serie_id,
                    "title": "Real Gross Domestic Product",
                    "frequency": "Quarterly",
                    "observation_start": "1947-01-01",
                    "observation_end": "2023-12-31",
                    "units": "Billions of Chained 2017 Dollars",
                    "seasonal_adjustment": "Seasonally Adjusted Annual Rate",
                    "last_updated": "2024-01-31",
                    "notes": "Test series",
                }
            ]
        }

    @staticmethod
    def create_series_data_response(serie_id="GDP", num_observations=12):
        """Create a mock response for series observations."""
        base_date = datetime(2023, 1, 1)
        observations = []

        for i in range(num_observations):
            date = base_date + timedelta(days=30 * i)
            observations.append(
                {"date": date.strftime("%Y-%m-%d"), "value": str(25000.0 + i * 100)}
            )

        return {"observations": observations}

    @staticmethod
    def create_series_data_with_missing_values(serie_id="GDP"):
        """Create mock data with missing values (.)."""
        return {
            "observations": [
                {"date": "2023-01-01", "value": "25000.0"},
                {"date": "2023-02-01", "value": "."},
                {"date": "2023-03-01", "value": "25100.0"},
            ]
        }


@pytest.fixture
def fred_factory():
    """Fixture providing FRED data factory."""
    return FredDataFactory()


# ============================================================================
# Mock Data Factories for BIS API
# ============================================================================


class BISDataFactory:
    """Factory for creating mock BIS API responses."""

    @staticmethod
    def create_series_data_response(serie_id="Q.US.N.A.LE.XDC.A", num_observations=12):
        """Create a mock response for BIS series data."""
        observations = {}

        for i in range(num_observations):
            observations[str(i)] = [100.0 + i * 0.5]

        return {
            "data": {
                "structure": {
                    "dimensions": {
                        "observation": [
                            {
                                "values": [
                                    {
                                        "id": str(i),
                                        "end": (
                                            datetime(2023, 1, 1)
                                            + timedelta(days=30 * i)
                                        ).strftime("%Y-%m-%d"),
                                    }
                                    for i in range(num_observations)
                                ]
                            }
                        ]
                    }
                },
                "dataSets": [{"series": {"0:0:0:0:0": {"observations": observations}}}],
            }
        }


@pytest.fixture
def bis_factory():
    """Fixture providing BIS data factory."""
    return BISDataFactory()


# ============================================================================
# Mock Data Factories for INEGI BIE API
# ============================================================================


class INEGIBIEDataFactory:
    """Factory for creating mock INEGI BIE API responses."""

    @staticmethod
    def create_series_metadata_response(serie_id="736183"):
        """Create a mock response for INEGI BIE metadata."""
        return {
            "Series": [
                {
                    "INDICADOR": serie_id,
                    "FREQ": 8,  # Monthly
                    "UNIT": 1,  # Index
                }
            ]
        }

    @staticmethod
    def create_frequency_metadata_response(frequency_id=8):
        """Create a mock response for frequency metadata."""
        freq_descriptions = {
            4: "Semestral",
            6: "Trimestral",
            8: "Mensual",
            12: "Diaria",
        }

        return {
            "CODE": [
                {
                    "Id": str(frequency_id),
                    "Description": freq_descriptions.get(frequency_id, "Unknown"),
                }
            ]
        }

    @staticmethod
    def create_unit_metadata_response(unit_id=1):
        """Create a mock response for unit metadata."""
        unit_descriptions = {1: "Índice", 2: "Porcentaje", 3: "Valor"}

        return {
            "CODE": [
                {
                    "Id": str(unit_id),
                    "Description": unit_descriptions.get(unit_id, "Unknown"),
                }
            ]
        }

    @staticmethod
    def create_series_data_response(serie_id="736183", num_observations=12):
        """Create a mock response for INEGI BIE series data."""
        observations = []
        base_date = datetime(2023, 1, 1)

        for i in range(num_observations):
            date = base_date + timedelta(days=30 * i)
            observations.append(
                {"TIME_PERIOD": date.strftime("%Y/%m"), "OBS_VALUE": 100.0 + i * 0.5}
            )

        return {
            "Series": [{"INDICADOR": serie_id, "FREQ": 8, "OBSERVATIONS": observations}]
        }


@pytest.fixture
def inegi_bie_factory():
    """Fixture providing INEGI BIE data factory."""
    return INEGIBIEDataFactory()


# ============================================================================
# Mock Data Factories for INEGI DENUE API
# ============================================================================


class INEGIDENUEDataFactory:
    """Factory for creating mock INEGI DENUE API responses."""

    @staticmethod
    def create_search_response(num_results=5):
        """Create a mock response for DENUE search."""
        results = []

        for i in range(num_results):
            results.append(
                {
                    "id": str(i + 1),
                    "nombre": f"Establecimiento {i + 1}",
                    "latitud": 19.4326 + i * 0.01,
                    "longitud": -99.1332 + i * 0.01,
                    "entidad": "CDMX",
                }
            )

        return results

    @staticmethod
    def create_ficha_response(id_establecimiento=1):
        """Create a mock response for establishment details."""
        return {
            "id": str(id_establecimiento),
            "nombre": "Test Establishment",
            "razon_social": "Test S.A. de C.V.",
            "actividad": "Servicios de alimentos",
            "estatus": "Activo",
        }


@pytest.fixture
def inegi_denue_factory():
    """Fixture providing INEGI DENUE data factory."""
    return INEGIDENUEDataFactory()


# ============================================================================
# DataFrame Fixtures
# ============================================================================


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame with economic data."""
    dates = pd.date_range(start="2023-01-01", periods=12, freq="MS")
    data = {
        "Serie1": [100.0 + i * 0.5 for i in range(12)],
        "Serie2": [200.0 + i * 0.3 for i in range(12)],
    }
    return pd.DataFrame(data, index=dates)


@pytest.fixture
def sample_dataframe_with_missing_values():
    """Create a sample DataFrame with missing values."""
    dates = pd.date_range(start="2023-01-01", periods=12, freq="MS")
    data = {
        "Serie1": [
            100.0,
            pd.NA,
            100.5,
            100.7,
            100.9,
            101.0,
            101.1,
            101.2,
            101.3,
            101.4,
            101.5,
            101.6,
        ],
        "Serie2": [
            200.0,
            200.2,
            200.3,
            pd.NA,
            200.5,
            200.6,
            200.7,
            200.8,
            200.9,
            201.0,
            201.1,
            201.2,
        ],
    }
    return pd.DataFrame(data, index=dates)


# ============================================================================
# Mock Session Fixtures
# ============================================================================


@pytest.fixture
def mock_session():
    """Create a mocked requests.Session object."""
    session = MagicMock()
    return session


@pytest.fixture
def mock_response_success():
    """Create a mock successful response."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"status": "success"}
    response.raise_for_status.return_value = None
    return response


@pytest.fixture
def mock_response_error():
    """Create a mock error response."""
    response = MagicMock()
    response.status_code = 404
    response.raise_for_status.side_effect = Exception("404 Not Found")
    return response


# ============================================================================
# Parametrized Fixtures for Common Test Cases
# ============================================================================


@pytest.fixture(params=["SF43718", ["SF43718", "SF43719"]])
def serie_id_single_or_list(request):
    """Fixture providing both single string and list of series IDs."""
    return request.param


@pytest.fixture(
    params=[
        ("2023-01-01", "2023-12-31"),
        ("2023-01-15", "2023-06-30"),
        ("2023-06-01", "2023-12-15"),
    ]
)
def date_ranges(request):
    """Fixture providing various date ranges."""
    return request.param


# ============================================================================
# Pytest Markers
# ============================================================================


def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test (requires API)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test (no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "parametrize_api: mark test as parametrized for different APIs"
    )
