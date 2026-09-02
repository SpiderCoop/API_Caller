# Guía de Pruebas Unitarias - Econ API Bridge

## Descripción General

Este directorio contiene todas las pruebas unitarias para el paquete `econ_api_bridge`. Las pruebas están organizadas siguiendo la estructura del paquete principal y utilizan las mejores prácticas de testing con pytest.

## Estructura de Directorios

```
tests/
├── __init__.py                 # Inicializador del paquete de tests
├── conftest.py                 # Configuración compartida de pytest y fixtures
├── pytest.ini                  # Configuración de pytest
├── test_baseapi.py            # Tests para BaseAPI
├── banxico/
│   ├── __init__.py
│   └── test_sie.py            # Tests para Banxico SIE API
├── bis/
│   ├── __init__.py
│   └── test_bis.py            # Tests para BIS API
├── fed/
│   ├── __init__.py
│   └── test_fed.py            # Tests para FRED API
├── inegi/
│   ├── __init__.py
│   ├── test_bie.py            # Tests para INEGI BIE API
│   └── test_denue.py          # Tests para INEGI DENUE API
└── wrldbank/
    ├── __init__.py
    └── test_wrldbank.py       # Tests para World Bank API
```

## Configuración

### Dependencias

Las pruebas requieren `pytest`, que ya está configurado como dependencia de desarrollo:

```bash
uv pip install pytest
```

O si está usando poetry/uv con el archivo pyproject.toml:

```bash
uv sync
```

## Ejecución de Pruebas

### Ejecutar todas las pruebas

```bash
pytest
```

### Ejecutar pruebas de un módulo específico

```bash
# Tests de Banxico
pytest tests/banxico/

# Tests de FRED
pytest tests/fed/

# Tests de BaseAPI
pytest tests/test_baseapi.py
```

### Ejecutar solo pruebas unitarias (sin integraciones)

```bash
pytest -m unit
```

### Ejecutar con salida verbosa

```bash
pytest -v
```

### Ejecutar con cobertura (si pytest-cov está instalado)

```bash
pytest --cov=econ_api_bridge --cov-report=html
```

### Ejecutar un test específico

```bash
pytest tests/banxico/test_sie.py::TestBanxicoSIEInitialization::test_initialization_with_api_key -v
```

## Fixtures Disponibles

El archivo `conftest.py` proporciona múltiples fixtures y factories para facilitar el testing:

### Fixtures de Configuración

- `api_key`: API key de prueba
- `base_url_banxico`, `base_url_bis`, etc.: URLs base de cada API

### Factories (Generadores de Datos Mock)

- `banxico_sie_factory`: Crea respuestas mock de Banxico SIE
- `fred_factory`: Crea respuestas mock de FRED
- `bis_factory`: Crea respuestas mock de BIS
- `inegi_bie_factory`: Crea respuestas mock de INEGI BIE
- `inegi_denue_factory`: Crea respuestas mock de INEGI DENUE

### Fixtures de Datos

- `sample_dataframe`: DataFrame de muestra con datos económicos
- `sample_dataframe_with_missing_values`: DataFrame con valores faltantes
- `mock_session`: Sesión requests mockeada
- `mock_response_success`: Respuesta HTTP exitosa mockeada
- `mock_response_error`: Respuesta HTTP de error mockeada

### Fixtures Parametrizadas

- `serie_id_single_or_list`: Serie ID como string o lista
- `date_ranges`: Rangos de fechas variados

## Ejemplo de Test

```python
import pytest
from unittest.mock import patch


class TestBanxicoSIE:
    """Tests para Banxico SIE API."""

    @pytest.mark.unit
    def test_initialization(self, api_key):
        """Test de inicialización."""
        from econ_api_bridge.banxico.sie import Banxico_SIE

        sie = Banxico_SIE(api_key)
        assert sie.base_url == "https://www.banxico.org.mx/SieAPIRest/service/v1"

    @pytest.mark.unit
    @patch("econ_api_bridge.banxico.sie.Banxico_SIE._make_request")
    def test_get_series_metadata(self, mock_request, api_key, banxico_sie_factory):
        """Test de obtención de metadatos."""
        from econ_api_bridge.banxico.sie import Banxico_SIE

        mock_request.return_value = (
            banxico_sie_factory.create_series_metadata_response()
        )

        sie = Banxico_SIE(api_key)
        result = sie.get_series_metadata("SF43718")

        assert "SF43718" in result
```

## Mejores Prácticas Implementadas

1. **Organización**: Tests organizados por módulo, espejando la estructura del paquete
2. **Fixtures**: Uso extensivo de fixtures para reutilización de código
3. **Factories**: Factories para crear datos mock de prueba
4. **Mocking**: Uso de `unittest.mock` para evitar llamadas reales a APIs
5. **Markers**: Marcadores para categorizar tests (unit, integration, etc.)
6. **Nombres descriptivos**: Nombres claros que describen qué se está testando
7. **Docstrings**: Cada test tiene documentación clara
8. **Cobertura**: Tests que cubren casos normales y excepciones

## Patrones de Testing

### Test de Inicialización

```python
@pytest.mark.unit
def test_initialization_with_api_key(self, api_key):
    """Test de inicialización con API key."""
    api = SomeAPI(api_key)
    assert api.base_url == "expected_url"
```

### Test con Mocking de Requests

```python
@pytest.mark.unit
@patch("module.SomeAPI._make_request")
def test_method(self, mock_request, api_key, factory):
    """Test con request mockeado."""
    mock_request.return_value = factory.create_response()

    api = SomeAPI(api_key)
    result = api.method()

    assert isinstance(result, pd.DataFrame)
```

### Test de Manejo de Errores

```python
@pytest.mark.unit
def test_invalid_parameter(self, api_key):
    """Test de parámetro inválido."""
    api = SomeAPI(api_key)
    
    with pytest.raises(ValueError, match="expected error message"):
        api.method(invalid_param=123)
```

## Cobertura de Tests

Cada módulo tiene cobertura completa:

- **BaseAPI**: Inicialización, requests, headers, manejo de errores
- **Banxico SIE**: Parámetros, metadatos, datos, validaciones
- **FRED**: Parámetros, metadatos, datos, múltiples series
- **BIS**: Parámetros, datos, rangos de fechas
- **INEGI BIE**: Manejo de frecuencias, unidades, transformación de periodos, datos
- **INEGI DENUE**: Búsquedas, fichas, búsquedas avanzadas
- **World Bank**: Datos por indicador y país, metadatos

## Añadir Nuevos Tests

Para añadir nuevos tests:

1. Crea un archivo `test_<nombre>.py` en el directorio apropiado
2. Usa fixtures de `conftest.py` cuando sea posible
3. Marca los tests con `@pytest.mark.unit` o `@pytest.mark.integration`
4. Sigue el patrón de nombres: `Test<Clase>` para clases y `test_<método>` para métodos
5. Incluye docstrings descriptivos

## Debugging

Para debuggear un test específico:

```bash
# Con salida detallada
pytest tests/banxico/test_sie.py -vvv

# Con print statements
pytest tests/banxico/test_sie.py -s

# Con debugger de Python
pytest tests/banxico/test_sie.py --pdb
```

## CI/CD

Estos tests están diseñados para integración con CI/CD. Ejecuta:

```bash
pytest --tb=short --junit-xml=test-results.xml
```

## Notas Importantes

- Todos los tests usan `unittest.mock` para evitar llamadas reales a las APIs
- No requieren claves de API reales (se usan valores ficticios)
- Los tests son rápidos y aislados
- Se puede ejecutar toda la suite de tests sin dependencias externas

## Contacto y Contribuciones

Para contribuir nuevos tests o mejorar los existentes, por favor sigue las convenciones establecidas y mantén la cobertura alta.
