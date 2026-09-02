# econ-api-bridge

`econ-api-bridge` es una pequeña librería para facilitar la conexión con distintas APIs económicas (Banxico, BIS, FRED, INEGI, World Bank, etc.) y obtener series de tiempo y metadatos en estructuras tipo `pandas.DataFrame` para análisis y procesamiento.

## Contenido rápido
- **Instalación**: crear un entorno e instalar dependencias.
- **Uso**: ejemplos prácticos listos en `examples/`.
- **Variables de entorno**: guardar tokens/API keys en un `.env`.
- **Pruebas**: ejecutar `pytest`.

## Requisitos
- Python >= 3.12
- Dependencias principales (ver `pyproject.toml`): `pandas`, `python-dotenv`, `requests`.

Instala las dependencias usando `uv` (recomendado para reproducibilidad):

```bash
# Crear y activar un entorno virtual (opcional si prefieres usar uno)
python -m venv .venv

# Instalar la herramienta `uv`
pip install --upgrade pip
pip install uv

# (Opcional) Generar un archivo de lock reproducible
uv lock

# Instalar las dependencias declaradas en pyproject
uv sync --locked   # usa el lock generado; si no generaste lock, usa `uv sync`

# Ejecutar comandos dentro del entorno gestionado por uv
# Por ejemplo, ejecutar un script de ejemplo o pytest:
uv run python examples/example_banxico.py
uv run pytest -q

# Si prefieres instalar el paquete en editable mode dentro del entorno
uv run python -m pip install -e .
```

## Quick start

1. Crea un entorno y activa como se muestra arriba.
2. Si la API que vas a usar requiere token (Banxico, FRED, INEGI...), crea un archivo `.env` en la raíz con las variables necesarias. Ejemplo:

```
BANXICO_TOKEN=tu_token_aqui
FRED_TOKEN=tu_token_aqui
INEGI_TOKEN=tu_token_aqui
```

3. Ejecuta uno de los scripts de ejemplo en `examples/` para ver cómo funciona. Por ejemplo:

```bash
python examples/example_banxico.py
```

### Ejemplo mínimo (Banxico)

```python
import os
from dotenv import load_dotenv
from econ_api_bridge.banxico import Banxico_SIE

load_dotenv()
token = os.environ.get("BANXICO_TOKEN")
api = Banxico_SIE(token)
series = ["SR17622", "SF61745"]
df = api.get_series_data(series, start_date="2023-01-01")
print(df.head())
```

Otros adaptadores tienen interfaces similares: `BIS_API`, `Fred`, `INEGI_BIE`, `wrldbank.WrldBank`.

## Estructura del repo

- `econ_api_bridge/`: código fuente de los adaptadores y utilidades.
- `examples/`: scripts de ejemplo (Banxico, BIS, FRED, INEGI).
- `tests/`: pruebas unitarias con `pytest`.

## Ejecutar pruebas

Instala `pytest` y ejecuta:

```bash
pytest -q
```

## Buenas prácticas
- Guarda tokens y claves en un archivo `.env` (no subirlo al repositorio).
- Revisa los ejemplos en `examples/` para entender los parámetros soportados por cada adaptador.

## Contribuciones
Si quieres añadir soporte para una nueva API o mejorar la librería, abre un Pull Request o crea un Issue describiendo el cambio. Sigue las convenciones del proyecto y añade pruebas cuando sea posible.

## Licencia
Apache 2.0

