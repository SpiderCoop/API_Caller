# Ejemplo de uso
import sys
import os
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from api_caller.fed import Fred

# Carga variables de un archivo .env (para almacenar el token de la API de INEGI)
load_dotenv()
FRED_Token = os.environ.get("FRED_Token")

# Ejemplo de uso de la clase API_INEGI
fred_api = Fred(FRED_Token)
serie_id='IRA'

# Obtener datos de las series de INEGI 628208, 736183 (PIB constante 2018 desestacionalizado var anual)
serie = fred_api.get_series_data(serie_id)
metadata = fred_api.get_series_metadata(serie_id)

print(serie)
print('\n')
print(metadata)
print('\n')
