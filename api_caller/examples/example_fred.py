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
serie_id='EXJPUS'

# Obtener datos de las series de INEGI 628208, 736183 (PIB constante 2018 desestacionalizado var anual)
serie = fred_api.get_data(serie_id,start_time='2023-01-01',end_time='2024-12-01')
print(serie)
print('\n')
