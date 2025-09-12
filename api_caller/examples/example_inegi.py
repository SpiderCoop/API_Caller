# Ejemplo de uso
import sys
import os
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from api_caller.inegi import INEGI_BIE

# Carga variables de un archivo .env (para almacenar el token de la API de INEGI)
load_dotenv()
INEGI_BIE_Token = os.environ.get("INEGI_Token")

# Ejemplo de uso de la clase API_INEGI
inegi_api = INEGI_BIE(INEGI_BIE_Token)
serie_id=['736183','628208']

# Obtener datos de las series de INEGI 628208, 736183 (PIB constante 2018 desestacionalizado var anual)
serie = inegi_api.get_series_data(serie_id,last_data=False)
print(serie.loc[pd.date_range(start='2024-01-01', end='2024-12-01', freq='QS-MAR').date,'736183'])
print('\n')

