# Ejemplo de uso
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from fed.fed import Fred
import pandas as pd
import os


# Carga variables de un archivo .env (para almacenar el token de la API de INEGI)
from dotenv import load_dotenv
load_dotenv()
FRED_Token = os.environ.get("FRED_Token")

# Ejemplo de uso de la clase API_INEGI
inegi_api = Fred(FRED_Token)
serie_id=['GNPCA']

# Obtener datos de las series de INEGI 628208, 736183 (PIB constante 2018 desestacionalizado var anual)
serie = inegi_api.get_data(serie_id,last_data=False)

print(serie.loc[pd.date_range(start='2024-01-01', end='2024-12-01', freq='QS-MAR').date,'736183'])
print('\n')
