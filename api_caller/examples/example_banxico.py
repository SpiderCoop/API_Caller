# Ejemplo de uso
import sys
import os
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from api_caller.banxico import Banxico_SIE

# Carga variables de un archivo .env (para almacenar el token de la API de Banxico)
load_dotenv()
Banxico_SIE_Token = os.environ.get("Banxico_SIE_Token")

# Ejemplo de uso de la clase API_Banxico
banxico_api = Banxico_SIE(Banxico_SIE_Token)
serie_id = ['SR17622', 'SF61745', 'SP68257', 'SF43718']

# Obtener datos de la serie desde 2023-01-01 hasta hoy
serie = banxico_api.get_data(serie_id, fecha_inicio='2023-01-01',variacion='PorcAnual')
print(serie.loc[pd.date_range(start='2024-01-01', end='2024-12-01', freq='QS-MAR').date,'SR17622'])
print('\n')

# Obtener datos de la serie desde 2023-01-01 hasta hoy
serie = banxico_api.get_data(serie_id, ult_obs=True,variacion='PorcAnual')
print(serie)
print('\n')