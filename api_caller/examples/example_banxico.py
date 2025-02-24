# Ejemplo de uso
    
from banxico.sie import BanxicoSIE
import pandas as pd
import os

# Carga variables de un archivo .env (para almacenar el token de la API de Banxico)
from dotenv import load_dotenv
load_dotenv()
BANXICO_TOKEN = os.environ.get("BANXICO_TOKEN")

# Ejemplo de uso de la clase API_Banxico
banxico_api = BanxicoSIE(BANXICO_TOKEN)
serie_id = ['SR17622', 'SF61745', 'SP68257', 'SF43718']

# Obtener datos de la serie desde 2023-01-01 hasta hoy
serie = banxico_api.get_data(serie_id, fecha_inicio='2023-01-01',variacion='PorcAnual')

print('\n')
print(serie.loc[pd.date_range(start='2024-01-01', end='2024-12-01', freq='QS-MAR').date,'SR17622'])
print('\n')