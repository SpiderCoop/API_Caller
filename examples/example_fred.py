# Ejemplo de uso
import os

import pandas as pd
from dotenv import load_dotenv

from econ_api_bridge.fed import Fred

# Carga variables de un archivo .env (para almacenar el token de la API de FRED)
load_dotenv()
FRED_Token = os.environ.get("FRED_TOKEN")

# Ejemplo de uso de la clase Fred
fred_api = Fred(FRED_Token)
serie_id = ["DFF", "IRA"]

# Obtener datos de las series de FRED DFF, IRA
serie = fred_api.get_series_data(serie_id, end_date=pd.Timestamp.today())
# metadata = fred_api.get_series_metadata(serie_id)

print(serie.loc["2025-07-01"])
print("\n")
# print(metadata)
print("\n")
