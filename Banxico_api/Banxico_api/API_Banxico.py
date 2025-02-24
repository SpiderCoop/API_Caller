
# Librerias necesarias -------------------------------------------------------------------------


import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests


if __name__ == '__main__':
    import os
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
    from API_Father.Base_api import Base_api
    
else:
    from ...API_Father.Base_api import Base_api

# Clase ---------------------------------------------------------------------------------------

class API_Banxico(Base_api):
    def __init__(self, api_key):
        super().__init__(api_key, "https://www.banxico.org.mx/SieAPIRest/service/v1")

    def _set_params(self,  ult_obs:bool=False, fecha_inicio:str='2000-01-01', fecha_fin:str=datetime.today().strftime('%Y-%m-%d'), variacion:str=None, sin_decimales:bool=False):
        
        # Validar los tipos de datos de los parámetros
        if not isinstance(ult_obs, bool):
            raise ValueError(f"ult_obs debe ser un valor booleano.")
        
        if variacion is not None:
            if not isinstance(variacion, str):
                raise ValueError(f"variacion debe ser una cadena de texto.")
            elif variacion not in ['PorcObsAnt', 'PorcAnual', 'PorcAcumAnual']:
                raise ValueError(f"variacion debe ser uno de los siguientes valores: 'PorcObsAnt', 'PorcAnual', 'PorcAcumAnual'")
        
        if not isinstance(sin_decimales, bool):
            raise ValueError(f"sin_decimales debe ser un valor booleano.")
        

        # Definir la URL de la API
        if ult_obs:
            # Validar que si ult_obs es True, no se proporcionen fechas de inicio y fin
            if (fecha_inicio != '2000-01-01' or fecha_fin != datetime.today().strftime('%Y-%m-%d')):
                raise ValueError("Si ult_obs es True, no se pueden proporcionar fechas de inicio y fin.")
            
            # Definir la URL de la API con el ID de la serie para obtener los datos de la última observación
            endpoint = f"/series/{','.join(serie_id)}/datos/oportuno"
        
        else:
            # Asegurar que las fechas esten en el formato correcto
            try:
                fecha_inicio = str(datetime.strptime(fecha_inicio, '%Y-%m-%d').date())
            except ValueError:
                raise ValueError("Las fechas deben estar en el formato 'YYYY-MM-DD'.")
            try:
                fecha_fin = str(datetime.strptime(fecha_fin, '%Y-%m-%d').date())
            except ValueError:
                raise ValueError("Las fechas deben estar en el formato 'YYYY-MM-DD'.")
            
            # Mandar mensaje de error si la fecha de inicio es mayor a la fecha de fin
            if fecha_inicio > fecha_fin:
                raise ValueError("La fecha de inicio no puede ser mayor a la fecha de fin.")
            
            # Definir la URL de la API con el ID de la serie para obtener los datos en un rango de fechas
            endpoint = f"/series/{','.join(serie_id)}/datos/{fecha_inicio}/{fecha_fin}"

        
        # Definir los parámetros adicionales si se proporcionan
        additional_params = []
        if sin_decimales:
            additional_params.append(f'decimales=sinCeros')
        
        if variacion:
            additional_params.append(f'incremento={variacion}')
            
        # Unir los parámetros adicionales en una cadena de query params
        additional_params_str = '&'.join(additional_params)

        # Agregar los parámetros adicionales a la URL si existen
        if additional_params_str:
            endpoint = f"{endpoint}?{additional_params_str}"

        # Encabezados para la solicitud con el token de la API
        headers = {
            'Bmx-Token': self._Base_api__api_key
        }
        
        return endpoint, headers
    
    def get_metadata(self, serie_id:str | list) -> dict:
        """
        Obtiene los metadatos de una serie económica desde la API de Banxico (SIE).

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).

        Returns:
            dict: Un diccionario con informacion de la serie

        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            >>> metadata = get_metadata(serie_id='SF43718')
        """
        
        # Validar los tipos de datos de las series
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")
        
        # Definir la URL de la API con el ID de la serie para obtener los metadatos de las series y realizar la solicitud
        endpoint_metadatos = f"/series/{','.join(serie_id)}"
        headers = {
            'Bmx-Token': self._Base_api__api_key
        }
        metadata_json = self._make_request(endpoint_metadatos, headers=headers)
        
        # Inicializar un diccionario para almacenar los metadatos
        series_dict = {}

        for serie_metadata in metadata_json['bmx']['series']:
            serie_id = serie_metadata['idSerie']
            series_dict[serie_id] = {'titulo': serie_metadata['titulo'], 'periodicidad': serie_metadata['periodicidad'], 'cifra': serie_metadata['cifra'], 'unidad': serie_metadata['unidad']}
        
        return series_dict
    

    # Función para obtener los datos de una serie desde la API de Banxico
    def get_data(self, serie_id:str | list, ult_obs:bool=False, fecha_inicio:str='2000-01-01', fecha_fin:str=datetime.today().strftime('%Y-%m-%d'), variacion:str=None, sin_decimales:bool=False) -> tuple[pd.DataFrame, dict]:
        """
        Obtiene datos de series económicas desde la API de Banxico (SIE) y los devuelve en un DataFrame de pandas.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).
            ult_obs (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.
            fecha_inicio (datetime, optional): La fecha de inicio de consulta tipo datetime para obtener datos en formato 'YYYY-MM-DD'. 
                                            Por defecto es '2000-01-01'.
            fecha_fin (datetime, optional): La fecha de fin de consulta tipo datetime  para obtener datos en formato 'YYYY-MM-DD'.
                                            Por defecto es la fecha actual.
            variacion (str, optional): Parámetro opcional que define si se desea obtener los incrmentos porcentuales de datos de la serie con respecto a observaciones anteriores
                                    ('PorcObsAnt', 'PorcAnual', 'PorcAcumAnual'). Por defecto es None.
            sin_decimales (bool, optional): Si se establece en True, los datos se devolverán sin decimales. 
                                            Por defecto es False.

        Returns:
            pandas.DataFrame: Un DataFrame con las series obtenidas. Las columnas representan las series, y las filas 
                            corresponden a las fechas de observación.
            dict: Un diccionario con informacion de la serie
                            
        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            Obtener la última observación de una serie:
            >>> df, dict = get_SIE_data(serie_id='SF43718', ult_obs=True)

            Obtener un rango de fechas para una serie histórica de su variación anual:
            >>> df, dict = get_SIE_data(serie_id='SF43718', fecha_inicio='2020-01-01', fecha_fin='2023-01-01', variacion='PorcAnual')
        """
        
        # Validar los tipos de datos de las series y los parámetros
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")


        # Definir la URL de la API con el ID de la serie para obtener los datos de las series y realizar la solicitud
        endpoint_datos, headers = self._set_params(ult_obs, fecha_inicio, fecha_fin, variacion, sin_decimales)
        data_json = self._make_request(endpoint_datos, headers=headers)

        # Definir la URL de la API con el ID de la serie para obtener los metadatos de las series y realizar la solicitud
        endpoint_metadatos = f"/series/{','.join(serie_id)}"
        metadata_json = self._make_request(endpoint_metadatos, headers=headers)

        metadata = self.get_metadata(serie_id)
        
        # Inicializar un DataFrame vacío para almacenar los datos de las series
        series_df = pd.DataFrame()

        for serie_data in data_json['bmx']['series']:

            serie_id = serie_data['idSerie']

            # Extraer datos de la serie
            serie_data = serie_data['datos']

            # Extraer los valores y las fechas
            obs_values = [float(entry['dato'].replace(",", "")) if entry['dato'] != 'N/E' else pd.NA for entry in serie_data]
            time_periods = [entry['fecha'] for entry in serie_data]

            # Formatear los periodos de tiempo
            time_periods_formatted = [pd.to_datetime(period, dayfirst=True).date() for period in time_periods]

            # Crear una serie de pandas con los datos obtenidos
            serie = pd.Series(obs_values, index=time_periods_formatted, name=serie_id)

            # Para series trimestrales se ajusta la fecha dos periodos hacia adelante. Esto es para que la fecha sea el último mes del trimestre
            if metadata[serie_id]['periodicidad'] == 'Trimestral':
                serie.index = serie.index + relativedelta(months=2)

            # Agregar la serie al DataFrame
            series_df = pd.concat([series_df, serie], axis=1, join='outer')

        # Ordenar el DataFrame por fecha
        series_df = series_df.sort_index()

        return series_df


# Ejemplo de uso
if __name__ == '__main__':

    # Carga variables de un archivo .env (para almacenar el token de la API de Banxico)
    from dotenv import load_dotenv
    load_dotenv()
    BANXICO_TOKEN = os.environ.get("BANXICO_TOKEN")

    # Ejemplo de uso de la clase API_Banxico
    banxico_api = API_Banxico(BANXICO_TOKEN)
    serie_id = ['SR17622', 'SF61745', 'SP68257', 'SF43718']

    # Obtener datos de la serie desde 2023-01-01 hasta hoy
    serie = banxico_api.get_data(serie_id, fecha_inicio='2023-01-01',variacion='PorcAnual')

    print('\n')
    print(serie.loc[pd.date_range(start='2024-01-01', end='2024-12-01', freq='QS-MAR').date,'SR17622'])
    print('\n')
