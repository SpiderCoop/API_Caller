
# Librerias necesarias -------------------------------------------------------------------------

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests

from ..baseapi.baseapi import BaseAPI

# Clase ---------------------------------------------------------------------------------------

class Fred(BaseAPI):
    def __init__(self, api_key):
        super().__init__(api_key, "https://api.stlouisfed.org/fred")


    def _set_series_params(self,serie_id:str, last_data:bool=False, start_date:str=None, end_date:str=pd.Timestamp.today().strftime('%Y-%m-%d'), get_metadata:bool=False) -> str:
        
        # Validar los tipos de datos de los parámetros
        if not isinstance(last_data, bool):
            raise ValueError(f"last_data must be a booleano.")
        
        # Validar los tipos de datos de las series y los parámetros
        if not isinstance(serie_id, str):
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")
        
        #if len(serie_id) > 1 and last_data:
            #raise ValueError("last_data must be False if multiple series are provided.")
        
        # Devuelve la URL de la API si se solicitan los metadatos
        if get_metadata:
            pass

        # Definir la URL de la API con el ID de la serie para obtener los datos de las series
        endpoint = f"/series/observations?series_id={serie_id}"

        if last_data:
            # Validar que si ult_obs es True, no se proporcionen fechas de inicio y fin
            if (start_date is not None or end_date != pd.Timestamp.today().strftime('%Y-%m-%d')):
                raise ValueError("If last_data is True, start_date and end_date cannot be provided.")
            
            # Definir la URL para que solo ponga como limite = 1 para obtener la última observación y ponemos el sort order en desc
            endpoint += f"&limit=1&sort_order=desc"
        
        else:
            # Definir los parámetros de las fechas si se proporcionan
            date_params = []
            
            if start_date != None:
                try:
                    start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                    end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d')
                except ValueError:
                    raise ValueError("The provided dates must be in format 'YYYY-MM-DD'.")
            
                # En caso de que la fecha de inicio es mayor a la fecha de fin, se invierten las variables
                if start_date > end_date:
                    start_date, end_date = end_date, start_date
                    print('start_date is greater than end_date. The dates have been switched.')

            # Agregar los parámetros de las fechas a la URL si se proporcionan
            if start_date != None:
                date_params.append(f'observation_start={start_date}')
            
            date_params.append(f'observation_end={end_date}')

            if date_params:
                endpoint += f"&{'&'.join(date_params)}"

        # Agregar el token de la API a la URL
        endpoint += f"&api_key={self._BaseAPI__api_key}&file_type=json"

        return endpoint
    

    def get_series_metadata(self, serie_id:str | list) -> dict: # Hace falta modificar
        """
        Obtiene los metadatos de una serie desde la API de la FED.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).

        Returns:
            dict: Un diccionario con informacion de la serie

        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            >>> metadata = get_metadata(serie_id='SF43718')
        """
        
        # Definir la URL de la API con el ID de la serie para obtener los metadatos de las series y realizar la solicitud
        endpoint = self._set_series_params(serie_id, get_metadata=True)
        metadata_json = self._make_request(endpoint)

        # Extraer los datos de la serie
        serie_metadata = metadata_json['seriess'][0]
        
        # Inicializar un diccionario para almacenar los metadatos
        series_dict = {}

        serie_id = serie_metadata['id']
        series_dict[serie_id] = {'title': serie_metadata['title'], 'frequency': serie_metadata['frequency'], 'observation_start': pd.to_datetime(serie_metadata['observation_start']).date(), 'observation_end': pd.to_datetime(serie_metadata['observation_end']).date(), 'units': serie_metadata['units'],'seasonal_adjustment': serie_metadata['seasonal_adjustment'], 'last_updated': serie_metadata['last_updated'], 'notes': serie_metadata['notes']}
        
        return series_dict
    
    

    # Función para obtener los datos de una serie desde la API
    def get_series_data(self, serie_id:str | list, last_data:bool=False, start_date:str=None, end_date:str=pd.Timestamp.today().strftime('%Y-%m-%d')) -> pd.DataFrame:
        """
        Obtiene datos de series económicas desde la API de Banxico (SIE) y los devuelve en un DataFrame de pandas.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).
            ult_obs (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.
            fecha_inicio (datetime, optional): La fecha de inicio de consulta tipo datetime para obtener datos en formato 'YYYY-MM-DD'. 
                                            Por defecto es '2000-01-01'.
            end_date (datetime, optional): La fecha de fin de consulta tipo datetime  para obtener datos en formato 'YYYY-MM-DD'.
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
            >>> df, dict = get_SIE_data(serie_id='SF43718', fecha_inicio='2020-01-01', end_date='2023-01-01', variacion='PorcAnual')
        """

        # Validar los tipos de datos de las series y los parámetros
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")

        # Inicializar un DataFrame vacío para almacenar los datos de las series
        series_df = pd.DataFrame()

        for id in serie_id:

            # Definir la URL de la API con el ID de la serie para obtener los datos de las series y realizar la solicitud
            endpoint = self._set_series_params(id, last_data, start_date, end_date)
            data_json = self._make_request(endpoint)

            # Extraer los datos de la serie
            serie_data = data_json['observations']

            # Extraer los valores y las fechas
            obs_values = [float(entry['value'].replace(",", "")) if entry['value'] != '.' else pd.NA for entry in serie_data]
            time_periods = [entry['date'] for entry in serie_data]

            # Formatear los periodos de tiempo
            time_periods_formatted = [pd.to_datetime(period).date() for period in time_periods]
            
            # Agregar la observación a un DataFrame de pandas
            serie = pd.Series(obs_values, index=time_periods_formatted, name=id)

            # Agregar la serie al DataFrame
            series_df = pd.concat([series_df, serie], axis=1, join='outer')

        # Ordenar el DataFrame por fecha
        series_df = series_df.sort_index()

        # Verificar que el indice es del  tipo datetime
        if not pd.api.types.is_datetime64_any_dtype(series_df.index):
            series_df.index = pd.to_datetime(series_df.index)
        
        return serie


    def get_releases_data(self, serie_id:str | list, last_data:bool=False, start_date:str=None, end_date:str=datetime.today().strftime('%Y-%m-%d')) -> pd.DataFrame:
        """
        Obtiene datos de series económicas desde la API de Banxico (SIE) y los devuelve en un DataFrame de pandas.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).
            ult_obs (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.
            fecha_inicio (datetime, optional): La fecha de inicio de consulta tipo datetime para obtener datos en formato 'YYYY-MM-DD'. 
                                            Por defecto es '2000-01-01'.
            end_date (datetime, optional): La fecha de fin de consulta tipo datetime  para obtener datos en formato 'YYYY-MM-DD'.
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
            >>> df, dict = get_SIE_data(serie_id='SF43718', fecha_inicio='2020-01-01', end_date='2023-01-01', variacion='PorcAnual')
        """

        # Definir la URL de la API con el ID de la serie para obtener los datos de las series y realizar la solicitud
        endpoint = self._set_series_params(serie_id, last_data, start_date, end_date)
        data_json = self._make_request(endpoint)

        # Extraer los datos de la serie
        serie_data = data_json['observations']

        # Extraer los valores y las fechas
        obs_values = [float(entry['value'].replace(",", "")) if entry['value'] != 'N/E' else pd.NA for entry in serie_data]
        time_periods = [entry['date'] for entry in serie_data]

        # Formatear los periodos de tiempo
        time_periods_formatted = [pd.to_datetime(period).date() for period in time_periods]
        
        # Agregar la observación a un DataFrame de pandas
        serie = pd.Series(obs_values, index=time_periods_formatted, name=serie_id)
        
        return serie