"""
Description:   Class to interact with the API of the Bank for International Settlements (BIS).
Author:        David Jiménez Cooper - SpiderCoop
Date:          2026-09-02
"""

import pandas as pd
from econ_api_bridge.baseapi.baseapi import BaseAPI


class BIS_API(BaseAPI):
    def __init__(self, api_key:str=None):
        super().__init__(api_key, "https://stats.bis.org/api/v2")


    def _set_series_params(self, serie_id:str | list,  last_data:bool=False, start_date:str=None, end_date:str=pd.Timestamp.today().strftime('%Y-%m-%d'), context:str='dataflow', agencyID:str='BIS', resourceID:str='%2A', version:str='%2A') -> tuple:

        headers = {
            'Accept': 'application/vnd.sdmx.data+json;version=1.0.0',
        }

        # Validar los tipos de datos de las series y los parámetros
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")
        
        # Construir el endpoint base para la consulta de datos o metadatos de las series
        endpoint = f'/data/{context}/{agencyID}/{resourceID}/{version}/{",".join(serie_id)}'

        # Inicializar una lista para almacenar los parámetros adicionales
        additional_params = []

        if not isinstance(last_data, bool):
            raise ValueError(f"last_data debe ser un valor booleano.") 

        # Definir la URL de la API con el ID de la serie para obtener los datos de las series
        if last_data:
            # Validar que si last_data es True, no se proporcionen fechas de inicio y fin
            if (start_date is not None or end_date != pd.Timestamp.today().strftime('%Y-%m-%d')):
                raise ValueError("Si last_data es True, no se pueden proporcionar fechas de inicio y fin.")
            
            # Definir la URL de la API con el ID de la serie para obtener los datos de la última observación
            additional_params.append(f"lastNObservations=1")
        
        elif start_date is not None:
            
            # Asegurar que las fechas esten en el formato correcto
            try:
                end_date = pd.to_datetime(end_date).strftime('%Y-%m-%d')
            except ValueError:
                raise ValueError("The provided dates must be in format 'YYYY-MM-DD'.")
            
            try:
                start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            except ValueError:
                raise ValueError("The provided dates must be in format 'YYYY-MM-DD'.")
        
            # Mandar mensaje de error si la fecha de inicio es mayor a la fecha de fin
            if start_date > end_date:
                raise ValueError("La fecha de inicio no puede ser mayor a la fecha de fin.")
        
            # Definir la URL de la API en un rango de fechas
            additional_params.append(f"startPeriod={start_date}")
            additional_params.append(f"endPeriod={end_date}")

            
        # Unir los parámetros adicionales en una cadena de query params
        additional_params_str = '&'.join(additional_params)

        # Agregar los parámetros adicionales a la URL si existen
        if additional_params_str:
            endpoint = f"{endpoint}?{additional_params_str}"

        return endpoint, headers
    

    
    def get_series_metadata(self, serie_id:str | list) -> dict:
        """
        Obtiene los metadatos de una serie económica desde la API del BIS.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).

        Returns:
            dict: Un diccionario con informacion de la serie

        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            >>> metadata = get_series_metadata(serie_id='SF43718')
        """
        
        # Definir la URL de la API con el ID de la serie para obtener los metadatos de las series y realizar la solicitud
        endpoint, headers = self._set_series_params(serie_id, get_series_metadata=True)
        metadata_json = self._make_request(endpoint, headers=headers)
        
        # Inicializar un diccionario para almacenar los metadatos
        series_dict = {}

        for serie_metadata in metadata_json['bmx']['series']:
            serie_id = serie_metadata['idSerie']
            series_dict[serie_id] = {'titulo': serie_metadata['titulo'], 'periodicidad': serie_metadata['periodicidad'], 'cifra': serie_metadata['cifra'], 'unidad': serie_metadata['unidad']}
        
        return series_dict
    

    # Función para obtener los datos de una serie desde la API de Banxico
    def get_series_data(self, serie_id:str | list, last_data:bool=False, start_date:str=None, end_date:str=pd.Timestamp.today().strftime('%Y-%m-%d')) -> pd.DataFrame:
        """
        Obtiene datos de series económicas desde la API de Banxico (SIE) y los devuelve en un DataFrame de pandas.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API del BIS. 
                                    Si se proporciona un solo ID, puede ser una cadena de texto (str).
            last_data (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.
            start_date (datetime, optional): La fecha de inicio de consulta tipo datetime para obtener datos en formato 'YYYY-MM-DD'. 
                                            Por defecto es '2000-01-01'.
            end_date (datetime, optional): La fecha de fin de consulta tipo datetime  para obtener datos en formato 'YYYY-MM-DD'.
                                            Por defecto es la fecha actual.

        Returns:
            pandas.DataFrame: Un DataFrame con las series obtenidas. Las columnas representan las series, y las filas 
                            corresponden a las fechas de observación.
                            
        Raises:
            Exception: Si la solicitud a la API de BIS falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            Obtener la última observación de una serie:
            >>> df = get_series_data(serie_id='SF43718', last_data=True)

            Obtener un rango de fechas para una serie histórica de su variación anual:
            >>> df = get_series_data(serie_id='SF43718', start_date='2020-01-01', end_date='2023-01-01')
        """

        
        # Definir la URL de la API con el ID de la serie para obtener los datos de las series y realizar la solicitud
        endpoint_datos, headers = self._set_series_params(serie_id, last_data=last_data, start_date=start_date, end_date=end_date)
        data_json = self._make_request(endpoint_datos, headers=headers)

        # Inicializar un DataFrame vacío para almacenar los datos de las series
        series_df = pd.DataFrame()

        # Obtenemos las fechas de observación de la serie a partir del JSON de datos
        time_periods = data_json["data"]["structure"]["dimensions"]["observation"][0]["values"]
        dates = pd.Series([pd.to_datetime(t["end"]).date() for t in time_periods])

        # Obtenemos los datos de cada serie a partir del JSON de datos y los agregamos al DataFrame
        series_dict = data_json["data"]["dataSets"][0]["series"]
        for i, (series_key, series_content) in enumerate(series_dict.items()):

            observations = series_content["observations"]

            # Crear una serie de pandas para cada serie de datos, tomamos los indices como guia para asignar los valores a las fechas correspondientes, y luego concatenamos cada serie al DataFrame principal
            serie = pd.Series(dtype=float, name=serie_id[i] if isinstance(serie_id, list) else serie_id)
            max_len = 0
            for obs_index, obs_value in observations.items():
                idx = int(obs_index)
                serie[idx] = float(obs_value[0])

            series_df = pd.concat([series_df, serie], axis=1, join='outer')
        
        series_df.index = dates
        series_df.sort_index(inplace=True, ascending=True)

        return series_df

