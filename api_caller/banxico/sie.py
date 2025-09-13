
# Librerias necesarias -------------------------------------------------------------------------

import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests

from ..baseapi.baseapi import BaseAPI

# Clase ---------------------------------------------------------------------------------------

class Banxico_SIE(BaseAPI):
    def __init__(self, api_key):
        super().__init__(api_key, "https://www.banxico.org.mx/SieAPIRest/service/v1")

    def _set_series_params(self,serie_id:str | list,  last_data:bool=False, start_date:str=None, end_date:str=pd.Timestamp.today().strftime('%Y-%m-%d'), percentage_change:str=None, no_decimals:bool=False, get_series_metadata:bool=False) -> tuple:
        
        # Encabezados para la solicitud con el token de la API
        headers = {
            'Bmx-Token': self._BaseAPI__api_key
        }


        # Validar los tipos de datos de las series y los parámetros
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")
        

        if not isinstance(last_data, bool):
            raise ValueError(f"last_data debe ser un valor booleano.")
        
        
        if percentage_change is not None:
            if not isinstance(percentage_change, str):
                raise ValueError(f"percentage_change debe ser una cadena de texto.")
            elif percentage_change not in ['PorcObsAnt', 'PorcAnual', 'PorcAcumAnual']:
                raise ValueError(f"percentage_change debe ser uno de los siguientes valores: 'PorcObsAnt', 'PorcAnual', 'PorcAcumAnual'")
        

        if not isinstance(no_decimals, bool):
            raise ValueError(f"no_decimals debe ser un valor booleano.")
        

        if not isinstance(get_series_metadata, bool):
            raise ValueError(f"get_series_metadata debe ser un valor booleano.")
        
        
        # Definir la URL de la API con el ID de la serie para obtener los datos de las series
        endpoint = f"/series/{','.join(serie_id)}"
        
        # Devuelve la URL de la API y los encabezados si se solicitan los metadatos
        if get_series_metadata:
            return endpoint, headers
        

        # Definir la URL de la API con el ID de la serie para obtener los datos de las series
        if last_data:
            # Validar que si last_data es True, no se proporcionen fechas de inicio y fin
            if (start_date is not None or end_date != pd.Timestamp.today().strftime('%Y-%m-%d')):
                raise ValueError("Si last_data es True, no se pueden proporcionar fechas de inicio y fin.")
            
            # Definir la URL de la API con el ID de la serie para obtener los datos de la última observación
            endpoint += f"/datos/oportuno"
        
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
            endpoint += f"/datos/{start_date}/{end_date}"

        
        # Definir los parámetros adicionales si se proporcionan
        additional_params = []
        if no_decimals:
            additional_params.append(f'decimales=sinCeros')
        
        if percentage_change:
            additional_params.append(f'incremento={percentage_change}')
            
        # Unir los parámetros adicionales en una cadena de query params
        additional_params_str = '&'.join(additional_params)

        # Agregar los parámetros adicionales a la URL si existen
        if additional_params_str:
            endpoint = f"{endpoint}?{additional_params_str}"

        return endpoint, headers
    

    
    def get_series_metadata(self, serie_id:str | list) -> dict:
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
    def get_series_data(self, serie_id:str | list, last_data:bool=False, start_date:str=None, end_date:str=pd.Timestamp.today().strftime('%Y-%m-%d'), percentage_change:str=None, no_decimals:bool=False) -> pd.DataFrame:
        """
        Obtiene datos de series económicas desde la API de Banxico (SIE) y los devuelve en un DataFrame de pandas.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).
            last_data (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.
            start_date (datetime, optional): La fecha de inicio de consulta tipo datetime para obtener datos en formato 'YYYY-MM-DD'. 
                                            Por defecto es '2000-01-01'.
            end_date (datetime, optional): La fecha de fin de consulta tipo datetime  para obtener datos en formato 'YYYY-MM-DD'.
                                            Por defecto es la fecha actual.
            percentage_change (str, optional): Parámetro opcional que define si se desea obtener los incrmentos porcentuales de datos de la serie con respecto a observaciones anteriores
                                    ('PorcObsAnt', 'PorcAnual', 'PorcAcumAnual'). Por defecto es None.
            no_decimals (bool, optional): Si se establece en True, los datos se devolverán sin decimales. 
                                            Por defecto es False.

        Returns:
            pandas.DataFrame: Un DataFrame con las series obtenidas. Las columnas representan las series, y las filas 
                            corresponden a las fechas de observación.
            dict: Un diccionario con informacion de la serie
                            
        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            Obtener la última observación de una serie:
            >>> df, dict = get_SIE_data(serie_id='SF43718', last_data=True)

            Obtener un rango de fechas para una serie histórica de su variación anual:
            >>> df, dict = get_SIE_data(serie_id='SF43718', start_date='2020-01-01', end_date='2023-01-01', percentage_change='PorcAnual')
        """

        # Ajuste para datos trimestrales
        if not last_data:
            start_date += pd.DateOffset(months=-2)
        
        # Definir la URL de la API con el ID de la serie para obtener los datos de las series y realizar la solicitud
        endpoint_datos, headers = self._set_series_params(serie_id, last_data, start_date, end_date, percentage_change, no_decimals)
        data_json = self._make_request(endpoint_datos, headers=headers)

        # Definir la URL de la API con el ID de la serie para obtener los metadatos de las series y realizar la solicitud
        metadata = self.get_series_metadata(serie_id)
        
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
            time_periods_formatted = [pd.to_datetime(period, dayfirst=True) for period in time_periods]

            # Crear una serie de pandas con los datos obtenidos
            serie = pd.Series(obs_values, index=time_periods_formatted, name=serie_id)
            
            # Verificar que el indice es del  tipo datetime
            if not pd.api.types.is_datetime64_any_dtype(serie.index):
                serie.index = pd.to_datetime(serie.index)

            # Para series trimestrales se ajusta la fecha dos periodos hacia adelante. Esto es para que la fecha sea el último mes del trimestre
            if metadata[serie_id]['periodicidad'] == 'Trimestral':
                serie.index = serie.index + pd.DateOffset(months=2)

            # Agregar la serie al DataFrame
            series_df = pd.concat([series_df, serie], axis=1, join='outer')

        # Ordenar el DataFrame por fecha
        series_df = series_df.sort_index()

        # Ajustamos la fecha a su dato original
        if not last_data:
            start_date += pd.DateOffset(months=2)
            series_df = series_df.loc[start_date:end_date]

        # Verificar que el indice es del  tipo datetime
        if not pd.api.types.is_datetime64_any_dtype(series_df.index):
            series_df.index = pd.to_datetime(series_df.index)

        return series_df

