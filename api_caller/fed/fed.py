
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



    def _set_params(self,serie_id:str | list, last_data:bool=False, start_time:str=None, end_time:str=datetime.today().strftime('%Y-%m-%d')) -> str:
        
        # Validar los tipos de datos de los parámetros
        if not isinstance(last_data, bool):
            raise ValueError(f"ult_obs debe ser un valor booleano.")
        
        # Validar los tipos de datos de las series y los parámetros
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")
        

        # Definir la URL de la API con el ID de la serie para obtener los datos de las series
        if last_data:
            # Validar que si ult_obs es True, no se proporcionen fechas de inicio y fin
            if (start_time != None or end_time != datetime.today().strftime('%Y-%m-%d')):
                raise ValueError("If last_data is True, start_time and end_time cannot be provided.")
            
            # Definir la URL de la API con el ID de la serie para obtener los datos de la última observación
            endpoint = f"/series?{','.join(serie_id)}"
        
        else:
            # Definir los parámetros de las fechas si se proporcionan
            date_params = []
            
            if start_time != None:
                try:
                    start_time = str(datetime.strptime(start_time, '%Y-%m-%d').date())
                except ValueError:
                    raise ValueError("The provided dates must be in format 'YYYY-MM-DD'.")

            try:
                fecha_fin = str(datetime.strptime(fecha_fin, '%Y-%m-%d').date())
            except ValueError:
                raise ValueError("The provided dates must be in format 'YYYY-MM-DD'.")
            
            
            # En caso de que la fecha de inicio es mayor a la fecha de fin, se invierten las variables
            if start_time > end_time:
                start_time, end_time = end_time, start_time
                print('start_time is greater than end_time. The dates have been switched.')

            # Agregar los parámetros de las fechas a la URL si se proporcionan
            if start_time != None:
                date_params.append(f'realtime_start={start_time}')
            
            date_params.append(f'realtime_end={end_time}')

            if date_params:
                endpoint = f"/series/observations?{','.join(serie_id)}&{'&'.join(date_params)}"
                
        endpoint += f"&api_key={self._BaseAPI__api_key}&format=json"

        return endpoint
    
    

    # Función para obtener los datos de una serie desde la API de Banxico
    def get_data(self, serie_id:str | list, last_data:bool=False, start_time:str='2000-01-01', end_time:str=datetime.today().strftime('%Y-%m-%d')) -> pd.DataFrame:
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

        # Definir la URL de la API con el ID de la serie para obtener los datos de las series y realizar la solicitud
        endpoint_datos = self._set_params(self,serie_id, last_data, start_time, end_time)
        data_json = self._make_request(endpoint_datos)

        return data_json

