
# Librerias necesarias -------------------------------------------------------------------------

import pandas as pd
from datetime import datetime, date
import requests

from ..baseapi.baseapi import BaseAPI

# Clase -------------------------------------------------------------------------

class INEGI_BIE(BaseAPI):
    def __init__(self, api_key):
        super().__init__(api_key, "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml")

    # Funcion para cambiar la presentacion de los periodos de tiempo de la serie de acuerdo con las especificacionesde la metadata de la API de INEGI
    def _freq_handler(self, frequency_id:int):
        """
        Recibe los datos de la frecuencia de actualización de los datos, de acuerdo con la metadata y descripcion revisada desde la API de INEGI, devuelve las fechas correspondientes en tipo datetime.

        Args:
            time_periods (list): Los periodos de tiempo de la serie.
            frequency_id (int): El ID de la frecuencia de la serie.

        Returns:
            datetime.date: Una lista de objetos datetime.date con las fechas correspondientes a los periodos de tiempo de la serie.
            string: Un objeto string con la descripcion de la frecuencia de la serie.
        """

        # Definir url de API
        endpoint = f"/CL_FREQ/{frequency_id}/es/BIE/2.0/{self._BaseAPI__api_key}?type=json"

        # Extraer y convertir datos
        data_json = self._make_request(endpoint=endpoint)
        serie_data = data_json['CODE'][0]
        freq_str = serie_data['Description']

        return freq_str
    
    
    def _unit_handler(self, unit_id:int):
        """
        Recibe el identificador de las unidades de medida de la serie y, de acuerdo con la metadata y descripcion revisada desde la API de INEGI, devuelve la descripcion de las unidades de medida.

        Args:
            unit_id (int): El identificador de la unidad de medida.

        Returns:
            string: Un objeto string con la descripcion de las unidades de la serie.
        """
        
        # Definir url de API
        endpoint = f"/CL_UNIT/{unit_id}/es/BIE/2.0/{self._BaseAPI__api_key}?type=json"

        # Extraer y convertir datos 
        data_json = self._make_request(endpoint=endpoint)
        serie_data = data_json['CODE'][0]
        unit_str = serie_data['Description']

        return unit_str
    
    
    def _transform_time_periods(self, time_periods:list, frequency_id:int):
        """
        Recibe los datos de los periodos de tiempo de la serie y, de acuerdo con la metadata y descripcion revisada desde la API de INEGI, devuelve las fechas correspondientes en tipo datetime.

        Args:
            time_periods (list): Los periodos de tiempo de la serie.
            frequency_id (int): El ID de la frecuencia de la serie.
        
        Returns:
            datetime.date: Una lista de objetos datetime.date con las fechas correspondientes a los periodos de tiempo de la serie.
        """

        if frequency_id == 1: # 10 años
        
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        elif frequency_id == 2: # 5 años
            
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        elif frequency_id == 3: # Anual
            
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        elif frequency_id == 4: # Semestral
            
            time_periods_formatted = []
            for period in time_periods:
                year, quarter = map(int, period.split('/'))
                time_periods_formatted.append(str(date(year,quarter*3,1)))

        elif frequency_id == 5: # Cuatrimestral
            
            time_periods_formatted = []
            for period in time_periods:
                year, quarter = map(int, period.split('/'))
                time_periods_formatted.append(str(date(year,quarter*3,1)))
        
        elif frequency_id == 6: # Trimestral

            time_periods_formatted = []
            for period in time_periods:
                year, quarter = map(int, period.split('/'))
                time_periods_formatted.append(str(date(year,quarter*3,1)))
        
        elif frequency_id == 7: # Bimestral

            time_periods_formatted = []
            for period in time_periods:
                year, quarter = map(int, period.split('/'))
                time_periods_formatted.append(str(date(year,quarter*3,1)))

        elif frequency_id == 8: # Mensual
            
            time_periods_formatted = [pd.to_datetime(period, dayfirst=True).strftime('%Y-%m-%d') for period in time_periods]

        elif frequency_id == 9: # Quincenal
            
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        elif frequency_id == 10: # Decenal
            
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        elif frequency_id == 11: # Semanal
            
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        elif frequency_id == 12: # Diaria
            
            time_periods_formatted = [pd.to_datetime(period, dayfirst=True).strftime('%Y-%m-%d') for period in time_periods]

        elif frequency_id == 13: # Irregular
            
            raise ValueError('Frecuencia no soportada actualmente. Es necesario modificar el codigo para soportar esta frecuencia.')

        # Cambiamos el tipo de datos a datetime
        time_periods_formatted = pd.to_datetime(time_periods_formatted).date

        return time_periods_formatted
    
    def _set_series_params(self, serie_id:str | list, last_data:bool=False) -> str:
        """
        Establece los parámetros necesarios para realizar una solicitud a la API de INEGI (BIE) y los devuelve en un diccionario.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).
            last_data (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.

        Returns:
            dict: Un diccionario con los parámetros necesarios para realizar una solicitud a la API de Banxico.
                            
        Raises:
            ValueError: Si los argumentos no son del tipo esperado.

        """

        # Verifica tipo y cambia el formato para adecuarse a la API
        if not isinstance(last_data, bool):
            raise ValueError(f"last_data debe ser un valor booleano.")
        if last_data:
            last_data = 'true'
        else:
            last_data = 'false'

        # Validar los tipos de datos de los argumentos
        if isinstance(serie_id, str):
            serie_id = [serie_id]
        elif isinstance(serie_id, list) and all(isinstance(i, str) for i in serie_id):
            pass
        else:
            raise ValueError("El 'serie_id' debe ser una cadena de texto o una lista de cadenas de texto.")

        # Definir url de API
        endpoint = f"/INDICATOR/{','.join(serie_id)}/es/0700/{last_data}/BIE/2.0/{self._BaseAPI__api_key}?type=json"

        return endpoint


    def get_series_metadata(self, serie_id:str | list) -> dict:
        """
        Obtiene la metadata de una serie económica o estadística desde la API de INEGI (BIE) y la devuelve en un diccionario.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).

        Returns:
            dict: Un diccionario con informacion de la serie
                            
        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            Obtener la metadata de una serie:
            >>> serie_info = get_series_metadata('736183')

        """

        # Definir url de API y realizar la solicitud
        endpoint = self._set_series_params(serie_id, last_data=False)
        data_json = self._make_request(endpoint=endpoint)

        # Inicializar un diccionario vacío para almacenar los metadatos
        series_dict = {}

        # Extraer y convertir datos
        data_json = self._make_request(endpoint=endpoint)

        for serie_data in data_json['Series']:
        
            # Extraer metadatos
            serie_id = serie_data['INDICADOR']
            freq = int(serie_data['FREQ'])
            unit = int(serie_data['UNIT'])

            # Tranforma las unidades de medida y freciuencia de actualizacion para que sea mas legible
            freq_str = self._freq_handler(freq)
            unit_str = self._unit_handler(unit)

            # Se crea el diccionario con la metadata de la serie
            series_dict[serie_id] = {'periodicidad': freq_str, 'unidad': unit_str}
        
        return series_dict


    # Función para obtener los datos de una serie desde la API de INEGI
    def get_series_data(self, serie_id:str | list, last_data:bool=False) -> pd.DataFrame:
        """
        Obtiene datos de series económicas y estadísticas desde la API de INEGI (BIE) y los devuelve en un DataFrame de pandas.

        Args:
            serie_id (str | list): El ID de la serie o una lista de IDs de series a consultar desde la API de Banxico. 
                                Si se proporciona un solo ID, puede ser una cadena de texto (str).
            last_data (bool, optional): Si se establece en True, obtendrá solo las últimas observaciones disponibles de la serie.
                                    Por defecto es False.

        Returns:
            pandas.DataFrame: Un DataFrame con las series obtenidas. Las columnas representan las series, y las filas 
                            corresponden a las fechas de observación.
            dict: Un diccionario con informacion de la serie
                            
        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            Obtener la última observación de varias series:
            >>> df, dict = get_BIE_data(['736183','628208'],last_data=True)

        """

        # Definir url de API y realizar la solicitud
        endpoint = self._set_series_params(serie_id, last_data)
        data_json = self._make_request(endpoint=endpoint)

        # Inicializar un DataFrame vacío para almacenar los datos
        series_df = pd.DataFrame()

        for serie_data in data_json['Series']:
        
            # Extraer metadatos
            serie_id = serie_data['INDICADOR']
            freq = int(serie_data['FREQ'])

            # Extraer datos de la serie
            obs = serie_data['OBSERVATIONS']

            # Extraer los valores y las fechas
            obs_values = [float(entry['OBS_VALUE']) for entry in obs]
            time_periods = [entry['TIME_PERIOD'] for entry in obs]

            # Transforma los periodos y frecuencia para que sea mas legible
            time_periods_formatted = self._transform_time_periods(time_periods, freq)

            # Crear una serie de pandas con los datos obtenidos
            serie = pd.Series(obs_values, index=time_periods_formatted, name=serie_id)

            # Agregar la serie al DataFrame
            series_df = pd.concat([series_df, serie], axis=1, join='outer')
        
        
        # Ordenar el DataFrame por fecha
        series_df = series_df.sort_index()

        # Verificar que el indice es del  tipo datetime
        if not pd.api.types.is_datetime64_any_dtype(series_df.index):
            series_df.index = pd.to_datetime(series_df.index)
        
        return series_df

