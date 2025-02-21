# -*- coding: utf-8 -*-
"""
Created on Thu Mar 28 21:20:28 2024

@author: DJIMENEZ

"""
# Librerias necesarias -------------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, date
from dotenv import load_dotenv
import requests
import json

if __name__ == '__main__':
    from API_Base import API_Base
    
else:
    from .API_Base import API_Base

# Clase -------------------------------------------------------------------------

class API_INEGI(API_Base):
    def __init__(self, api_key):
        super().__init__(api_key, "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml")

    # Funcion para cambiar la presentacion de los periodos de tiempo de la serie de acuerdo con las especificacionesde la metadata de la API de INEGI
    def _freq_handler(self, time_periods:list, frequency_id:int):
        """
        Recibe los datos de las fechas o periodos de la serie y, de acuerdo con la metadata y descripcion revisada desde la API de INEGI, devuelve las fechas correspondientes en tipo datetime.

        Args:

            time_periods (list): Los periodos de tiempo de la serie.
            frequency_id (int): El ID de la frecuencia de la serie.

        Returns:
            datetime.date: Una lista de objetos datetime.date con las fechas correspondientes a los periodos de tiempo de la serie.
            string: Un objeto string con la descripcion de la frecuencia de la serie.
                            
        Raises:
            Exception: Si la solicitud a la API de Banxico falla, devuelve un mensaje con el código de error y la respuesta.

        Example:
            Obtener la última observación de varias series:
            >>> df, dict = get_BIE_data(['736183','628208'],last_data=True)

        """

        # Definir url de API
        endpoint = f"/CL_FREQ/{frequency_id}/es/BIE/2.0/{self.api_key}?type=json"

        # Extraer y convertir datos
        data_json = self._make_request(endpoint=endpoint)
        serie_data = data_json['CODE'][0]
        freq_str = serie_data['Description']

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

        return time_periods_formatted, freq_str


    def _unit_handler(self, unit_id:int):
        
        # Definir url de API
        endpoint = f"/CL_UNIT/{unit_id}/es/BIE/2.0/{self.api_key}?type=json"

        # Extraer y convertir datos 
        data_json = self._make_request(endpoint=endpoint)
        serie_data = data_json['CODE'][0]
        unit_str = serie_data['Description']

        return unit_str


    # Función para obtener los datos de una serie desde la API de INEGI
    def get_data(self, serie_id:str | list, last_data:bool=False) -> tuple[pd.DataFrame, dict]:
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
        endpoint = f"/INDICATOR/{','.join(serie_id)}/es/0700/{last_data}/BIE/2.0/{self.api_key}?type=json"

        # Inicializar un DataFrame vacío para almacenar los datos y un diccionario para almacenar los metadatos
        series_df = pd.DataFrame()
        series_dict = {}

        # Extraer y convertir datos
        data_json = self._make_request(endpoint=endpoint)

        for serie_data in data_json['Series']:
        
            # Extraer metadatos
            serie_id = serie_data['INDICADOR']
            freq = int(serie_data['FREQ'])
            unit = int(serie_data['UNIT'])

            # Extraer datos de la serie
            obs = serie_data['OBSERVATIONS']

            # Extraer los valores y las fechas
            obs_values = [float(entry['OBS_VALUE']) for entry in obs]
            time_periods = [entry['TIME_PERIOD'] for entry in obs]

            # Transforma los periodos y frecuencia para que sea mas legible
            time_periods_formatted, freq_str = self._freq_handler(time_periods, freq)

            # Tranforma la unidad de medida para que sea mas legible
            unit_str = self._unit_handler(unit)

            # Se crea el diccionario con la metadata de la serie
            series_dict[serie_id] = {'periodicidad': freq_str, 'unidad': unit_str}

            # Crear una serie de pandas con los datos obtenidos
            serie = pd.Series(obs_values, index=time_periods_formatted, name=serie_id)

            
            # Agregar la serie al DataFrame
            series_df = pd.concat([series_df, serie], axis=1, join='outer')
        
        
        # Ordenar el DataFrame por fecha
        series_df = series_df.sort_index()
        
        return series_df, series_dict



# Ejemplo de uso

if __name__ == '__main__':

    # Carga variables de un archivo .env (para almacenar el token de la API de INEGI)
    load_dotenv()

    # Token
    INEGI_Token = os.environ.get("INEGI_Token")

    inegi_api = API_INEGI(INEGI_Token)

    # IDs 736181, 454527, 628208, 736183 (PIB constante 2018 desestacionalizado var anual)
    serie, serie_info = inegi_api.get_data(serie_id=['736183','628208'],last_data=False)
    print(serie)
    print('\n')
    print(serie_info)
    print('\n')
    print(serie.loc[pd.date_range(start='2024-01-01', end='2024-12-01', freq='QS-MAR').date,'736183'])

    print('\n')

