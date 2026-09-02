"""
Description:   Class to interact with the DENUE API of INEGI. Provides methods to search for establishments and retrieve their details.
Author:        David Jiménez Cooper - SpiderCoop
Date:          2026-09-02
"""

import pandas as pd
from econ_api_bridge.baseapi.baseapi import BaseAPI


class INEGI_DENUE(BaseAPI):
    def __init__(self, api_key):
        super().__init__(api_key, "https://www.inegi.org.mx/app/api/denue/v1/consulta")


    def buscar(self, condicion:str, latitud:float, longitud:float, metros:int = 250) -> pd.DataFrame:
        """
        Método Buscar: Consulta establecimientos dentro de un radio específico.

        Args:
            condicion (str): Término de búsqueda (ej. 'restaurantes', 'camiones', 'todos').
            latitud (float): Latitud del punto central de la búsqueda.
            longitud (float): Longitud del punto central de la búsqueda.
            metros (int): Radio de búsqueda en metros (máximo permitido: 5000).

        Returns:
            pd.DataFrame: Un DataFrame con los establecimientos encontrados.
        """
        
        # Estructura requerida por DENUE: Buscar/{condicion}/{latitud},{longitud}/{metros}/{token}
        endpoint = f"/Buscar/{condicion}/{latitud},{longitud}/{metros}/{self._BaseAPI__api_key}?type=json"
        
        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)
        
        # Retornar como DataFrame, o un DataFrame vacío en caso de que no haya resultados
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df
    

    def ficha(self, id_establecimiento:int) -> pd.DataFrame:
        """
        Método Ficha: Obtiene los detalles y metadatos de un establecimiento en específico.

        Args:
            id_establecimiento (int): El número de Identificación (ID) del establecimiento.

        Returns:
            pd.DataFrame: Un DataFrame con la información del establecimiento consultado.
        """
        
        # Estructura requerida por DENUE: Ficha/{id_establecimiento}/{token}
        endpoint = f"/Ficha/{id_establecimiento}/{self._BaseAPI__api_key}"
        
        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)
        
        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df
    
    def buscar_entidad(self, condicion:str, entidad_federativa:int, registro_inicial:int=0, registro_final:int|None=None) -> pd.DataFrame:
        """
        Método Ficha: Obtiene los detalles y metadatos de un establecimiento en específico.

        Args:
            id_establecimiento (int): El número de Identificación (ID) del establecimiento.

        Returns:
            pd.DataFrame: Un DataFrame con la información del establecimiento consultado.
        """
        
        # Estructura requerida por DENUE: Ficha/{id_establecimiento}/{token}
        endpoint = f"/BuscarEntidad/{condicion}/{entidad_federativa}/{registro_inicial}/{registro_final}/{self._BaseAPI__api_key}"
        
        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)
        
        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df
    
    def buscar_avanzado(self, entidad_federativa:int = 00, municipio:int = 0, localidad:int = 0, ageb:int = 0, manzana:int = 0, sector:int = 0, subsector:int = 0, rama:int = 0, clase:int = 0, nombre_establecimiento:str|int = 0, clave_establecimiento:int = 0, estrato:int = 0, registro_inicial:int=0, registro_final:int|None=None) -> pd.DataFrame:
        """
        Método Ficha: Obtiene los detalles y metadatos de un establecimiento en específico.

        Args:
            id_establecimiento (int): El número de Identificación (ID) del establecimiento.

        Returns:
            pd.DataFrame: Un DataFrame con la información del establecimiento consultado.
        """
        
        # Estructura requerida por DENUE: Ficha/{id_establecimiento}/{token}
        endpoint = f"/BuscarEntidad/{condicion}/{entidad_federativa}/{registro_inicial}/{registro_final}/{self._BaseAPI__api_key}"
        
        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)
        
        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df