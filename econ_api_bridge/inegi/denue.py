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

    def buscar(
        self, condicion: str, latitud: float, longitud: float, metros: int = 250
    ) -> pd.DataFrame:
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

    def ficha(self, id_establecimiento: int) -> pd.DataFrame:
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

    def nombre(
        self,
        nombre_establecimiento: str,
        entidad_federativa: int = 0,
        registro_inicial: int = 1,
        registro_final: int = 100,
    ) -> pd.DataFrame:
        """
        Método Nombre: Consulta establecimientos por nombre o razón social, acotado opcionalmente por entidad federativa.

        Args:
            nombre_establecimiento (str): Palabra(s) a buscar en el nombre del establecimiento o razón social.
            entidad_federativa (int): Clave de dos dígitos de la entidad federativa (01 a 32). 0 para todas las entidades.
            registro_inicial (int): Número de registro a partir del cuál se mostrarán los resultados.
            registro_final (int): Número de registro final que se mostrará en los resultados.

        Returns:
            pd.DataFrame: Un DataFrame con los establecimientos encontrados.
        """

        # Estructura requerida por DENUE: Nombre/{nombre_establecimiento}/{entidad_federativa}/{registro_inicial}/{registro_final}/{token}
        endpoint = f"/Nombre/{nombre_establecimiento}/{entidad_federativa}/{registro_inicial}/{registro_final}/{self._BaseAPI__api_key}"

        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)

        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df

    def buscar_entidad(
        self,
        condicion: str,
        entidad_federativa: int,
        registro_inicial: int = 1,
        registro_final: int = 100,
    ) -> pd.DataFrame:
        """
        Método BuscarEntidad: Consulta establecimientos acotados por entidad federativa.

        Args:
            condicion (str): Término de búsqueda (ej. 'restaurantes', 'todos').
            entidad_federativa (int): Clave de dos dígitos de la entidad federativa (01 a 32). 0 para todas las entidades.
            registro_inicial (int): Número de registro a partir del cuál se mostrarán los resultados.
            registro_final (int): Número de registro final que se mostrará en los resultados.

        Returns:
            pd.DataFrame: Un DataFrame con los establecimientos encontrados.
        """

        # Estructura requerida por DENUE: BuscarEntidad/{condicion}/{entidad_federativa}/{registro_inicial}/{registro_final}/{token}
        endpoint = f"/BuscarEntidad/{condicion}/{entidad_federativa}/{registro_inicial}/{registro_final}/{self._BaseAPI__api_key}"

        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)

        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df

    def buscar_area_actividad(
        self,
        entidad_federativa: int = 0,
        municipio: int = 0,
        localidad: int = 0,
        ageb: int = 0,
        manzana: int = 0,
        sector: int = 0,
        subsector: int = 0,
        rama: int = 0,
        clase: int = 0,
        nombre_establecimiento: str | int = 0,
        registro_inicial: int = 1,
        registro_final: int = 100,
        id_establecimiento: int = 0,
    ) -> pd.DataFrame:
        """
        Método BuscarAreaAct: Consulta establecimientos acotando la búsqueda por área geográfica, actividad económica, nombre y clave del establecimiento.

        Args:
            entidad_federativa (int): Clave de dos dígitos de la entidad federativa (01 a 32). 0 para todas las entidades.
            municipio (int): Clave de tres dígitos del municipio. 0 para todos los municipios.
            localidad (int): Clave de cuatro dígitos de la localidad. 0 para todas las localidades.
            ageb (int): Clave de cuatro dígitos del AGEB. 0 para todas las AGEBS.
            manzana (int): Clave de tres dígitos de la manzana. 0 para todas las manzanas.
            sector (int): Clave de dos dígitos del sector de la actividad económica. 0 para todos los sectores.
            subsector (int): Clave de tres dígitos del subsector de la actividad económica. 0 para todos los subsectores.
            rama (int): Clave de cuatro dígitos de la rama de la actividad económica. 0 para todas las ramas.
            clase (int): Clave de seis dígitos de la clase de la actividad económica. 0 para todas las clases.
            nombre_establecimiento (str | int): Nombre del establecimiento a buscar. 0 para todos los establecimientos.
            registro_inicial (int): Número de registro a partir del cuál se mostrarán los resultados.
            registro_final (int): Número de registro final que se mostrará en los resultados.
            id_establecimiento (int): Clave única del establecimiento. 0 para todos los establecimientos.

        Returns:
            pd.DataFrame: Un DataFrame con los establecimientos encontrados.
        """

        # Estructura requerida por DENUE: BuscarAreaAct/{entidad}/{municipio}/{localidad}/{ageb}/{manzana}/{sector}/{subsector}/{rama}/{clase}/{nombre}/{registro_inicial}/{registro_final}/{id}/{token}
        endpoint = (
            f"/BuscarAreaAct/{entidad_federativa}/{municipio}/{localidad}/{ageb}/{manzana}/"
            f"{sector}/{subsector}/{rama}/{clase}/{nombre_establecimiento}/{registro_inicial}/"
            f"{registro_final}/{id_establecimiento}/{self._BaseAPI__api_key}"
        )

        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)

        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df

    def buscar_area_actividad_estrato(
        self,
        entidad_federativa: int = 0,
        municipio: int = 0,
        localidad: int = 0,
        ageb: int = 0,
        manzana: int = 0,
        sector: int = 0,
        subsector: int = 0,
        rama: int = 0,
        clase: int = 0,
        nombre_establecimiento: str | int = 0,
        registro_inicial: int = 1,
        registro_final: int = 100,
        id_establecimiento: int = 0,
        estrato: int = 0,
    ) -> pd.DataFrame:
        """
        Método BuscarAreaActEstr: Consulta establecimientos acotando la búsqueda por área geográfica, actividad económica, nombre, clave del establecimiento y estrato.

        Args:
            entidad_federativa (int): Clave de dos dígitos de la entidad federativa (01 a 32). 0 para todas las entidades.
            municipio (int): Clave de tres dígitos del municipio. 0 para todos los municipios.
            localidad (int): Clave de cuatro dígitos de la localidad. 0 para todas las localidades.
            ageb (int): Clave de cuatro dígitos del AGEB. 0 para todas las AGEBS.
            manzana (int): Clave de tres dígitos de la manzana. 0 para todas las manzanas.
            sector (int): Clave de dos dígitos del sector de la actividad económica. 0 para todos los sectores.
            subsector (int): Clave de tres dígitos del subsector de la actividad económica. 0 para todos los subsectores.
            rama (int): Clave de cuatro dígitos de la rama de la actividad económica. 0 para todas las ramas.
            clase (int): Clave de seis dígitos de la clase de la actividad económica. 0 para todas las clases.
            nombre_establecimiento (str | int): Nombre del establecimiento a buscar. 0 para todos los establecimientos.
            registro_inicial (int): Número de registro a partir del cuál se mostrarán los resultados.
            registro_final (int): Número de registro final que se mostrará en los resultados.
            id_establecimiento (int): Clave única del establecimiento. 0 para todos los establecimientos.
            estrato (int): Clave de un dígito del estrato (1 a 7). 0 para todos los tamaños.

        Returns:
            pd.DataFrame: Un DataFrame con los establecimientos encontrados.
        """

        # Estructura requerida por DENUE: BuscarAreaActEstr/{entidad}/{municipio}/{localidad}/{ageb}/{manzana}/{sector}/{subsector}/{rama}/{clase}/{nombre}/{registro_inicial}/{registro_final}/{id}/{estrato}/{token}
        endpoint = (
            f"/BuscarAreaActEstr/{entidad_federativa}/{municipio}/{localidad}/{ageb}/{manzana}/"
            f"{sector}/{subsector}/{rama}/{clase}/{nombre_establecimiento}/{registro_inicial}/"
            f"{registro_final}/{id_establecimiento}/{estrato}/{self._BaseAPI__api_key}"
        )

        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)

        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df

    def cuantificar(
        self,
        actividad_economica: str | int | list = 0,
        area_geografica: str | int | list = 0,
        estrato: int = 0,
    ) -> pd.DataFrame:
        """
        Método Cuantificar: Realiza un conteo de establecimientos acotando la búsqueda por área geográfica, actividad económica y estrato.

        Args:
            actividad_economica (str | int | list): Clave(s) de dos a seis dígitos de la actividad económica. 0 para todas las actividades.
            area_geografica (str | int | list): Clave(s) de dos a nueve dígitos del área geográfica. 0 para todo el país.
            estrato (int): Clave de un dígito del estrato (1 a 7). 0 para todos los tamaños.

        Returns:
            pd.DataFrame: Un DataFrame con el conteo de establecimientos.
        """

        # Las claves múltiples se separan con coma
        if isinstance(actividad_economica, list):
            actividad_economica = ",".join(
                str(actividad) for actividad in actividad_economica
            )
        if isinstance(area_geografica, list):
            area_geografica = ",".join(str(area) for area in area_geografica)

        # Estructura requerida por DENUE: Cuantificar/{actividad_economica}/{area_geografica}/{estrato}/{token}
        endpoint = f"/Cuantificar/{actividad_economica}/{area_geografica}/{estrato}/{self._BaseAPI__api_key}"

        # Realizar la solicitud HTTP usando el método de la clase base
        data_json = self._make_request(endpoint=endpoint)

        # Retornar DataFrame
        df = pd.DataFrame(data_json) if data_json else pd.DataFrame()

        return df
