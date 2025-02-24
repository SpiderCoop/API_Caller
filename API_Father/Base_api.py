import requests

class Base_api:
    def __init__(self,api_key, base_url):
        self.__api_key = api_key
        self.base_url = base_url

    def _make_request(self, endpoint, headers=None, params=None):
        # Construir la URL
        url = self.base_url + endpoint

        if headers is None:
            headers = {}
        if params is None:
            params = {}
        
        # Realizar la solicitud GET
        response = requests.get(url, headers=headers, params=params)

        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception (f"Error: {response.status_code} - {response.text}")