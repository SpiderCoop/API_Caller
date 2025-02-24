
# Librerias necesarias -------------------------------------------------------------------------

import requests
import pandas as pd

from ..baseapi.baseapi import BaseAPI

# Clase ---------------------------------------------------------------------------------------

class WorldBank:
    def __init__(self, api_key):
        super().__init__(api_key, "https://api.worldbank.org/v2")

    def get_data(self, indicator_id, country_code, start_date, end_date):
        url = f'{self.base_url}/country/{country_code}/indicator/{indicator_id}'
        params = {
            'format': 'json',
            'date': f'{start_date}:{end_date}',
            'per_page': 100,
            'api_key': self.api_key
        }

        response = requests.get(url, params=params)
        data = response.json()

        # Extract data and metadata
        data_list = data[1]
        metadata = data[0]

        # Convert data to dataframe
        df = pd.DataFrame(data_list)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'])

        return df, metadata