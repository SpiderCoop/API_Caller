"""
Description:   Class to interact with the World Bank API. Provides methods to retrieve series data for specific indicators and countries.
Author:        David Jiménez Cooper - SpiderCoop
Date:          2026-09-02
"""

import pandas as pd

from econ_api_bridge.baseapi.baseapi import BaseAPI


class WorldBank(BaseAPI):
    def __init__(self, api_key):
        super().__init__(api_key, "https://api.worldbank.org/v2")

    def get_series_data(self, indicator_id, country_code, start_date, end_date):
        url = (
            f"{self._BaseAPI__base_url}/country/{country_code}/indicator/{indicator_id}"
        )
        params = {
            "format": "json",
            "date": f"{start_date}:{end_date}",
            "per_page": 100,
            "api_key": self._BaseAPI__api_key,
        }

        response = self._make_request(url, params=params)
        data = response.json()

        # Extract data and metadata
        data_list = data[1]
        metadata = data[0]

        # Convert data to dataframe
        df = pd.DataFrame(data_list)
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"])

        return df, metadata
