"""
Description:   Base class for making API requests.
Author:        David Jiménez Cooper - SpiderCoop
Date:          2026-09-02
"""

import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class BaseAPI:
    def __init__(self, api_key: str = None, base_url: str = "", timeout: int = 10):

        self.__api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        logging.basicConfig(level=logging.INFO)

    def _make_request(self, endpoint, headers=None, params=None, data=None, json=None):

        url = f"{self.base_url}{endpoint}"

        if headers is None:
            headers = {}
        if params is None:
            params = {}

        if self._BaseAPI__api_key:
            headers["Authorization"] = f"Bearer {self._BaseAPI__api_key}"

        try:
            response = self.session.request(
                method="GET",
                url=url,
                headers=headers,
                params=params,
                data=data,
                json=json,
                timeout=self.timeout,
            )
            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
            raise
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err}")
            raise
        except ValueError as json_err:
            logging.error(f"JSON decode error: {json_err}")
            raise
