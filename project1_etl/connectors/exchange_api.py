import requests
from pathlib import Path
from typing import Optional
import json
import os
import logging

class OpenExchangeRates:
    def __init__(self, api_key_id: str, cache_dir: Optional[Path] = None):
        self.api_key_id = api_key_id
        self.base_url = "https://openexchangerates.org/api/latest.json"
        self.base_currency = "USD" # only USD is supported for free accounts
        self.cache_dir = cache_dir
        if self.cache_dir is not None and not isinstance(self.cache_dir, Path):
            self.cache_dir = Path(self.cache_dir)
        self.cache_file = Path("oer_latest.json")

    def get_latest(self) -> dict:
        """
        Get the latest currency exchange rates from OpenExchangeRates. 

        Args: 
            api_key_id: sign up to get api_key_id
            api_secret_key : set an empty string
        
        Returns: 
            A dictionary of exchange rates
        
        Raises:
            Exception if response code is not 200. 
        """
        cache_path = None
        if self.cache_dir is not None:
            logging.info(f"Using cache directory {self.cache_dir}")
            cache_path = self.cache_dir / self.cache_file
            if cache_path.exists():
                logging.info(f"Using cache file {cache_path}")
                with open(cache_path, "r") as f:
                    return json.load(f)
        params ={ 
            'app_id':self.api_key_id
        }
        response = requests.get(url=self.base_url,params=params)
        if response.status_code == 200: 
            response_json = response.json()
            logging.info(f"Extracted data from OpenExchangeRates API. Status Code: {response.status_code}")
            if cache_path is not None:
                logging.info(f"Saving cache file {cache_path}")
                os.makedirs(self.cache_dir, exist_ok=True)
                with open(cache_path, "w") as f:
                    json.dump(response_json, f)
            return response.json()
        else: 
            logging.error(f"Failed to extract data from OpenExchangeRates API. Status Code: {response.status_code}. Response: {response.text}")
            raise Exception(f"Failed to extract data from OpenExchangeRates API. Status Code: {response.status_code}. Response: {response.text}")