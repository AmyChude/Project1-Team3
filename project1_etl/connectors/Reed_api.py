from typing import Optional
import requests
from dotenv import load_dotenv
import os
import json
import os
import logging
from pathlib import Path

class Reed:
    def __init__(self, api_key_id: str,api_secret_key: str, cache_dir: Optional[Path] = None):
        self.user = api_key_id
        self.password = api_secret_key
        self.base_url = "https://www.reed.co.uk/api/1.0/search/"
        self.cache_dir = cache_dir
        if self.cache_dir is not None and not isinstance(self.cache_dir, Path):
            self.cache_dir = Path(self.cache_dir)
        self.cache_file = Path("reed_latest.json")
        self.keywords = 'data' # TODO: read from parameter

    def get_jobs(self) -> list[dict]:
        """
        Get the job related to data from reed. 

        Args: 
            api_key_id: sign up to get api_key_id
            api_secret_key : set an empty string
        
        Returns: 
            A list of jobs
        
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
        all_results = []
        url = self.base_url
        resultsToSkip = 0
        data_available = True
        while data_available:
            params ={ 
                'keywords': self.keywords,
                'resultsToTake' :100,
                'resultsToSkip': resultsToSkip
            }

            response = requests.get(url=url,auth=(self.user,self.password),params=params)
            if response.status_code == 200 and response.json().get("results") is not None: 
                page_data = response.json().get("results")
                all_results.extend(page_data)
                resultsToSkip += 100
            elif response.json().get("resutls") is None:
                data_available = False
            else: 
                raise Exception(f"Failed to extract data from Reed API. Status Code: {response.status_code}. Response: {response.text}")
        if cache_path is not None:
                logging.info(f"Saving cache file {cache_path}")
                os.makedirs(self.cache_dir, exist_ok=True)
                with open(cache_path, "w") as f:
                    json.dump(all_results, f)    
        return all_results
        
if __name__=='__main__':
    load_dotenv()
    api_key_id = os.environ.get("REED_USER")
    api_secret_key = os.environ.get("REED_PASSWORD")
    print("loading ...")
    redoo = Reed(api_key_id=api_key_id,api_secret_key=api_secret_key)
    response = redoo.get_jobs()
    print(len(response))