from dotenv import load_dotenv
from project1_etl.connectors.Reed_api import Reed
import os 
import pytest


@pytest.fixture
def setup():
    load_dotenv()

def test_get_jobs(setup):
    API_KEY_ID = os.environ.get("REED_USER")
    API_SECRET_KEY = os.environ.get("REED_PASSWORD")
    print("loading ...")
    redoo = Reed(api_key_id=API_KEY_ID,api_secret_key=API_SECRET_KEY,cache_dir='project1_etl_test/cache')
    data = redoo.get_jobs()    
    assert type(data) == list
    assert len(data) > 0