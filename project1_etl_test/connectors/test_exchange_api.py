from dotenv import load_dotenv
from project1_etl.connectors.exchange_api import OpenExchangeRates
import os 
import pytest


@pytest.fixture
def setup():
    load_dotenv()

def test_get_jobs(setup):
    API_KEY_ID = os.environ.get("OER_API_KEY")
    print("loading ...")
    open_exchange = OpenExchangeRates(api_key_id=API_KEY_ID)
    data = open_exchange.get_latest()    
    assert type(data) == dict
    assert len(data) > 0