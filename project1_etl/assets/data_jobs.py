import pandas as pd

print("__file__ value:", __file__)
from project1_etl.connectors.Reed_api import Reed
import os 
from dotenv import load_dotenv

def extract(reed : Reed):
    data = reed.get_jobs()
    df = pd.json_normalize(data=data)
    df.to_excel("Project1-Team3\project1_etl\data.xlsx")
    return df

def transform():
    df = pd.read_excel("Project1-Team3\project1_etl\data.xlsx")
    return df

if __name__ == '__main__':
    load_dotenv()
    api_key_id = os.environ.get("REED_USER")
    api_secret_key = os.environ.get("REED_PASSWORD")
    print("loading ...")
    reed = Reed(api_key_id=api_key_id,api_secret_key=api_secret_key)
    df = extract(reed= reed)