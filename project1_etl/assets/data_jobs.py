import pandas as pd
import os
import sys
from connectors.Reed_api import Reed
 
from dotenv import load_dotenv

def extract(reed : Reed) -> pd.DataFrame:
    data = reed.get_jobs()
    df = pd.json_normalize(data=data)
    #df.to_csv("data.csv") # just added this to do transformation utilising csv instead of calling extract function  in order avoid getting max retries when calling API too many time
    return df

def transform(df:pd.DataFrame) ->pd.DataFrame:
    """
    Transform Reed results:
        - drop duplicated in case there is any due to pagination
        - drop the the rows that contains Nan is these columns 'locationName', 'minimumSalary','jobTitle'
        - filter data to only keep those that contains 'Data' or 'engineer' in both JobTile and jobDescription

    Args: 
        df: Reed api results
        
    
    Returns: 
        a dataframe with clean data
 
    """
    df = pd.read_csv("data.csv")
    df.drop_duplicates(inplace=True) 
    if df.isna().any().any():
        df.dropna(subset=['locationName', 'minimumSalary','jobTitle'],inplace=True)
    df = df[(df['jobTitle'].str.contains('data|engineer',regex=True,case=False)) & (df['jobDescription'].str.contains('data|engineer',regex=True,case=False))].reset_index(drop=True)
    return df

if __name__ == '__main__':
    print("Current working directory:", os.getcwd())
    load_dotenv()
    api_key_id = os.environ.get("REED_USER")
    api_secret_key = os.environ.get("REED_PASSWORD")
    print("loading ...")
    reed = Reed(api_key_id=api_key_id,api_secret_key=api_secret_key)
    df = extract(reed= reed)
    df_final = transform(df=df)
    print(df_final)