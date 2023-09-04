from project1_etl.connectors.postgresql import *
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData
import pandas as pd

@pytest.fixture
def setup_postgresql_client():
    load_dotenv()
    SERVER_NAME = os.environ.get("STAGING_SERVER_NAME")
    DATABASE_NAME = os.environ.get("STAGING_DB_NAME")
    DB_USERNAME = os.environ.get("STAGING_DB_USERNAME")
    DB_PASSWORD = os.environ.get("STAGING_DB_PASSWORD")
    PORT = os.environ.get("STAGING_DB_PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    return postgresql_client

def test_insert_data(setup_postgresql_client):
    table_name='test'
    data = pd.DataFrame([
        {"col1": 1, "value": "hello"},
        {"col1": 2, "value": "world"}
             ])
    columns = dataframe_to_column_definitions(data)

    setup_postgresql_client.create_table_add_pk(
        table_name=table_name, columns=columns, drop_if_exists=True
    )
   

    setup_postgresql_client.insert_data(
        table_name=table_name,
        data=data.to_dict(orient="records")
    )
    result = setup_postgresql_client.get_data(table_name)
    assert type(result) == list
    assert len(result) == 2 
