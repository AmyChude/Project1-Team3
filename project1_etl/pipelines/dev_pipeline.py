import logging
import os
from pathlib import Path
from dotenv import load_dotenv

from project1_etl.assets.extract_load_exchange_rates import oer_extract
from project1_etl.assets.data_jobs import extract, transform, extract_postcodes, transform_postcodes
from project1_etl.connectors.exchange_api import OpenExchangeRates
from project1_etl.connectors.postgresql import (
    PostgreSqlClient,
    dataframe_to_column_definitions,
)
from project1_etl.connectors.Reed_api import Reed


logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    load_dotenv()
    
    oer_api_key = os.environ.get("OER_API_KEY")

    reed_api_key = os.environ.get("REED_USER")
    reed_api_secret = os.environ.get("REED_PASSWORD")

    DB_USER = os.getenv("STAGING_DB_USERNAME")
    DB_PASSWORD = os.getenv("STAGING_DB_PASSWORD")
    DB_HOST = os.getenv("STAGING_SERVER_NAME")
    DB_PORT = os.getenv("STAGING_DB_PORT")
    DB_NAME = os.getenv("STAGING_DB_NAME")
    
    JOBS_TABLE_NAME = "jobs"
    EXCHANGE_RATES_TABLE_NAME = "exchange_rates"
    POSTCODES_TABLE_NAME = "postcodes"
    
    sql_client = PostgreSqlClient(
        server_name=DB_HOST,
        database_name=DB_NAME,
        username=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )
    
    logging.info("ETL job started")

    logging.info("Extracting data from Open Exchange Rates API")
    oer_client = OpenExchangeRates(api_key_id=oer_api_key, cache_dir="project1_etl/cache")
    rates_raw = oer_client.get_latest()
    rates_df = oer_extract(rates_raw)
    oer_columns = dataframe_to_column_definitions(rates_df)

    sql_client.create_table_add_pk(
        table_name=EXCHANGE_RATES_TABLE_NAME, columns=oer_columns, drop_if_exists=True
    )
   

    sql_client.insert_data(
        table_name=EXCHANGE_RATES_TABLE_NAME,
        data=rates_df.to_dict(orient="records")
    )
    

    logging.info("Extracting data from Reed API")
    reed_client = Reed(api_key_id=reed_api_key, api_secret_key=reed_api_secret, cache_dir="project1_etl/cache")
    
    jobs_df = extract(reed_client)
    jobs_df = transform(jobs_df)
    
    reed_columns = dataframe_to_column_definitions(jobs_df)
    sql_client.create_table_add_pk(
        table_name=JOBS_TABLE_NAME, columns=reed_columns, drop_if_exists=True
    )
    
    sql_client.insert_data(
        table_name=JOBS_TABLE_NAME,
        data=jobs_df.to_dict(orient="records")
    )
    
    logging.info("Extracting data from postcodes file")
    postcodes_path = Path("project1_etl/data/postcodes/postcodes_uk.zip") # TODO read from config file
    postcodes_df = extract_postcodes(postcodes_path)
    postcodes_df = transform_postcodes(postcodes_df)
    postcodes_columns = dataframe_to_column_definitions(postcodes_df)
    sql_client.create_table_add_pk(
        table_name=POSTCODES_TABLE_NAME, columns=postcodes_columns, drop_if_exists=True
    )
    
    sql_client.insert_data_in_chunks(
        table_name=POSTCODES_TABLE_NAME,
        data=postcodes_df.to_dict(orient="records")
    )

    logging.info("ETL job completed")
