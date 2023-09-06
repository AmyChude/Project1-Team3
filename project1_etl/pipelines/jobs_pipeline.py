from dotenv import load_dotenv
from typing import Optional
from pathlib import Path
import schedule 
import time

from project1_etl.tools.config import get_env_variable, load_pipeline_config
from project1_etl.tools.schedule import Counter
from project1_etl.assets.extract_load_exchange_rates import oer_extract
from project1_etl.assets.data_jobs import extract, transform, extract_postcodes, transform_postcodes
from project1_etl.assets.transform import load, extract_from_staging_df
from project1_etl.connectors.exchange_api import OpenExchangeRates
from project1_etl.connectors.postgresql import (
    PostgreSqlClient,
    dataframe_to_column_definitions,
)
from project1_etl.connectors.Reed_api import Reed
from project1_etl.assets.metadata_logger import MetaDataLogger
from project1_etl.assets.pipeline_logger import PipelineLogger

from jinja2 import Environment, FileSystemLoader


def jobs_pipeline(pipeline_name: str, config: dict, pipeline_logger: PipelineLogger):
    pipeline_logger.logger.info(f"Pipeline {pipeline_name} started")

    pipeline_logger.logger.info("Getting pipeline config from environment variables")
    oer_api_key = get_env_variable("OER_API_KEY")

    reed_api_key = get_env_variable("REED_USER")
    reed_api_secret = get_env_variable("REED_PASSWORD")

    STAGING_DB_USER = get_env_variable("STAGING_DB_USERNAME")
    STAGING_DB_PASSWORD = get_env_variable("STAGING_DB_PASSWORD")
    STAGING_DB_HOST = get_env_variable("STAGING_SERVER_NAME")
    STAGING_DB_PORT = get_env_variable("STAGING_DB_PORT")
    STAGING_DB_NAME = get_env_variable("STAGING_DB_NAME")

    SERVING_DB_USER = get_env_variable("SERVING_DB_USERNAME")
    SERVING_DB_PASSWORD = get_env_variable("SERVING_DB_PASSWORD")
    SERVING_DB_HOST = get_env_variable("SERVING_SERVER_NAME")
    SERVING_DB_PORT = get_env_variable("SERVING_DB_PORT")
    SERVING_DB_NAME = get_env_variable("SERVING_DB_NAME")

    LOAD_POSTCODES = get_env_variable("LOAD_POSTCODES")
    if LOAD_POSTCODES.lower() == "true":
        LOAD_POSTCODES = True
    else:
        LOAD_POSTCODES = False
    
    pipeline_logger.logger.info("Setting up SQL clients")
    staging_sql_client = PostgreSqlClient(
        server_name=STAGING_DB_HOST,
        database_name=STAGING_DB_NAME,
        username=STAGING_DB_USER,
        password=STAGING_DB_PASSWORD,
        port=STAGING_DB_PORT,
    )

    serving_sql_client = PostgreSqlClient(
        server_name=SERVING_DB_HOST,
        database_name=SERVING_DB_NAME,
        username=SERVING_DB_USER,
        password=SERVING_DB_PASSWORD,
        port=SERVING_DB_PORT,
    )

    JOBS_TABLE_NAME = "jobs"
    EXCHANGE_RATES_TABLE_NAME = "exchange_rates"
    POSTCODES_TABLE_NAME = "postcodes"

    pipeline_logger.logger.info("Extracting data from Open Exchange Rates API")
    oer_client = OpenExchangeRates(api_key_id=oer_api_key, cache_dir="project1_etl/cache") # TODO: move cache dir to config
    rates_raw = oer_client.get_latest()
    rates_df = oer_extract(rates_raw)
    oer_columns = dataframe_to_column_definitions(rates_df)

    staging_sql_client.create_table_add_pk(
        table_name=EXCHANGE_RATES_TABLE_NAME, columns=oer_columns, drop_if_exists=True
    )
   

    staging_sql_client.insert_data(
        table_name=EXCHANGE_RATES_TABLE_NAME,
        data=rates_df.to_dict(orient="records")
    )

    pipeline_logger.logger.info("Extracting data from Reed API")
    reed_client = Reed(api_key_id=reed_api_key, api_secret_key=reed_api_secret, cache_dir="project1_etl/cache")
    
    jobs_df = extract(reed_client)
    jobs_df = transform(jobs_df)
    
    reed_columns = dataframe_to_column_definitions(jobs_df)
    staging_sql_client.create_table_add_pk(
        table_name=JOBS_TABLE_NAME, columns=reed_columns, drop_if_exists=True
    )
    
    staging_sql_client.insert_data(
        table_name=JOBS_TABLE_NAME,
        data=jobs_df.to_dict(orient="records")
    )

    if LOAD_POSTCODES:
        pipeline_logger.logger.info("Extracting data from postcodes file")
        postcodes_path = Path("project1_etl/data/postcodes/postcodes_uk.zip") # TODO read from config file
        postcodes_df = extract_postcodes(postcodes_path)
        postcodes_df = transform_postcodes(postcodes_df)
        postcodes_columns = dataframe_to_column_definitions(postcodes_df)
        staging_sql_client.create_table_add_pk(
            table_name=POSTCODES_TABLE_NAME, columns=postcodes_columns, drop_if_exists=True
        )
        
        staging_sql_client.insert_data_in_chunks(
            table_name=POSTCODES_TABLE_NAME,
            data=postcodes_df.to_dict(orient="records")
        )
    else:
        pipeline_logger.logger.info("Skipping postcodes data extraction. Enable by setting LOAD_POSTCODES environment variable to 'True'")

    pipeline_logger.logger.info("Extracting and loading data to staging database completed")

    pipeline_logger.logger.info("Transforming data")
    environment = Environment(loader=FileSystemLoader("project1_etl/assets/sql")) # TODO: move sql folder to config
    
    for sql_path in environment.list_templates():
        pipeline_logger.logger.info(f"Transforming data for {sql_path}")
        sql_template = environment.get_template(sql_path)
        target_table_name = sql_template.make_module().config.get("target_table_name")
        sql = sql_template.render()
        transformed_df = extract_from_staging_df(
            sql=sql,
            engine=staging_sql_client.engine
        )
        transform_columns = dataframe_to_column_definitions(transformed_df)
        serving_sql_client.create_table_add_pk(
            table_name=target_table_name, columns=transform_columns, drop_if_exists=True)
        serving_sql_client.insert_data_in_chunks(target_table_name, transformed_df.to_dict(orient="records"))


    pipeline_logger.logger.info("Transforming data completed")

def pipeline_run(pipeline_name: str, postgresql_logging_client: PostgreSqlClient, pipeline_config: dict,
                 run_counter: Optional[Counter] = None):
    """Run a pipeline and log the results"""
    if run_counter is not None:
        run_counter.increment()
    pipeline_logger = PipelineLogger(pipeline_name=pipeline_name, log_folder_path=pipeline_config.get("config").get("log_folder_path"))
    metadata_logger = MetaDataLogger(
        pipeline_name=pipeline_name, 
        postgresql_client=postgresql_logging_client,
        pipeline_config=pipeline_config.get("config")
    )
    try: 
        metadata_logger.log_start() # log start
        jobs_pipeline(pipeline_name=pipeline_name, config=pipeline_config, pipeline_logger=pipeline_logger)
        metadata_logger.log_success(logs=pipeline_logger.get_logs()) # log end
        pipeline_logger.clear()
    except BaseException as e:
        pipeline_logger.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log_failure(logs= pipeline_logger.get_logs()) # log error
        pipeline_logger.clear()


if __name__ == "__main__":
    load_dotenv()
    LOGGING_SERVER_NAME = get_env_variable("LOGGING_SERVER_NAME")
    LOGGING_DB_NAME = get_env_variable("LOGGING_DB_NAME")
    LOGGING_DB_USERNAME = get_env_variable("LOGGING_DB_USERNAME")
    LOGGING_DB_PASSWORD = get_env_variable("LOGGING_DB_PASSWORD")
    LOGGING_DB_PORT = get_env_variable("LOGGING_DB_PORT")
    
    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DB_NAME,
        username=LOGGING_DB_USERNAME,
        password=LOGGING_DB_PASSWORD,
        port=LOGGING_DB_PORT
    )

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    pipeline_config = load_pipeline_config(yaml_file_path)
    PIPELINE_NAME = pipeline_config.get("name")

    POLL_SECONDS = pipeline_config.get("schedule").get("poll_seconds")
    RUN_SECONDS = pipeline_config.get("schedule").get("run_seconds")
    MAX_RUNS = pipeline_config.get("schedule").get("max_runs")

    run_counter = Counter()
    schedule.every(RUN_SECONDS).seconds.do(pipeline_run, pipeline_name=PIPELINE_NAME, postgresql_logging_client=postgresql_logging_client, pipeline_config=pipeline_config, run_counter=run_counter)
    while run_counter.get_count() < MAX_RUNS: 
        schedule.run_pending()
        time.sleep(POLL_SECONDS)