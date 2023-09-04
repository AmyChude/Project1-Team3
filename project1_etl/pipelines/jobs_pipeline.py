from dotenv import load_dotenv
from typing import Optional
import schedule 
import time

from project1_etl.tools.config import get_env_variable, load_pipeline_config
from project1_etl.tools.schedule import Counter
from project1_etl.connectors.postgresql import PostgreSqlClient
from project1_etl.assets.metadata_logging import MetaDataLogger


def pipeline_run(pipeline_name: str, postgresql_logging_client: PostgreSqlClient, pipeline_config: dict,
                 run_counter: Optional[Counter] = None):
    """Run a pipeline and log the results"""
    if run_counter is not None:
        run_counter.increment()
    # pipeline_logging = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=pipeline_config.get("config").get("log_folder_path"))
    metadata_logger = MetaDataLogger(
        pipeline_name=pipeline_name, 
        postgresql_client=postgresql_logging_client,
        pipeline_config=pipeline_config.get("config")
    )
    try: 
        metadata_logger.log_start() # log start
        # pipeline(config=pipeline_config.get("config"), pipeline_logging=pipeline_logging)
        metadata_logger.log_success(logs="success") #pipeline_logging.get_logs()) # log end
        # pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        # pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log_failure(logs="fail") # pipeline_logging.get_logs()) # log error
        # pipeline_logging.logger.handlers.clear()


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