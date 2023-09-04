from enum import Enum
from project1_etl.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Column, Integer, String, JSON
from sqlalchemy import select, func
from datetime import datetime

class PipelineStatus(Enum):
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class MetaDataLogger:
    def __init__(self, pipeline_name: str, postgresql_client: PostgreSqlClient, pipeline_config: dict, table_name: str = "pipeline_metadata"):
        self.pipeline_name = pipeline_name
        self.postgresql_client = postgresql_client
        self.table_name = table_name
        self.pipeline_config = pipeline_config
        self.run_id = None
        self._init_table()

    def _init_table(self):
        columns = [
            Column("pipeline_name", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("timestamp", String, primary_key=True),
            Column("status", String, primary_key=True),
            Column("config", JSON),
            Column("logs", String)
        ]
        self.postgresql_client.create_table(table_name=self.table_name, columns=columns, drop_if_exists=False)

    def _log(self, status: PipelineStatus, logs: str = None):
        self.postgresql_client.insert_data(
            table_name=self.table_name,
            data={
                "pipeline_name": self.pipeline_name,
                "run_id": self.run_id,
                "timestamp": datetime.now(),
                "status": status.value,
                "config": self.pipeline_config,
                "logs": logs
            }
        )

    def _get_run_id(self) -> int:
        logging_table = self.postgresql_client.get_table(table_name=self.table_name)
        latest_run_id = self.postgresql_client.engine.execute(
            select(
                func.max(
                    logging_table.c.run_id
                )
            ).where(logging_table.c.pipeline_name == self.pipeline_name)
        ).first()[0]
        run_id = 1
        if latest_run_id is not None:
            run_id = latest_run_id + 1
        return run_id


    def log_start(self):
        if self.run_id is not None:
            raise Exception("Run ID already set. Call log_start() only once")
        else:
            self.run_id = self._get_run_id()
        self._log(status=PipelineStatus.STARTED)

    def log_success(self, logs: str = None):
        if self.run_id is None:
            raise Exception("Run ID not set. Call log_start() first")
        self._log(status=PipelineStatus.SUCCESS, logs=logs)

    def log_failure(self, logs: str = None):
        if self.run_id is None:
            raise Exception("Run ID not set. Call log_start() first")
        self._log(status=PipelineStatus.FAILED, logs=logs)