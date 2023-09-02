from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, DateTime
from sqlalchemy.engine import URL
import pandas as pd
import logging
from tqdm import tqdm

class PostgreSqlClient:
    def __init__(self, 
                server_name: str, 
                database_name: str, 
                username: str, 
                password: str, 
                port: int = 5432
        ):  
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername = "postgresql+pg8000", 
            username = username,
            password = password,
            host = server_name, 
            port = port,
            database = database_name, 
        )

        self.engine = create_engine(connection_url)
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect() # get target table schemas into metadata object 

    def drop_table(self, table_name: str):
        if self.engine.has_table(table_name):
            self.metadata.tables[table_name].drop(self.engine)
            self.metadata.remove(self.metadata.tables[table_name])
            logging.info(f"Table {table_name} dropped")
        else:
            logging.info(f"Table {table_name} does not exist")

    def create_table_add_pk(self, table_name: str, columns: dict, drop_if_exists: bool = False):
        """
        Create a table in the database. Additional primary key column named 'id' will be added to the table.
        """
        if drop_if_exists:
            self.drop_table(table_name)
        if self.engine.has_table(table_name):
            logging.info(f"Table {table_name} already exists")
        else:
            table = Table(table_name, self.metadata, Column('id', Integer, primary_key=True))
            for column_name, column_type in columns.items():
                table.append_column(Column(column_name, column_type))
            table.create()
            logging.info(f"Table {table_name} created")

    def insert_data(self, table_name: str, data: list[dict]) -> int:
        """
        Insert data into a table. 
        """                                        
        table = self.metadata.tables[table_name]
        with self.engine.connect() as conn:
            result = conn.execute(table.insert().values(data))
            logging.info(f"{result.rowcount} rows inserted into {table_name}")
            return result.rowcount

    def insert_data_in_chunks(self, table_name: str, data: list[dict], chunk_size: int = 1000) -> int:
        """
        Insert data into a table in chunks. 
        """                                        
        table = self.metadata.tables[table_name]
        with self.engine.connect() as conn:
            inserted_rows = 0
            data_lenght = len(data)
            logging.info(f"Inserting {data_lenght} rows into {table_name} in chunks of {chunk_size} rows")
            for chunk_start in tqdm(range(0, len(data), chunk_size)):
                chunk_end = chunk_start + chunk_size
                chunk_end = min(chunk_end, data_lenght)
                chunk = data[chunk_start:chunk_end]
                result = conn.execute(table.insert().values(chunk))
                inserted_rows += result.rowcount
        logging.info(f"{inserted_rows} rows inserted into {table_name}")
        return inserted_rows

    def get_data(self, table_name: str) -> list[dict]:
        """
        Get data from a table. 
        """
        table = self.metadata.tables[table_name]
        with self.engine.connect() as conn:
            result = conn.execute(table.select())
            return [dict(row) for row in result]
        

def dataframe_to_column_definitions(df: pd.DataFrame) -> dict:
    """
    Convert a dataframe to a dictionary of column definitions. 
    """
    column_types = dict()
    for column_name in df.columns:
        column_type = df[column_name].dtype
        if column_type == "object":
            column_types[column_name] = String
        elif column_type == "int64":
            column_types[column_name] = Integer
        elif column_type == "float64":
            column_types[column_name] = Float
        elif column_type == "datetime64[ns]":
            column_types[column_name] = DateTime
        else:
            raise Exception(f"Unsupported data type {column_type} for column {column_name}")
    return column_types
