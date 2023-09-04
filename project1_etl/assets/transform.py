from dotenv import load_dotenv
import os 
from sqlalchemy.engine import URL, Engine
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy.dialects import postgresql
from jinja2 import Environment, FileSystemLoader

def extract_from_staging(
        sql: str, 
        engine: Engine
    ) -> list[dict]:     
    return [dict(row) for row in engine.execute(sql).all()]

def get_schema_metadata(engine: Engine) -> Table:
    metadata = MetaData(bind=engine)
    metadata.reflect() # get target table schemas into metadata object 
    return metadata

def _create_table(table_name: str, metadata: MetaData, engine: Engine):
    existing_table = metadata.tables[table_name]
    new_metadata = MetaData()
    columns = [Column(column.name, 
                        column.type, 
                        primary_key=column.primary_key
        ) for column in existing_table.columns]
    new_table = Table(
        table_name, 
        new_metadata,
        *columns    
    )
    new_metadata.create_all(bind = engine)
    return new_metadata

def load(
        data: list[dict],
        table_name: str, 
        engine: Engine,
        source_metadata: MetaData
    ): 
    target_metadata = _create_table(table_name=table_name, metadata=source_metadata, engine=engine) 
    table = target_metadata.tables[table_name]
    key_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
    engine.execute(upsert_statement)

if __name__ == "__main__": 
    load_dotenv()
    
    STAGING_DATABASE_NAME=os.environ.get("STAGING_DB_NAME")
    STAGING_SERVER_NAME=os.environ.get("STAGING_SERVER_NAME")
    STAGING_DB_USERNAME=os.environ.get("STAGING_DB_USERNAME")
    STAGING_DB_PASSWORD=os.environ.get("STAGING_DB_PASSWORD")
    STAGING_PORT=os.environ.get("STAGING_PORT")

    source_connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = STAGING_DB_USERNAME,
        password = STAGING_DB_PASSWORD,
        host = STAGING_SERVER_NAME, 
        port = STAGING_PORT,
        database = STAGING_DATABASE_NAME, 
    )
    source_engine = create_engine(source_connection_url)

    TARGET_DATABASE_NAME=os.environ.get("TARGET_DATABASE_NAME")
    TARGET_SERVER_NAME=os.environ.get("TARGET_SERVER_NAME")
    TARGET_DB_USERNAME=os.environ.get("TARGET_DB_USERNAME")
    TARGET_DB_PASSWORD=os.environ.get("TARGET_DB_PASSWORD")
    TARGET_PORT=os.environ.get("TARGET_PORT")

    target_connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = TARGET_DB_USERNAME,
        password = TARGET_DB_PASSWORD,
        host = TARGET_SERVER_NAME, 
        port = TARGET_PORT,
        database = TARGET_DATABASE_NAME, 
    )
    target_engine = create_engine(target_connection_url)
    
    environment = Environment(loader=FileSystemLoader("sql")) # ("project1_etl/assets/sql"))
    
    for sql_path in environment.list_templates():
        sql_template = environment.get_template(sql_path)
        table_name = sql_template.make_module().config.get("source_table_name")
        sql = sql_template.render()
        data = extract_from_staging(
            sql=sql,
            engine=source_engine
        )
        source_metadata = get_schema_metadata(engine=source_engine)
        load(data=data, table_name=table_name, engine=target_engine, source_metadata=source_metadata)