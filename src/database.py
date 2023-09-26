import duckdb
import logging
from pathlib import Path
import os
from tenacity import retry, stop_after_attempt, wait_fixed

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def connect(db, db_name, db_path=None):
    """
    Connects to a database and returns a connection object.

    Parameters:
        db (str): The type of database to connect to. Valid values are "duckdb" and any other string.
        db_name (str): The name of the database.
        db_path (str, optional): The path to the database folder. Defaults to None.
   
    Returns:
        Connection: A connection object for the connected database.

    Raises:
        Exception: If there is an error creating the database folder.
        Exception: If there is an error connecting to the database.
    """
    if db == "duckdb":
        if db_path:
            db_name = f"{db_path}/{db_name}"
            try:
                Path(db_path).mkdir(exist_ok=True)
            except Exception as e:
                logging.critical(f"Can not create folder {db_path}")
                raise e
        try:
            return duckdb.connect(f"{db_name}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
            raise e
    else:
        # Need motherduck code in here
        MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
        try:
            return duckdb.connect(f"md:{db_name}?motherduck_token={MOTHERDUCK_TOKEN}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
            raise e

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def df_load(conn, df_upload, sqlfile, sqlschema, sqlwrite="append"):

    try:
        if sqlwrite == "replace":
            sql = f"CREATE OR REPLACE TABLE {sqlschema}.{sqlfile} AS SELECT * FROM df_upload"
        else:
            sql = f"INSERT INTO {sqlschema}.{sqlfile} SELECT * FROM df_upload"

        logging.info(f"- SQL: {sql}")
        conn.sql(sql)

        sql = f"SELECT * FROM {sqlschema}.{sqlfile}"
        result = conn.sql(sql).df()
        logging.info(f"- Verifying Uploaded Data - Result: {sqlschema}.{sqlfile}: {result.shape}")
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)
        raise e
