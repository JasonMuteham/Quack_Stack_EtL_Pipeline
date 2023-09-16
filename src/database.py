import duckdb
import logging
from pathlib import Path
import os

def connect(db, db_name, dbpath = None):

    if db == "duckdb":
        try:
            Path(dbpath).mkdir(exist_ok=True)
        except Exception as e:
            logging.critical(f"Can not create folder {dbpath}")
            raise
        try:
            return duckdb.connect(f"{dbpath}{db_name}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
    else:
        # Need motherduck code in here
        MOTHERDUCK_DB = os.getenv('MOTHERDUCK_DB')
        MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
        try:
            return duckdb.connect(f"md:{MOTHERDUCK_DB}?{MOTHERDUCK_TOKEN}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)




def df_load(conn, df_upload, sqlfile, sqlschema, sqlwrite = "append"):
    try:
        sql = f"CREATE SCHEMA IF NOT EXISTS {sqlschema}"
        conn.sql(sql)
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)

    try:
        if sqlwrite == "replace":
            sql = f"CREATE OR REPLACE TABLE {sqlschema}.{sqlfile} AS SELECT * FROM df_upload"
        else:
            sql = f"INSERT INTO {sqlschema}.{sqlfile} SELECT * FROM df_upload"
            
        logging.info(f"- SQL: {sql}")
        conn.sql(sql)

        sql = f"SELECT * FROM {sqlschema}.{sqlfile}"
        result = conn.sql(sql).df()
        logging.info(f"- SQL Result: {sqlschema}.{sqlfile}: {result.shape}")
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)