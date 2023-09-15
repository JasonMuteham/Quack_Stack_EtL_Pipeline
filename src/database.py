import duckdb
import logging
import tomllib
from pathlib import Path
import os

def connect(db, db_name, dbpath = None, local = True):

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
        if local:
        # Need motherduck code in here
            try:
                with open("config/secret.toml", "rb") as f:
                    shush = tomllib.load(f)['motherduck']['credentials']
            except Exception as e:
                print(f"*** Where's the secret pipe? ***\n{e}")
                raise

            try:
                return duckdb.connect(f"md:{shush['database']}?{shush['token']}")
            except Exception as e:
                logging.error(f"Database connection error: {e}")
                print(e)
        else:
            try:
                return duckdb.connect(f"md:{motherduck.database}?{motherduck.token}")
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