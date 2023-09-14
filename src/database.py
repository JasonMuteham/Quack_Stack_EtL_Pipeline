import duckdb
import logging

def connect(db, db_name):
    if db == "duckdb":
        try:
            return duckdb.connect(db_name)
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
    else:
        # Need motherduck code in here
        return duckdb.connect(db_name)
def df_load(conn, df_upload, sqlfile, sqlwrite = "append"):
    try:
        if sqlwrite == "replace":
            sql = f"CREATE OR REPLACE TABLE {sqlfile} AS SELECT * FROM df_upload"
        else:
            sql = f"INSERT INTO {sqlfile} SELECT * FROM df_upload"
            
        logging.info(f"- SQL: {sql}")
        conn.sql(sql)

        sql = f"SELECT COUNT(*) FROM {sqlfile}"
        result = conn.sql(sql)
        logging.info(f"SQL Result: {result}")
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)