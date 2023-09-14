import duckdb

def connect(db, db_name):
    if db == "duckdb":
        return duckdb.connect(db_name)
    else:
        # Need motherduck code in here
        return duckdb.connect(db_name)