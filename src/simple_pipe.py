from pathlib import Path
import pond
import logging
import tomllib
import custom # put your custom code in this module # noqa: F401
import pandas as pd

if __name__ == "__main__":
    print(f"{'*'*50}\nSearching for my duck pond....")
    failed_tasks = 0

    try:
        with open("config/pipeline.toml", "rb") as f:
            pipe_cfg = tomllib.load(f)
    except Exception as e:
        print(f"*** Where is the duck pond? ***\n{e}")
        raise

    log_file = pipe_cfg["logging"]["logfile"]
    log_path = pipe_cfg["logging"]["log_folder"]
    print("Searching for something to write on...")

    if log_path: 
        log_file = f"{log_path}/{log_file}"
        try:
            Path(log_path).mkdir(exist_ok=True)
        except Exception as e:
            print(e)
            raise

    print(f"Making notes... {log_file}")

    logging.basicConfig(
        filename=log_file,
        format="%(asctime)s %(levelname)s: %(message)s",
        level=pipe_cfg["logging"]["level"],
        datefmt="%d/%m/%Y %H:%M:%S",
        filemode="w",
    )
    logging.info(f"Duck Pond: Taking flight...\n{'*'*100}")
    print("Taking flight...")

    pipeline = pipe_cfg["pipeline"]
    logging.info(f'Found pipeline: {pipeline["name"]}')
    logging.info(f'Description: {pipeline["description"]}')

    logging.info("Loading environment variables")
    pond.env_load()

    logging.info(
        f"Pipeline Config: schema: {pipeline['schema']} , database engine: {pipeline['database']}"
    )

    db_name = pipe_cfg[pipeline["database"]]["credentials"]["database"]
    db_path = pipe_cfg[pipeline["database"]]["credentials"]["path"]

    logging.info(f"Connecting to database {pipeline['database']}: {db_path}/{db_name} ")
    try:
        db_con = pond.connect(pipeline["database"], db_name, db_path=db_path)
    except Exception as e:
        print(e)
        raise
    else:
        logging.info("Checking duckdb extensions")
        pond.checkdb(db_con)
        logging.info("Checking duckdb schema: staging")
        pond.schema(db_con,"staging")
        logging.info(f"Checking duckdb schema: {pipeline['schema']}")
        pond.schema(db_con, pipeline['schema'])

    logging.info("Duck Pond: Get my ducks in a row!")
    logging.info("Duck Pond: Looking at task list....")
    tasks = pipe_cfg["task"]

    for task in tasks:
        if tasks[task]["active"]:
            logging.info(f"- Task: {task} - {tasks[task]['description']}")
        else:
            logging.info(f"- Skipping Task: {task} - {tasks[task]['description']}")

    logging.info("Duck Pond: Processing Task List")

    for task in tasks:
        if tasks[task]["active"]:
            logging.info(f"Starting task: {task} type: {tasks[task]['file_type']}")
            try:
                sql_table = tasks[task]["sql_table"]
            except Exception:
                sql_table = None
                sql_filter_name = None
                sql_write = None
            else:    
                sql_filter_name = tasks[task]["sql_filter"]
                if sql_filter_name:
                    sql_filter = pipe_cfg["sql"][sql_filter_name]["sql"]
                else:
                    sql_filter = None
                sql_write = tasks[task]["sql_write"]

            if tasks[task]["file_type"] == "excel":
                """
                Use Pandas for excel read as it has more features!
                """
                df_upload = pond.excel(
                    tasks[task]["url"],
                    tasks[task]["workbook"],
                    tasks[task]["skiprows"],
                    tasks[task]["columns"],
                )
                if df_upload.empty:
                    logging.warning(f"- {task}: No raw data extracted")
                    failed_tasks += 1
                else:
                    if sql_filter_name:
                        logging.info(f"- SQL filtering: {sql_filter_name}")
                        df_upload = pond.sqlfilter(db_con, df_upload, sql_filter)
                    if df_upload.empty:
                        logging.warning(f"- {sql_filter_name}: Returned no data!")
                        failed_tasks += 1
                    else:
                        pond.df_load(db_con,df_upload,sql_table,sqlschema=pipeline["schema"],sqlwrite=sql_write)
                        if tasks[task]["csv_file"]:
                            df_upload.to_csv(tasks[task]["csv_file"], index=False)

            elif tasks[task]["file_type"] == "csv.filelist":
                pond.csv_filelist(db_con, tasks[task]["url"], sql_table)
                if sql_filter_name:
                     logging.info(f"- SQL filtering: {sql_filter_name}")

                pond.load(db_con,sql_table,pipeline["schema"], schema_from="staging",sql_write=sql_write, sql_filter= sql_filter)  # noqa: E501

            elif tasks[task]["file_type"] == "csv":
                url=tasks[task]["url"]
                pond.csv(db_con, url, sql_table, schema="staging", replace=True)
                if sql_filter_name:
                     logging.info(f"- SQL filtering: {sql_filter_name}")

                pond.load(db_con,sql_table,pipeline["schema"], schema_from="staging",sql_write=sql_write, sql_filter= sql_filter)  # noqa: E501
            #
            # If duckdb csv loading is not working, use pandas! 
            #
            elif tasks[task]["file_type"] == "pandas_csv":
                url=tasks[task]["url"]
                try:
                    Path(tasks[task]['file_path']).mkdir(exist_ok=True)
                except Exception as e:
                    print(e)
                    raise
                
                try:
                    df_download = pd.read_csv(url)
                except Exception as e:
                    print(e)
                    raise

                if df_download.empty:
                    logging.warning(f"- {task}: No raw data extracted")
                    failed_tasks += 1
                    print("Oh No! pipe went out...")
                else:
                    file_path = f"{tasks[task]['file_path']}/{tasks[task]['file_name']}"
                    df_download.to_csv(file_path, index=False)
                    logging.info(f"- {task}: Result: {df_download.shape} extracted")

            elif tasks[task]["file_type"].split(".")[0] == "function":
                """
                your custom function call 'function.'<module>.<function>
                eg. function.custom.get_mps
                """
                params = tasks[task]['param']
                eval('.'.join(tasks[task]["file_type"].split('.')[1:]))(params)  
            else:
                logging.info(
                    f'- Oops problem with the task "{task}". The task type {tasks[task]["file_type"]} is not support (yet!)'  # noqa: E501
                )
                failed_tasks += 1
 
            logging.info(f"Finished processing task: {task}")

    if failed_tasks:
        fail_txt = "Oh No!"
    else:
        fail_txt = "Yay!"

    print(f"{fail_txt} Ducks went missing {failed_tasks} times!")
    logging.info(f"Duck Pond: {fail_txt} {failed_tasks} tasks failed!")
    logging.info(f"Duck Pond: Ducks safely in the pond!\n{'*'*100}")
    print(f"Ducks safely in the pond!\n{'*'*50}")
