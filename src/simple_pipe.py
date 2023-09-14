from pathlib import Path
import pandas as pd
import get
#import database
import logging
import tomllib

if __name__ == "__main__":

    print('Searching for my pipe....')
    failed_tasks = 0

    try:
        with open("config/pipeline.toml", "rb") as f:
            pipe_cfg = tomllib.load(f)
    except Exception as e:
         print(f"*** Where's the pipe? ***\n{e}")
         raise

    log_file = pipe_cfg['logging']['logfile']

    try:
        Path('log').mkdir(exist_ok=True)
    except Exception as e:
        print(e)
        raise
    
    print(f'Making notes in {log_file}')

    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        level=pipe_cfg['logging']['level'],
                        datefmt='%d/%m/%Y %H:%M:%S',
                        filemode='w'
                        )
    logging.info(f"Simple Pipe: Lighting the fire!\n{'*'*60}")
    print("Pipe lit!")

    pipeline = pipe_cfg['pipeline']
    logging.info(f"Pipeline Config: schema: {pipeline['schema']} , database: {pipeline['database']}" )

    db_name = pipe_cfg[pipeline['database']]['credentials']['database']
  
    logging.info(f"Connecting to database {pipeline['database']}: {db_name} ")
    try:
        db_con = database.connect(pipeline['database'], db_name)
    except Exception as e:
        print(e)
        raise


    logging.info("Simple Pipe: Smoking!")
    logging.info("Simple Pipe: Looking at task list....")
    tasks = pipe_cfg['pipeline_tasks']
    for task in tasks:
         logging.info(f"Task: {task} type: {tasks[task]}")
    """
        Put the pipeline code below this!
    """
    logging.info("Simple Pipe: Processing Task List")

    for task in tasks:
            logging.info(f"Looking at task: {task} type: {tasks[task]}")
            task_cfg = pipe_cfg[task]
            if tasks[task] == "excel":
                df_excel = get.excel(task_cfg['url'],task_cfg['workbook'],task_cfg['skiprows'],task_cfg['columns'])

                if df_excel is None:
                    logging.error(f"{task}: No raw data extracted")
                    failed_tasks += 1

            else:
                logging.info(f'Oops problem with the task "{task}". The task type "{tasks[task]}" is not support (yet!)')
                failed_tasks += 1
    if failed_tasks :
      fail_txt = "Oh No!"  
    else:
        fail_txt = "Yay!"   
    print(f"{fail_txt} Pipe went out this many times: {failed_tasks}")
    logging.info(f"Simple Pipe: {fail_txt} {failed_tasks} tasks failed!")
    logging.info(f"Simple Pipe: Smoked!\n{'*'*60}")
    print('Yay! Pipe smoked!')
