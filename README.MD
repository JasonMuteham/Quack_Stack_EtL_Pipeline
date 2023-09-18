# Simple EtL Pipeline

A very lightweight EtL pipeline builder with transformations and scheduling.

Flat Files -> transform -> DuckDB or Motherduck -> GitHub Actions

Test pipeline locally with **DuckDB** then to the cloud with **Motherduck**.

Currently works with excel & csv files.  

Uses pandas for import and Duckdb native connection/SQL for export to database.

Transformation of the data is performed as a SQL select on the loaded DataFrame.


## pipeline.toml

The pipeline.toml configures the whole pipeline.

### [pipeline]

name = "My_Simple_Pipe"

description = "My simple pipeline template"

schema = "raw" or "staging" or "Prod" # any preferred name.

database = "duckdb" or "motherduck"

### [logging]

level = "INFO" or "DEBUG" or other logging level

log_folder = "log/"

logfile = "simple_pipe.log"

### [task.load_csv_file]

active = false or true # is the task to be run?

file_type = "csv"

description = "Load csv file"

url = "data/my_data.csv"

sql_filter = "my_filter" # which sql statement to run, configured below. 

sql_table = "my_table_A" # name of table for loaded data 

sql_write = "replace" or "append"

### [task.load_excel_workbook]

active = false

file_type = "excel"

description = "Extract google sheet in xlsx format"

url = "https://docs.google.com/spreadsheets/??????????"

workbook = "workbook name"

skiprows = 4

columns = "a:k"

sql_filter = "my_filter"

sql_table = "my_table_b"

sql_write = "replace"

### [task.another_task]

...

### [task.yet_another_task]

...

### [duckdb.credentials]

path = "data/"

database = "simple_pipe.duckdb"

### [motherduck.credentials]

#MotherDuck access credentials are stored in secret.toml see below.

database = "simple_pipe"

### [sql.my_filter]

sql = """

SELECT * ***or a selection of columns***

FROM ***df_upload***   #always use df_upload 

WHERE ***your_condition_is_met***

"""

### [sql.another_sql_filter_select_statement]

sql = """

SELECT "column: 7" as "My Descriptive Column Name"

FROM ***df_upload***

WHERE "My Descriptive Column Name" IS NOT NULL

"""

The pipeline's internal DataFrame **df_upload** is processed with the **SELECT** statement before uploading to the database. So columns could be renamed, excluded from the selection, new derived columns added, etc. Rows filtered with the **WHERE** clause. Think **DBT!** 

## secret.toml

To use access MotherDuck locally update config/secret.toml 

MOTHERDUCK_TOKEN = "**Your Motherduck Access Token**"

**Be Safe** and add this file to .gitignore 

## GitHub Actions

pipeline_workflow.yml github action scheduling

**Repository secrets to be setup**

MOTHERDUCK_TOKEN = **Your Motherduck Access Token**

