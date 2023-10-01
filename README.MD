# Quack Stack EtL Pipeline 

A very lightweight EtL pipeline builder with transformations and scheduling.

Flat Files -> transform -> DuckDB or Motherduck -> GitHub Actions

run pipeline with

### python src/simple_pipe.py ###

configure pipeline in

### config/pipeline.toml ###

Test pipeline locally with **DuckDB** then to the cloud with **Motherduck**.

Currently works with excel & csv files.  

Uses pandas for import of excel files and Duckdb native connection/SQL for csv import/export to database.

Data is initially loaded to the schema "staging". Then filtered and loaded to the schema in pipeline.toml

Transformation of the data is performed as a SQL select on the staged table, dbt-ish.

Sample Pipeline of Lego Bricks Data included. The pipeline loads various .csv file transforming
'color' to 'colour'. 

![rebrickable schema](https://rebrickable.com/static/img/diagrams/downloads_schema_v3.png)

Schema & data from [rebrickable](https://rebrickable.com)


## pipeline.toml

The pipeline.toml configures the whole pipeline.

### [pipeline]

name = "My_Simple_Pipe"

description = "My simple pipeline template"

schema = "raw" or "Prod" # any preferred name **NOT** "staging" as used internally

database = "duckdb" or "motherduck"

### [logging]

level = "INFO" or "DEBUG" or other logging level

log_folder = "log/"

logfile = "simple_pipe.log"

### [task.load_csv_file]

active = false or true # is the task to be run?

file_type = "csv"

description = "Load csv file"

url = "data/my_data.csv" # local path or url

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

### [task.custom_task]

active = true

description = "Call a custom function"

file_type = "function.custom.myfunction"

param.first_parameter = "Lego"

param.second_parameter = "Is Cool!"

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

FROM ***staging.<sql_table>***   #eg. FROM staging.colours

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

