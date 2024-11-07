# Importing libraries

from datetime import datetime
from urllib import request

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# Crawling data
def crawling_data(**kwargs):
    year = kwargs['year']
    month = kwargs['month']
    day = kwargs['day']
    hour = kwargs['hour']
    output_path = kwargs['output_path']

    url = (f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:0>2}"
           f"/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz")

    request.urlretrieve(url, output_path)


# Replacing a single quote with 2 single quotes, make data available for insert_into database script
def escape_single_quotes(text):
    return text.replace("'", "''")

# Extracting data from a file then write INSERT INTO scripts
def writing_insert_script(**kwargs):
    # Getting values of op_kwargs from PythonOperator
    input_path = kwargs['input_path']
    sql_execute_path = kwargs['sql_execute_path']
    d = kwargs['_date']

    # Initializing an empty dict
    result = dict()

    # Parsing a file then add result into a dict
    with open(input_path, 'r',encoding='utf-8', errors='ignore') as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == 'en' and int(view_counts) > 100:
                result[page_title] = view_counts

    # Writing a SQL script for inserting data
    with open(sql_execute_path,'w') as f:
        for page_title, view_counts in result.items():
            # Formatting a page title
            page_title = escape_single_quotes(page_title.replace("_", " "))

            f.write(f"""
                INSERT INTO pageviews (page_title,nums_of_views, datetime)
                VALUES ('{page_title}',{view_counts} , '{d}'); \n
            """)


# Creating DAG for Data Pipeline
# max_active_runs=1 on DAG initialization and depends_on_past=True on each task
# To finish all the tasks of the current day then move on to the task on the next day
with DAG(
    dag_id='wikimedia_pipeline',
    start_date=datetime(2024, 10, 18),
    schedule_interval='0 0 */2 * *', # Executing task every 2 days
    template_searchpath='/var/tmp', # A path to SQL script
    max_active_runs=1

) as dag:

    # Task: Crawling data
    crawling_data = PythonOperator(
        task_id='crawling_data',
        python_callable=crawling_data,
        depends_on_past=True,
        op_kwargs={
            "year": "{{ execution_date.year }}",
            "month": "{{ execution_date.month }}",
            "day": "{{ execution_date.day }}",
            "hour": "{{ execution_date.hour }}",
            "output_path": "/var/tmp/wikipageview.gz"
        }
    )

    # Task: Unzipping an gzip file
    gunzip = BashOperator(
        task_id='gunzip',
        depends_on_past=True,
        bash_command='gunzip --force /var/tmp/wikipageview.gz',
    )

    # Task: Creating a table in database if not exists
    creating_table = PostgresOperator(
        task_id='creating_table',
        postgres_conn_id='postgres_conn',
        depends_on_past=True,
        sql="""
            CREATE TABLE IF NOT EXISTS pageviews (
            page_title VARCHAR(100),
            nums_of_views INTEGER,
            datetime TIMESTAMP
        );
        """
    )

    # Task: Writing an insert SQL script
    writing_insert_script = PythonOperator(
        task_id='writing_insert_script',
        python_callable=writing_insert_script,

        depends_on_past=True,
        op_kwargs={
            "input_path": "/var/tmp/wikipageview",
            "sql_execute_path": "/var/tmp/insert_data.sql",
            "_date": "{{ execution_date }}",
        }
    )

    # Task: Executing insert SQL script
    inserting_into_db = PostgresOperator(
        task_id='inserting_into_db',
        postgres_conn_id='postgres_conn',
        depends_on_past=True,
        sql='insert_data.sql'
    )

# Data flow
crawling_data >> gunzip >> creating_table >> writing_insert_script >> inserting_into_db