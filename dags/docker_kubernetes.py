from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 3, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
with DAG("docker_kubernetes", default_args=default_args, schedule_interval='* 6 * * *',
         template_searchpath=['/usr/local/airflow/sql_files'], catchup=False) as dag:
    # Creating the table same as csv columns
    task1 = PostgresOperator(task_id="create_new_table", postgres_conn_id='postgres_conn', sql="create_new_table.sql")
    # Filling up the columns of the table while reading the data from the csv file
    task1
