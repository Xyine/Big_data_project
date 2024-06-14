from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from fetch_tmdb_data import fetch_tmdb_data_task
from fetch_tvmaze_data import fetch_tvmaze_data_task
from standardize_data import standardize_data_task
from json_to_parquet import json_to_parquet_task
from combine_parquet import combine_parquet_task
from index_to_elasticsearch import index_to_elasticsearch_task

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weekly_tasks',
    default_args=default_args,
    description='A simple weekly DAG',
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

fetch_tmdb = PythonOperator(
    task_id='fetch_tmdb',
    python_callable=fetch_tmdb_data_task,
    provide_context=True,
    dag=dag,
)

fetch_tvmaze = PythonOperator(
    task_id='fetch_tvmaze',
    python_callable=fetch_tvmaze_data_task,
    provide_context=True,
    dag=dag,
)

standardize_data = PythonOperator(
    task_id='standardize_data',
    python_callable=standardize_data_task,
    provide_context=True,
    dag=dag,
)

json_to_parquet = PythonOperator(
    task_id='json_to_parquet',
    python_callable=json_to_parquet_task,
    provide_context=True,
    dag=dag,
)

combine_parquet = PythonOperator(
    task_id='combine_parquet',
    python_callable=combine_parquet_task,
    provide_context=True,
    dag=dag,
)

index_to_elasticsearch = PythonOperator(
    task_id='index_to_elasticsearch',
    python_callable=index_to_elasticsearch_task,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> fetch_tmdb >> fetch_tvmaze >> standardize_data >> json_to_parquet >> combine_parquet >> index_to_elasticsearch >> end
