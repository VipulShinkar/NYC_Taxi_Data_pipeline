from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os

from functions.download_api import download_trip_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'NYC_Taxi_Pipeline',
    default_args=default_args,
    description='NYC Taxi trip data pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the Python callable function for the PythonOperator
def download_data_callable(taxi_type, year, month, base_dir, **kwargs):
    download_trip_data(taxi_type, year, month, base_dir)

# Base directory for the data within the container
base_dir = '/usr/local/airflow/data'

# Year and month for the data
year = '2023'
month = '12'

# Create a TaskGroup for all taxi data downloads
with TaskGroup('taxidata_download_tasks', dag=dag) as taxidata_download:
    download_yellow_taxi = PythonOperator(
        task_id='download_yellow_taxi',
        python_callable=download_data_callable,
        op_kwargs={'taxi_type': 'yellow', 'year': year, 'month': month, 'base_dir': base_dir},
        dag=dag,
    )

    download_green_taxi = PythonOperator(
        task_id='download_green_taxi',
        python_callable=download_data_callable,
        op_kwargs={'taxi_type': 'green', 'year': year, 'month': month, 'base_dir': base_dir},
        dag=dag,
    )

    download_fhv_taxi = PythonOperator(
        task_id='download_fhv_taxi',
        python_callable=download_data_callable,
        op_kwargs={'taxi_type': 'fhv', 'year': year, 'month': month, 'base_dir': base_dir},
        dag=dag,
    )

    download_fhvhv_taxi = PythonOperator(
        task_id='download_fhvhv_taxi',
        python_callable=download_data_callable,
        op_kwargs={'taxi_type': 'fhvhv', 'year': year, 'month': month, 'base_dir': base_dir},
        dag=dag,
    )

# Define the order of execution
taxidata_download