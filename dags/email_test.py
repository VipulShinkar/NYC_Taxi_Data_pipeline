from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def failing_task():
    raise ValueError("This task is designed to fail.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['vipulshinkar08@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'manual_trigger_fail_task',
    default_args=default_args,
    description='A DAG with a task that fails on manual trigger',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=datetime(2023, 1, 1),  # Set your desired start date
    catchup=False,
)

failing_task = PythonOperator(
    task_id='failing_task',
    python_callable=failing_task,
    dag=dag,
)
