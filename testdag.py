from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def print_hello():
    return "Hello Airflow!"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
)

task1 = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

def sleep_function():
    import time
    time.sleep(5)

task2 = PythonOperator(
    task_id='sleep_task',
    python_callable=sleep_function,
    dag=dag,
)

task1 >> task2
