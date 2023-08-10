from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello():
    return "Hello Airflow!"

dag = DAG(
    'simple_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime.utcnow(),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
)

task1 = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)
