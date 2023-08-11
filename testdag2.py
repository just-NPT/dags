from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def generate_log(task_number):
    log_message = f"Generated log for Task {task_number}"
    return log_message

def task_1_func():
    log = generate_log(1)
    print(log)

def task_2_func():
    log = generate_log(2)
    print(log)

def task_3_func():
    log = generate_log(3)
    print(log)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'log_generation_dag',
    default_args=default_args,
    description='A simple DAG to generate logs',
    schedule_interval=timedelta(days=1),
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1_func,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2_func,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3_func,
    dag=dag,
)

task_1 >> task_2 >> task_3
