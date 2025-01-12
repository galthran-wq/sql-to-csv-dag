from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import zipfile
import pandas as pd

def print_hello():
    return 'Hello, World!'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'print_hello',
    default_args=default_args,
    schedule_interval='@once',
)

print_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)



