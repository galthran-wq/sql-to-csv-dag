from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models.param import Param

import zipfile
import pandas as pd

def print_hello(name: str):
    return f'Hello, World! Hello, {name}'

dag = DAG(
    dag_id='print_hello',
    schedule=None,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        "name": Param("example", type="string", description="name to greet")
    },
)

print_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    op_args=["{{ params.name }}"],
    # params={"name": Param(type="string", description="name to greet")},
    dag=dag,
)



