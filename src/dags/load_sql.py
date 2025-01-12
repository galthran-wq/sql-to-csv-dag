from functools import partial
import os
from datetime import datetime
import os
import zipfile

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.lib.convert_sql import convert_all_files, get_files_to_convert
from src.common.config import get_config
from src.lib.mega_tools import upload_file_to_mega, download_file_from_mega
from src.lib.io_tools import extract_archive, archive
from src.lib.mariadb_tools import MariaDBClient

# Patching funcs with config values
config = get_config()
download_file_from_mega = partial(download_file_from_mega, artifacts_folder=config.artifacts_dir)
upload_file_to_mega = partial(
    upload_file_to_mega, 
    mega_login=config.mega.email, 
    mega_password=config.mega.password
)
convert_all_files = partial(
    convert_all_files,
    mariadb_client=MariaDBClient(
        user=config.mariadb.user,
        password=config.mariadb.password,
        host=config.mariadb.host,
        port=config.mariadb.port,
    )
)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'sql_to_csv',
    default_args=default_args,
    schedule_interval='@once',
)

download_task = PythonOperator(
    task_id='download_file_from_mega',
    python_callable=download_file_from_mega,
    op_kwargs={
        'mega_url': '{{ dag_run.conf.get("mega_url", "/default/path/to/archive.zip") }}'
    },
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_archive',
    python_callable=extract_archive,
    op_kwargs={
        'archive_path': '{{ task_instance.xcom_pull(task_ids="download_file_from_mega") }}',
    },
    dag=dag,
)

get_files_to_convert_task = PythonOperator(
    task_id='get_files_to_convert',
    python_callable=get_files_to_convert,
    op_kwargs={'root_folder': '{{ task_instance.xcom_pull(task_ids="extract_archive") }}'},
    dag=dag,
)

convert_all_files_task = PythonOperator(
    task_id='convert_all_files',
    python_callable=convert_all_files,
    op_kwargs={
        'files_to_convert': '{{ task_instance.xcom_pull(task_ids="get_files_to_convert") }}',
        'root_folder': '{{ task_instance.xcom_pull(task_ids="extract_archive") }}',
    },
    dag=dag,
)

archive_task = PythonOperator(
    task_id='archive_files',
    python_callable=archive,
    op_kwargs={'files_to_convert': '{{ task_instance.xcom_pull(task_ids="get_files_to_convert") }}'},
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_files_to_mega',
    python_callable=upload_file_to_mega,
    op_kwargs={'file_path': '{{ task_instance.xcom_pull(task_ids="convert_all_files") }}'},
    dag=dag,
)

download_task >> extract_task >> get_files_to_convert_task >> convert_all_files_task >> upload_task
