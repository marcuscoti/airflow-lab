import json
import os
import shutil
from pathlib import Path
import datetime
import pendulum
import pandas as pd
from airflow import DAG, Dataset
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator, SQLColumnCheckOperator
from airflow.providers.standard.operators.bash import BashOperator, BaseOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.sdk import get_current_context
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure_datalake_operators import ADLSUploadFileOperator, ADLSListBlobsOperator, ADLSRemoveBlobOperator

ADLS_CONN_ID = 'az_blob_raw'

DATA_DIR = "/opt/airflow/data"

TABLE_NAME = 'sales'

CONTAINER_NAME = "raw"

DATASET = Dataset(f"{DATA_DIR}/sales/")

DEFAULT_ARGUMENTS = {
    "execution_timeout": datetime.timedelta(seconds=7200),
    "retry_delay": 300,
    "retries": 0,
    "priority_weight": 1,
    "pool": "raw_bronze_pool",
    "weight_rule": "absolute",
}



with DAG(
    dag_id="adls_sync_sales_data",
    description="Sync PARQUET files to ADLS",
    schedule=[DATASET],
    default_args=DEFAULT_ARGUMENTS,
    start_date=pendulum.datetime(2026,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["raw","ADLS","sales","fact", "inc load"]
) as dag:
    

    @task
    def list_local_files(path):
        # List all files in the specified directory
        files = os.listdir(path)
        print(f"Files in {path}: {files}")
        return files


    adls_list_blob_task = ADLSListBlobsOperator(
        task_id = "adls_list_blob",
        connection_id = ADLS_CONN_ID,
        container_name = CONTAINER_NAME,
        prefix = TABLE_NAME,
        endswith = '.parquet'
    )


    @task
    def filter_files_to_remove(local_files, blob_files):
        files_to_remove = []
        for f in blob_files:
             if f.replace(f"{TABLE_NAME}/",'') not in local_files:
                files_to_remove.append(f)
        return files_to_remove


    @task
    def filter_new_files(local_files, blob_files):
        new_files = []
        for f in local_files:
             if f"{TABLE_NAME}/{f}" not in blob_files:
                new_files.append(f)
        return new_files


    adls_remove_blob_task = ADLSRemoveBlobOperator.partial(
        task_id = "adls_remove_blob_task",
        connection_id = ADLS_CONN_ID,
        container_name = CONTAINER_NAME
    )

    adls_upload_task = ADLSUploadFileOperator.partial(
        task_id = "upload_task",
        connection_id = ADLS_CONN_ID,
        source_file_path = f"{DATA_DIR}/{TABLE_NAME}",
        container_name = CONTAINER_NAME,
        blob_name = f"{TABLE_NAME}",
        trigger_rule="none_failed_min_one_success"
    )


    local_files = list_local_files(f"{DATA_DIR}/{TABLE_NAME}")
    blob_files = adls_list_blob_task.output
    files_to_remove = filter_files_to_remove(local_files, blob_files)
    new_files = filter_new_files(local_files, blob_files)
    adls_remove_blob_task.expand(file=files_to_remove) >> adls_upload_task.expand(file=new_files)