import json
import os
import shutil
from pathlib import Path
import datetime
import pendulum
import pandas as pd
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator, SQLColumnCheckOperator
from airflow.providers.standard.operators.bash import BashOperator, BaseOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import get_current_context
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


POSTGRES_CONN_ID = 'pg_nova_drive'

TARGET_DIR = Path("/opt/airflow/data")

WATERMARK_DIR = Path("/opt/airflow/data/watermark")

TABLE_NAME = 'sales'

LOAD_TYPE = 'incremental' # Full load


WATERMARK = datetime.datetime.utcnow()

TABLE_PATH = (TARGET_DIR / TABLE_NAME)

FILE_PATH = f"{TARGET_DIR}/{TABLE_NAME}/batch_{WATERMARK.strftime("%Y%m%d%H%M%S")}.parquet"

ADLS_CONN_ID = 'az_blob_raw'

DEFAULT_ARGUMENTS = {
    "execution_timeout": datetime.timedelta(seconds=7200),
    "retry_delay": 300,
    "retries": 0,
    "priority_weight": 1,
    "pool": "raw_bronze_pool",
    "weight_rule": "absolute",
}

def _load_watermark() -> str:
    json_file = WATERMARK_DIR / f"{TABLE_NAME}.json"
    if json_file.exists():
        json_content = json.loads(json_file.read_text())
        return json_content['watermark']
    duration = datetime.timedelta(days=-365)
    return str((WATERMARK + duration))

with DAG(
    dag_id="adls_send_test_data",
    description="Sync PARQUET files to ADLS",
    schedule=None,
    default_args=DEFAULT_ARGUMENTS,
    start_date=pendulum.datetime(2026,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["raw","ADLS","sales","fact", "inc load"]
) as dag:
    
    #init_task = BashOperator(task_id="init_task", bash_command="echo Starting")

    def clear_target_folder() -> None:
        context = get_current_context()
        ti = context['ti']
        if os.path.exists(TABLE_PATH) and os.path.isdir(TABLE_PATH):
            shutil.rmtree(TABLE_PATH)
        else:
            ti.xcom_push(key='Error', value="Folder does not exist or is not a directory.")
            print("Folder does not exist or is not a directory.")


    @task
    def upload_to_blob():
        hook = WasbHook(wasb_conn_id=ADLS_CONN_ID)
        # Upload a local file
        hook.load_file(
            file_path='/opt/airflow/data/cities/batch_20260402191138.parquet', 
            container_name='raw', 
            blob_name='cities/batch_20260402191138.parquet'
            )

    upload_to_blob()