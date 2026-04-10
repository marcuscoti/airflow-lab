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
from airflow.sdk import get_current_context


POSTGRES_CONN_ID = 'pg_nova_drive'

TARGET_DIR = Path("/opt/airflow/data")

WATERMARK_DIR = Path("/opt/airflow/data/watermark")

TABLE_NAME = 'cities'

LOAD_TYPE = 'overwrite' # Full load

SQL_QUERY = '''
        SELECT id_cidades, cidade, id_estados, data_inclusao, data_atualizacao FROM public.cidades;
        '''

WATERMARK = datetime.datetime.utcnow()

TABLE_PATH = (TARGET_DIR / TABLE_NAME)

FILE_PATH = f"{TARGET_DIR}/{TABLE_NAME}/batch_{WATERMARK.strftime("%Y%m%d%H%M%S")}.parquet"

DATASET = Dataset("/opt/airflow/data/cities/")

DEFAULT_ARGUMENTS = {
    "execution_timeout": datetime.timedelta(seconds=7200),
    "retry_delay": 300,
    "retries": 0,
    "priority_weight": 1,
    "pool": "raw_bronze_pool",
    "weight_rule": "absolute",
}


with DAG(
    dag_id="raw_cities",
    description="copy cities table from database",
    schedule=None,
    default_args=DEFAULT_ARGUMENTS,
    start_date=pendulum.datetime(2026,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["raw","postgre","cities","dimension", "full load"]
) as dag:
    
    init_task = BashOperator(task_id="init_task", bash_command="echo Starting")


    @task()
    def clear_target_folder() -> None:
        context = get_current_context()
        ti = context['ti']

        if os.path.exists(TABLE_PATH) and os.path.isdir(TABLE_PATH):
            shutil.rmtree(TABLE_PATH)
        else:
            ti.xcom_push(key='Error', value="Folder does not exist or is not a directory.")
            print("Folder does not exist or is not a directory.")


    @task(outlets=[DATASET])
    def copy_from_source() -> None:
        TABLE_PATH.mkdir(parents=True, exist_ok=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        df = hook.get_df(SQL_QUERY)
        df.to_parquet(FILE_PATH, index=False, engine='pyarrow')



    @task()
    def write_watermark() -> None:

        WATERMARK_DIR.mkdir(parents=True, exist_ok=True)

        json_file = WATERMARK_DIR / f"{TABLE_NAME}.json"

        d = {"table": str(TABLE_NAME),
             "table_path": str(TABLE_PATH),
             "watermark": str(WATERMARK)
             }

        json_file.write_text(json.dumps(d))



    init_task >> clear_target_folder() >> copy_from_source() >> write_watermark()