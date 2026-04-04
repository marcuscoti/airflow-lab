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


POSTGRES_CONN_ID = 'pg_nova_drive'

TARGET_DIR = Path("/opt/airflow/data")

WATERMARK_DIR = Path("/opt/airflow/data/watermark")

TABLE_NAME = 'sales'

LOAD_TYPE = 'incremental' # Full load

SQL_QUERY = '''
SELECT 
id_vendas, 
id_veiculos, 
id_concessionarias, 
id_vendedores, 
id_clientes, 
valor_pago, 
data_venda, 
data_inclusao, 
data_atualizacao,
CURRENT_TIMESTAMP as _insert_timestamp
FROM public.vendas
WHERE data_atualizacao > '{watermark}'
'''

WATERMARK = datetime.datetime.utcnow()

TABLE_PATH = (TARGET_DIR / TABLE_NAME)

FILE_PATH = f"{TARGET_DIR}/{TABLE_NAME}/batch_{WATERMARK.strftime("%Y%m%d%H%M%S")}.parquet"


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
    dag_id="raw_sales",
    description="copy states table from database",
    schedule=None,
    default_args=DEFAULT_ARGUMENTS,
    start_date=pendulum.datetime(2026,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["raw","postgre","sales","fact", "inc load"]
) as dag:
    
    init_task = BashOperator(task_id="init_task", bash_command="echo Starting")


    @task
    def clear_target_folder() -> None:
        context = get_current_context()
        ti = context['ti']

        if os.path.exists(TABLE_PATH) and os.path.isdir(TABLE_PATH):
            shutil.rmtree(TABLE_PATH)
        else:
            ti.xcom_push(key='Error', value="Folder does not exist or is not a directory.")
            print("Folder does not exist or is not a directory.")


    @task
    def copy_from_source_incremental() -> None:
        TABLE_PATH.mkdir(parents=True, exist_ok=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        watermark = _load_watermark()
        print(f"Watermark: {watermark}")
        
        transformed_sql_query = SQL_QUERY.replace("{watermark}", watermark)
        print(f"SQL QUERY: {transformed_sql_query}")
        df = hook.get_df(transformed_sql_query)
        df.to_parquet(FILE_PATH, index=False, engine='pyarrow')

    
    @task
    def copy_from_source_full() -> None:
        TABLE_PATH.mkdir(parents=True, exist_ok=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        #watermark = _load_watermark()

        watermark = str(WATERMARK + datetime.timedelta(days=-365))
        print(f"Watermark: {watermark}")
        
        transformed_sql_query = SQL_QUERY.replace("{watermark}", watermark)
        print(f"SQL QUERY: {transformed_sql_query}")
        df = hook.get_df(transformed_sql_query)
        df.to_parquet(FILE_PATH, index=False, engine='pyarrow')


    @task
    def write_watermark() -> None:

        WATERMARK_DIR.mkdir(parents=True, exist_ok=True)

        json_file = WATERMARK_DIR / f"{TABLE_NAME}.json"

        d = {"table": str(TABLE_NAME),
             "table_path": str(TABLE_PATH),
             "watermark": str(WATERMARK)
             }
        json_file.write_text(json.dumps(d))


    @task
    def check_load_type(**kwargs):
        context = get_current_context()
        ti = context['ti']
        conf_value = kwargs['dag_run'].conf.get('load_type')
        if conf_value == None: conf_value = 'incremental'
        ti.xcom_push(key='load_type', value=conf_value)
        print(f"Config received: {conf_value}")


    @task.branch
    def branch_task():
        context = get_current_context()
        ti = context['ti']
        check_value = ti.xcom_pull(key='load_type', task_ids='check_load_type')  
        if check_value == 'full':
            return 'clear_target_folder'
        return 'empty_task'
    


    empty_task = EmptyOperator(task_id="empty_task")

    join_task = EmptyOperator(task_id="join_task", trigger_rule="none_failed_min_one_success")

    branch_result = branch_task()

    init_task >> check_load_type() >> branch_result
    branch_result >> empty_task >> copy_from_source_incremental() >> join_task
    branch_result >> clear_target_folder() >> copy_from_source_full() >> join_task
    join_task >> write_watermark()