import sys
sys.path.append('/app')

from airflow.decorators import dag
from datetime import datetime
from typing import Dict, List
from airflow_demo.etl_task import get_batch_params, extract_batch, load_batch, rollback_on_failure
from airflow_demo.etl_func import get_dest_table_name

@dag(
    dag_id='postgres_arrow_etl_dag',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description='PostgreSQL to Arrow ETL pipeline using Airflow',
)
def postgres_arrow_etl_dag():
    conn_params: Dict[str, str] = {
        'dbname': "postgres",
        'user': "postgres",
        'password': "postgres",
        'host': "mq-airflow-etl-test.cwanclutkkrz.ap-southeast-1.rds.amazonaws.com",
    }
    source_table_name: str = "public.dummy_tbl"
    batch_size: int = 400
    where: str = ""
    dest_table_name = get_dest_table_name(source_table_name)

    # 1. Calculate total batches
    batch_params: List[Dict] = get_batch_params(conn_params, source_table_name, dest_table_name, batch_size, where=where)

    # 2. Dynamically create extract tasks per batch
    extract_batch_params = extract_batch.expand_kwargs(batch_params)

    # 3. Map load tasks per batch after extract
    load_batch_task = load_batch.expand_kwargs(extract_batch_params)

    # 4. Rollback data in case of any load failure
    rollback_task = rollback_on_failure(conn_params, dest_table_name)
    load_batch_task >> rollback_task

# Instantiate the DAG
dag_instance = postgres_arrow_etl_dag()